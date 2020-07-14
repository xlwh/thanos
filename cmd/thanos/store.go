package main

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/tls"
	"gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

// registerStore registers a store command.
// Store应该是负责读写对象存储把
func registerStore(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command(component.Store.String(), "store node giving access to blocks in a bucket provider. Now supported GCS, S3, Azure, Swift and Tencent COS.")

	// HTTP 服务和GRPC服务
	httpBindAddr, httpGracePeriod := regHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := regGRPCFlags(cmd)

	// 数据保存目录
	dataDir := cmd.Flag("data-dir", "Data directory in which to cache remote blocks.").
		Default("./data").String()

	// 索引缓存大小
	indexCacheSize := cmd.Flag("index-cache-size", "Maximum size of items held in the index cache.").
		Default("250MB").Bytes()

	indexCacheItemSize := cmd.Flag("index-cache-item-size", "Maximum size of items held in the index cache.").
		Default("100MB").Bytes()

	// chunk池大小
	chunkPoolSize := cmd.Flag("chunk-pool-size", "Maximum size of concurrently allocatable bytes for chunks.").
		Default("2GB").Bytes()

	// 最大的数据查询点数
	maxSampleCount := cmd.Flag("store.grpc.series-sample-limit",
		"Maximum amount of samples returned via a single Series call. 0 means no limit. NOTE: for efficiency we take 120 as the number of samples in chunk (it cannot be bigger than that), so the actual number of samples might be lower, even though the maximum could be hit.").
		Default("0").Uint()

	// 最大查询并发数
	maxConcurrent := cmd.Flag("store.grpc.series-max-concurrency", "Maximum number of concurrent Series calls.").Default("20").Int()

	// 是否需要公共的标签
	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	syncInterval := cmd.Flag("sync-block-duration", "Repeat interval for syncing the blocks between local and remote view.").
		Default("3m").Duration()
	// block同步的并发度
	blockSyncConcurrency := cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing blocks from object storage.").
		Default("20").Int()

	// minTime和maxTime
	minTime := model.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to serve. Thanos Store will serve only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))

	maxTime := model.TimeOrDuration(cmd.Flag("max-time", "End of time range limit to serve. Thanos Store will serve only blocks, which happened eariler than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z"))

	advertiseCompatibilityLabel := cmd.Flag("debug.advertise-compatibility-label", "If true, Store Gateway in addition to other labels, will advertise special \"@thanos_compatibility_store_type=store\" label set. This makes store Gateway compatible with Querier before 0.8.0").
		Hidden().Default("true").Bool()

	selectorRelabelConf := regSelectorRelabelFlags(cmd)

	// 启动存储服务
	m[component.Store.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, debugLogging bool) error {
		// minTime 必须等于maxTime，防止Prometheus进行compaction进行一些计算
		if minTime.PrometheusTimestamp() > maxTime.PrometheusTimestamp() {
			return errors.Errorf("invalid argument: --min-time '%s' can't be greater than --max-time '%s'",
				minTime, maxTime)
		}

		return runStore(g,
			logger,         // 日志组件
			reg,            // 监控组件
			tracer,         // 查询Trace服务，进行一些请求的Trace
			objStoreConfig, // 是否需要使用一些公共的tag
			*dataDir,       // 数据目录，可能是保存一些从对象存储上同步下来的缓存数据吧

			// 下面是一些HTTP 和RPC的参数选项
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),

			uint64(*indexCacheSize), // 索引缓存大小
			uint64(*indexCacheItemSize),
			uint64(*chunkPoolSize),  // chunkPool的大小
			uint64(*maxSampleCount), // 最大的数据点数
			*maxConcurrent,          // 最大并发数
			component.Store,         // 对应的组件
			debugLogging,            // 是否要开启debug日志模式
			*syncInterval,           // 数据同步间隔，定期从对象存储
			*blockSyncConcurrency,   // block同步的并发度
			&store.FilterConfig{
				MinTime: *minTime,
				MaxTime: *maxTime,
			},
			selectorRelabelConf,
			*advertiseCompatibilityLabel,
		)
	}
}

// runStore starts a daemon that serves queries to cluster peers using data from an object store.
func runStore(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	objStoreConfig *extflag.PathOrContent,
	dataDir string,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	indexCacheSizeBytes uint64,
	indexMaxItemSizeBytes uint64,
	chunkPoolSizeBytes uint64,
	maxSampleCount uint64,
	maxConcurrent int,
	component component.Component,
	verbose bool,
	syncInterval time.Duration,
	blockSyncConcurrency int,
	filterConf *store.FilterConfig, // 定义在store上的maxTime和minTime，可以有效的过滤一些无效数据
	selectorRelabelConf *extflag.PathOrContent,
	advertiseCompatibilityLabel bool,
) error {
	// Initiate HTTP listener providing metrics endpoint and readiness/liveness probes.
	// 启动HTTP 服务，这个服务带一些探活的功能
	statusProber := prober.NewProber(component, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	srv := httpserver.New(logger, reg, component, statusProber,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
	)

	g.Add(srv.ListenAndServe, srv.Shutdown)

	// 连接对象存储，可以是本地目录
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	// 启动对象存储的client
	bkt, err := client.NewBucket(logger, confContentYaml, reg, component.String())
	if err != nil {
		return errors.Wrap(err, "create bucket client")
	}

	// relabel 配置
	relabelContentYaml, err := selectorRelabelConf.Content()
	if err != nil {
		return errors.Wrap(err, "get content of relabel configuration")
	}

	// 可以对数据进行一些Relabel
	relabelConfig, err := parseRelabelConfig(relabelContentYaml)
	if err != nil {
		return err
	}

	// Ensure we close up everything properly.
	// 确保进程在退出的时候，可以正确的退出我们的对象存储的Client
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
	}()

	// TODO(bwplotka): Add as a flag?
	maxItemSizeBytes := indexCacheSizeBytes / 2 // 每个Item的最低大小

	indexCache, err := storecache.NewIndexCache(logger, reg, storecache.Opts{
		MaxSizeBytes:     indexCacheSizeBytes,
		MaxItemSizeBytes: maxItemSizeBytes,
	})
	if err != nil {
		return errors.Wrap(err, "create index cache")
	}

	// 创建一个对象存储
	bs, err := store.NewBucketStore(
		logger,     // 日志
		reg,        // 监控注册
		bkt,        // 对应的对象存储的Client
		dataDir,    // 数据保存目录
		indexCache, // 索引缓存，索引中保存的是什么呢？？
		chunkPoolSizeBytes,
		maxSampleCount,
		maxConcurrent,
		verbose,              // 是否进行Debug模式
		blockSyncConcurrency, // 是否允许Block同步上传
		filterConf,           // maxTime、minTime
		relabelConfig,
		advertiseCompatibilityLabel,
	)
	if err != nil {
		return errors.Wrap(err, "create object storage store")
	}

	// bucketStoreReady signals when bucket store is ready.
	bucketStoreReady := make(chan struct{})
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			level.Info(logger).Log("msg", "initializing bucket store")
			begin := time.Now()
			// 初始化对象存储,会删除不存在的数据
			//  map[ulid.ULID]*bucketBlock
			if err := bs.InitialSync(ctx); err != nil {
				close(bucketStoreReady)
				return errors.Wrap(err, "bucket store initial sync")
			}
			level.Info(logger).Log("msg", "bucket store ready", "init_duration", time.Since(begin).String())
			close(bucketStoreReady)

			// 定期同步数据？？
			err := runutil.Repeat(syncInterval, ctx.Done(), func() error {
				if err := bs.SyncBlocks(ctx); err != nil {
					level.Warn(logger).Log("msg", "syncing blocks failed", "err", err)
				}
				return nil
			})

			runutil.CloseWithLogOnErr(logger, bs, "bucket store")
			return err
		}, func(error) {
			cancel()
		})
	}

	// 启动Grpc服务
	// Start query (proxy) gRPC StoreAPI.
	{
		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		s := grpcserver.New(logger, reg, tracer, component, bs,
			grpcserver.WithListen(grpcBindAddr),
			grpcserver.WithGracePeriod(grpcGracePeriod),
			grpcserver.WithTLSConfig(tlsCfg),
		)

		g.Add(func() error {
			<-bucketStoreReady
			statusProber.SetReady()
			return s.ListenAndServe()
		}, func(err error) {
			statusProber.SetNotReady(err)
			s.Shutdown(err)
		})
	}

	level.Info(logger).Log("msg", "starting store node")
	return nil
}

func parseRelabelConfig(contentYaml []byte) ([]*relabel.Config, error) {
	var relabelConfig []*relabel.Config
	if err := yaml.Unmarshal(contentYaml, &relabelConfig); err != nil {
		return nil, errors.Wrap(err, "parsing relabel configuration")
	}

	return relabelConfig, nil
}
