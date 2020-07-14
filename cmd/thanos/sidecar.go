package main

import (
	"context"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	thanosmodel "github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/reloader"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tls"
	"gopkg.in/alecthomas/kingpin.v2"
)

// 注册组件，传入命令行参数
func registerSidecar(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command(component.Sidecar.String(), "sidecar for Prometheus server")

	// HTTP参数和GRPC参数
	httpBindAddr, httpGracePeriod := regHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := regGRPCFlags(cmd)

	// Promtheus地址
	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API. For better performance use local network.").
		Default("http://localhost:9090").URL()

	// Prometheus读超时时间
	promReadyTimeout := cmd.Flag("prometheus.ready_timeout", "Maximum time to wait for the Prometheus instance to start up").
		Default("10m").Duration()

	// Prometheus数据保存路径
	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	reloaderCfgFile := cmd.Flag("reloader.config-file", "Config file watched by the reloader.").
		Default("").String()

	reloaderCfgOutputFile := cmd.Flag("reloader.config-envsubst-file", "Output file for environment variable substituted config file.").
		Default("").String()

	reloaderRuleDirs := cmd.Flag("reloader.rule-dir", "Rule directories for the reloader to refresh (repeated field).").Strings()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", false)

	uploadCompacted := cmd.Flag("shipper.upload-compacted", "[Experimental] If true sidecar will try to upload compacted blocks as well. Useful for migration purposes. Works only if compaction is disabled on Prometheus.").Default("false").Hidden().Bool()

	minTime := thanosmodel.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to serve. Thanos sidecar will serve only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))

	m[component.Sidecar.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		rl := reloader.New(
			log.With(logger, "component", "reloader"),
			reloader.ReloadURLFromBase(*promURL),
			*reloaderCfgFile,
			*reloaderCfgOutputFile,
			*reloaderRuleDirs,
		)

		// 运行组件
		return runSidecar(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
			*promURL,
			*promReadyTimeout,
			*dataDir,
			objStoreConfig,
			rl,
			*uploadCompacted,
			component.Sidecar,
			*minTime,
		)
	}
}

func runSidecar(
	g *run.Group, // 进程组？？
	logger log.Logger, // 日志组件
	reg *prometheus.Registry, // 自身监控注册
	tracer opentracing.Tracer,
	grpcBindAddr string, // grpc的监听地址
	grpcGracePeriod time.Duration,
	grpcCert string, // Grpc的一些HTTPS相关的设置
	grpcKey string,
	grpcClientCA string,
	httpBindAddr string, // HTTP 相关的一些配置
	httpGracePeriod time.Duration,
	promURL *url.URL, // prometheus的地址，拉取一些数据，是否把Prometheus作为RemoteRead??
	promReadyTimeout time.Duration,
	dataDir string, // 数据保存的目录
	objStoreConfig *extflag.PathOrContent,
	reloader *reloader.Reloader,
	uploadCompacted bool, // 是否上传历史已经压缩过的数据
	comp component.Component,
	limitMinTime thanosmodel.TimeOrDurationValue,
) error {
	// Prometheus的Meta数据
	var m = &promMetadata{
		promURL: promURL,

		// Start out with the full time range. The shipper will constrain it later.
		// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
		mint: limitMinTime.PrometheusTimestamp(),
		maxt: math.MaxInt64,

		limitMinTime: limitMinTime,
	}

	// 对象存储的配置，包括地址、账号、密码等
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return errors.Wrap(err, "getting object store config")
	}

	// 默认不开启数据上传对象存储的功能
	// 只有当把对象存储配置上以后，才会开启这个功能
	var uploads = true
	if len(confContentYaml) == 0 {
		level.Info(logger).Log("msg", "no supported bucket was configured, uploads will be disabled")
		uploads = false
	}

	// Initiate HTTP listener providing metrics endpoint and readiness/liveness probes.
	// 初始化和启动HTTP 服务,封装了状态探针的HTTP服务
	statusProber := prober.NewProber(comp, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	srv := httpserver.New(logger, reg, comp, statusProber,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
	)

	g.Add(srv.ListenAndServe, srv.Shutdown)

	// Setup all the concurrent groups.
	{
		promUp := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_prometheus_up",
			Help: "Boolean indicator whether the sidecar can reach its Prometheus peer.",
		})
		lastHeartbeat := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_last_heartbeat_success_time_seconds",
			Help: "Second timestamp of the last successful heartbeat.",
		})
		// 注册心跳服务？？
		reg.MustRegister(promUp, lastHeartbeat)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			// Only check Prometheus's flags when upload is enabled.
			// 如果需要上传数据，那么需要校验prometheus里面的东西
			if uploads {
				// Check prometheus's flags to ensure sane sidecar flags.
				if err := validatePrometheus(ctx, logger, m); err != nil {
					return errors.Wrap(err, "validate Prometheus flags")
				}
			}

			// Blocking query of external labels before joining as a Source Peer into gossip.
			// We retry infinitely until we reach and fetch labels from our Prometheus.
			// 从Prometheus中拉取一些label
			err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
				// 从Prometheus中拉取一些配置，暂时不知道是做什么用的
				if err := m.UpdateLabels(ctx, logger); err != nil {
					level.Warn(logger).Log(
						"msg", "failed to fetch initial external labels. Is Prometheus running? Retrying",
						"err", err,
					)
					promUp.Set(0)
					statusProber.SetNotReady(err)
					return err
				}

				level.Info(logger).Log(
					"msg", "successfully loaded prometheus external labels",
					"external_labels", m.Labels().String(),
				)
				// 设置服务状态为ready
				promUp.Set(1)
				statusProber.SetReady()
				lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "initial external labels query")
			}

			if len(m.Labels()) == 0 {
				return errors.New("no external labels configured on Prometheus server, uniquely identifying external labels must be configured")
			}

			// Periodically query the Prometheus config. We use this as a heartbeat as well as for updating
			// the external labels we apply.
			// 每30s进行一次心跳上报
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer iterCancel()

				if err := m.UpdateLabels(iterCtx, logger); err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					promUp.Set(0)
				} else {
					promUp.Set(1)
					lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				}

				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return reloader.Watch(ctx)
		}, func(error) {
			cancel()
		})
	}

	// 下面这段代码是启动我们的Grpc服务
	{
		promStore, err := store.NewPrometheusStore(
			logger, nil, promURL, component.Sidecar, m.Labels, m.Timestamps)
		if err != nil {
			return errors.Wrap(err, "create Prometheus store")
		}

		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		// 启动Grpc服务
		// 有如下的几个方法
		// 服务信息 Info(context.Context, *InfoRequest) (*InfoResponse, error)
		// 时序数据查询 Series(*SeriesRequest, Store_SeriesServer) error
		// tagK 查询  LabelNames(context.Context, *LabelNamesRequest) (*LabelNamesResponse, error)
		// tagV 查询  LabelValues(context.Context, *LabelValuesRequest) (*LabelValuesResponse, error)
		s := grpcserver.New(logger, reg, tracer, comp, promStore,
			grpcserver.WithListen(grpcBindAddr),
			grpcserver.WithGracePeriod(grpcGracePeriod),
			grpcserver.WithTLSConfig(tlsCfg),
		)
		// 启动Grpc服务，进行监听，等待请求，一般是把Slidecar作为Proxy组件的时候有用
		g.Add(func() error {
			statusProber.SetReady()
			return s.ListenAndServe()
		}, func(err error) {
			statusProber.SetNotReady(err)
			s.Shutdown(err)
		})
	}

	// 启动Upload服务
	if uploads {
		// The background shipper continuously scans the data directory and uploads
		// new blocks to Google Cloud Storage or an S3-compatible storage service.
		// 根据配置，创建后端对象存储
		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Sidecar.String())
		if err != nil {
			return err
		}

		// Ensure we close up everything properly.
		defer func() {
			if err != nil {
				runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			}
		}()

		if err := promclient.IsWALDirAccesible(dataDir); err != nil {
			level.Error(logger).Log("err", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			extLabelsCtx, cancel := context.WithTimeout(ctx, promReadyTimeout)
			defer cancel()

			// 重试检查两次，Prometheus的配置中必须要有external_labels
			// 可以标记一下数据来源于哪个region，哪个replica
			if err := runutil.Retry(2*time.Second, extLabelsCtx.Done(), func() error {
				if len(m.Labels()) == 0 {
					return errors.New("not uploading as no external labels are configured yet - is Prometheus healthy/reachable?")
				}
				return nil
			}); err != nil {
				return errors.Wrapf(err, "aborting as no external labels found after waiting %s", promReadyTimeout)
			}

			// 数据上传的是由Shipper负责的，中文翻译是发货人
			var s *shipper.Shipper
			if uploadCompacted {
				s = shipper.NewWithCompacted(logger, reg, dataDir, bkt, m.Labels, metadata.SidecarSource)
			} else {
				s = shipper.New(logger, reg, dataDir, bkt, m.Labels, metadata.SidecarSource)
			}

			// 每30s进行一次文件夹数据同步
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				if uploaded, err := s.Sync(ctx); err != nil {
					level.Warn(logger).Log("err", err, "uploaded", uploaded)
				}

				minTime, _, err := s.Timestamps()
				if err != nil {
					level.Warn(logger).Log("msg", "reading timestamps failed", "err", err)
					return nil
				}
				// 更新一下时间
				m.UpdateTimestamps(minTime, math.MaxInt64)
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	level.Info(logger).Log("msg", "starting sidecar")
	return nil
}

// 校验maxTime和minTime,其实这可以把prometheus的数据目录也拉处理，少一次配置和维护
func validatePrometheus(ctx context.Context, logger log.Logger, m *promMetadata) error {
	var (
		flagErr error
		flags   promclient.Flags
	)

	if err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
		if flags, flagErr = promclient.ConfiguredFlags(ctx, logger, m.promURL); flagErr != nil && flagErr != promclient.ErrFlagEndpointNotFound {
			level.Warn(logger).Log("msg", "failed to get Prometheus flags. Is Prometheus running? Retrying", "err", flagErr)
			return errors.Wrapf(flagErr, "fetch Prometheus flags")
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "fetch Prometheus flags")
	}

	if flagErr != nil {
		level.Warn(logger).Log("msg", "failed to check Prometheus flags, due to potentially older Prometheus. No extra validation is done.", "err", flagErr)
		return nil
	}

	// Check if compaction is disabled.
	if flags.TSDBMinTime != flags.TSDBMaxTime {
		return errors.Errorf("found that TSDB Max time is %s and Min time is %s. "+
			"Compaction needs to be disabled (storage.tsdb.min-block-duration = storage.tsdb.max-block-duration)", flags.TSDBMaxTime, flags.TSDBMinTime)
	}

	// Check if block time is 2h.
	if flags.TSDBMinTime != model.Duration(2*time.Hour) {
		level.Warn(logger).Log("msg", "found that TSDB block time is not 2h. Only 2h block time is recommended.", "block-time", flags.TSDBMinTime)
	}

	return nil
}

type promMetadata struct {
	promURL *url.URL

	mtx    sync.Mutex
	mint   int64
	maxt   int64
	labels labels.Labels // 从Prometheus中拉到的一些tag

	limitMinTime thanosmodel.TimeOrDurationValue
}

func (s *promMetadata) UpdateLabels(ctx context.Context, logger log.Logger) error {
	elset, err := promclient.ExternalLabels(ctx, logger, s.promURL)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.labels = elset
	return nil
}

func (s *promMetadata) UpdateTimestamps(mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if mint < s.limitMinTime.PrometheusTimestamp() {
		mint = s.limitMinTime.PrometheusTimestamp()
	}

	s.mint = mint
	s.maxt = maxt
}

func (s *promMetadata) Labels() labels.Labels {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.labels
}

func (s *promMetadata) LabelsPB() []storepb.Label {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	lset := make([]storepb.Label, 0, len(s.labels))
	for _, l := range s.labels {
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return lset
}

func (s *promMetadata) Timestamps() (mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.mint, s.maxt
}
