package receive

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	conntrack "github.com/mwitkow/go-conntrack"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	// DefaultTenantHeader is the default header used to designate the tenant making a write request.
	DefaultTenantHeader = "THANOS-TENANT"
	// DefaultReplicaHeader is the default header used to designate the replica count of a write request.
	DefaultReplicaHeader = "THANOS-REPLICA"
)

// conflictErr is returned whenever an operation fails due to any conflict-type error.
var conflictErr = errors.New("conflict")

// Options for the web Handler.
type Options struct {
	Writer            *Writer
	ListenAddress     string
	Registry          prometheus.Registerer
	Endpoint          string
	TenantHeader      string
	ReplicaHeader     string
	ReplicationFactor uint64
	Tracer            opentracing.Tracer
	TLSConfig         *tls.Config
	TLSClientConfig   *tls.Config
}

// Handler serves a Prometheus remote write receiving HTTP endpoint.
type Handler struct {
	client   *http.Client
	logger   log.Logger
	writer   *Writer
	router   *route.Router
	options  *Options
	listener net.Listener

	mtx      sync.RWMutex
	hashring Hashring

	// Metrics.
	forwardRequestsTotal *prometheus.CounterVec
}

func NewHandler(logger log.Logger, o *Options) *Handler {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	transport := http.DefaultTransport.(*http.Transport)
	transport.TLSClientConfig = o.TLSClientConfig
	client := &http.Client{Transport: transport}
	if o.Tracer != nil {
		client.Transport = tracing.HTTPTripperware(logger, client.Transport)
	}

	h := &Handler{
		client:  client,
		logger:  logger,
		writer:  o.Writer,
		router:  route.New(),
		options: o,
		forwardRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_receive_forward_requests_total",
				Help: "The number of forward requests.",
			}, []string{"result"},
		),
	}

	ins := extpromhttp.NewNopInstrumentationMiddleware()
	if o.Registry != nil {
		ins = extpromhttp.NewInstrumentationMiddleware(o.Registry)
		o.Registry.MustRegister(h.forwardRequestsTotal)
	}

	readyf := h.testReady
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		if o.Tracer != nil {
			next = tracing.HTTPMiddleware(o.Tracer, name, logger, http.HandlerFunc(next))
		}
		return ins.NewHandler(name, http.HandlerFunc(next))
	}

	h.router.Post("/api/v1/receive", instrf("receive", readyf(h.receive)))

	return h
}

// SetWriter sets the writer.
// The writer must be set to a non-nil value in order for the
// handler to be ready and usable.
// If the writer is nil, then the handler is marked as not ready.
func (h *Handler) SetWriter(w *Writer) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.writer = w
}

// Hashring sets the hashring for the handler and marks the hashring as ready.
// The hashring must be set to a non-nil value in order for the
// handler to be ready and usable.
// If the hashring is nil, then the handler is marked as not ready.
func (h *Handler) Hashring(hashring Hashring) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.hashring = hashring
}

// Verifies whether the server is ready or not.
func (h *Handler) isReady() bool {
	h.mtx.RLock()
	hr := h.hashring != nil
	sr := h.writer != nil
	h.mtx.RUnlock()
	return sr && hr
}

// Checks if server is ready, calls f if it is, returns 503 if it is not.
func (h *Handler) testReady(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.isReady() {
			f(w, r)
			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := fmt.Fprintf(w, "Service Unavailable")
		if err != nil {
			h.logger.Log("msg", "failed to write to response body", "err", err)
		}
	}
}

// Close stops the Handler.
func (h *Handler) Close() {
	if h.listener != nil {
		runutil.CloseWithLogOnErr(h.logger, h.listener, "receive HTTP listener")
	}
}

// Run serves the HTTP endpoints.
func (h *Handler) Run() error {
	level.Info(h.logger).Log("msg", "Start listening for connections", "address", h.options.ListenAddress)

	var err error
	h.listener, err = net.Listen("tcp", h.options.ListenAddress)
	if err != nil {
		return err
	}

	// Monitor incoming connections with conntrack.
	h.listener = conntrack.NewListener(h.listener,
		conntrack.TrackWithName("http"),
		conntrack.TrackWithTracing())

	errlog := stdlog.New(log.NewStdlibAdapter(level.Error(h.logger)), "", 0)

	httpSrv := &http.Server{
		Handler:   h.router,
		ErrorLog:  errlog,
		TLSConfig: h.options.TLSConfig,
	}

	return httpSrv.Serve(h.listener)
}

// replica encapsulates the replica number of a request and if the request is already replicated.
type replica struct {
	n          uint64
	replicated bool
}

func (h *Handler) receive(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		level.Error(h.logger).Log("msg", "snappy decode error", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var wreq prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var rep replica
	replicaRaw := r.Header.Get(h.options.ReplicaHeader)
	// If the header is empty, we assume the request is not yet replicated.
	if replicaRaw != "" {
		if rep.n, err = strconv.ParseUint(replicaRaw, 10, 64); err != nil {
			http.Error(w, "could not parse replica header", http.StatusBadRequest)
			return
		}
		rep.replicated = true
	}
	// The replica value in the header is zero-indexed, thus we need >=.
	if rep.n >= h.options.ReplicationFactor {
		http.Error(w, "replica count exceeds replication factor", http.StatusBadRequest)
		return
	}

	tenant := r.Header.Get(h.options.TenantHeader)

	// Forward any time series as necessary. All time series
	// destined for the local node will be written to the receiver.
	// Time series will be replicated as necessary.
	if err := h.forward(r.Context(), tenant, rep, &wreq); err != nil {
		if countCause(err, isConflict) > 0 {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// forward accepts a write request, batches its time series by
// corresponding endpoint, and forwards them in parallel to the
// correct endpoint. Requests destined for the local node are written
// the the local receiver. For a given write request, at most one outgoing
// write request will be made to every other node in the hashring,
// unless the request needs to be replicated.
// The function only returns when all requests have finished
// or the context is canceled.
// 转发写入请求，按照hash进行数据转发，保证一个TSDB处理自己的数据
func (h *Handler) forward(ctx context.Context, tenant string, r replica, wreq *prompb.WriteRequest) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)

	// It is possible that hashring is ready in testReady() but unready now,
	// so need to lock here.
	h.mtx.RLock()
	if h.hashring == nil {
		h.mtx.RUnlock()
		return errors.New("hashring is not ready")
	}

	// Batch all of the time series in the write request
	// into several smaller write requests that are
	// grouped by target endpoint. This ensures that
	// for any incoming write request to a node,
	// at most one outgoing write request will be made
	// to every other node in the hashring, rather than
	// one request per time series.
	for i := range wreq.Timeseries {
		endpoint, err := h.hashring.GetN(tenant, &wreq.Timeseries[i], r.n)
		if err != nil {
			h.mtx.RUnlock()
			return err
		}
		if _, ok := wreqs[endpoint]; !ok {
			wreqs[endpoint] = &prompb.WriteRequest{}
			replicas[endpoint] = r
		}
		wr := wreqs[endpoint]
		wr.Timeseries = append(wr.Timeseries, wreq.Timeseries[i])
	}
	h.mtx.RUnlock()

	return h.parallelizeRequests(ctx, tenant, replicas, wreqs)
}

// parallelizeRequests parallelizes a given set of write requests.
// The function only returns when all requests have finished
// or the context is canceled.
func (h *Handler) parallelizeRequests(ctx context.Context, tenant string, replicas map[string]replica, wreqs map[string]*prompb.WriteRequest) error {
	ec := make(chan error)
	defer close(ec)
	// We don't wan't to use a sync.WaitGroup here because that
	// introduces an unnecessary second synchronization mechanism,
	// the first being the error chan. Plus, it saves us a goroutine
	// as in order to collect errors while doing wg.Wait, we would
	// need a separate error collection goroutine.
	var n int
	for endpoint := range wreqs {
		n++
		// If the request is not yet replicated, let's replicate it.
		// If the replication factor isn't greater than 1, let's
		// just forward the requests.
		if !replicas[endpoint].replicated && h.options.ReplicationFactor > 1 {
			go func(endpoint string) {
				ec <- h.replicate(ctx, tenant, wreqs[endpoint])
			}(endpoint)
			continue
		}
		// If the endpoint for the write request is the
		// local node, then don't make a request but store locally.
		// By handing replication to the local node in the same
		// function as replication to other nodes, we can treat
		// a failure to write locally as just another error that
		// can be ignored if the replication factor is met.
		if endpoint == h.options.Endpoint {
			go func(endpoint string) {
				var err error
				h.mtx.RLock()
				if h.writer == nil {
					err = errors.New("storage is not ready")
				} else {
					err = h.writer.Write(wreqs[endpoint])
					// When a MultiError is added to another MultiError, the error slices are concatenated, not nested.
					// To avoid breaking the counting logic, we need to flatten the error.
					if errs, ok := err.(terrors.MultiError); ok {
						if countCause(errs, isConflict) > 0 {
							err = errors.Wrap(conflictErr, errs.Error())
						} else {
							err = errors.New(errs.Error())
						}
					}
				}
				h.mtx.RUnlock()
				if err != nil {
					level.Error(h.logger).Log("msg", "storing locally", "err", err, "endpoint", endpoint)
				}
				ec <- err
			}(endpoint)
			continue
		}
		// Make a request to the specified endpoint.
		go func(endpoint string) {
			buf, err := proto.Marshal(wreqs[endpoint])
			if err != nil {
				level.Error(h.logger).Log("msg", "marshaling proto", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(snappy.Encode(nil, buf)))
			if err != nil {
				level.Error(h.logger).Log("msg", "creating request", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			req.Header.Add(h.options.TenantHeader, tenant)
			req.Header.Add(h.options.ReplicaHeader, strconv.FormatUint(replicas[endpoint].n, 10))

			// Increment the counters as necessary now that
			// the requests will go out.
			defer func() {
				if err != nil {
					h.forwardRequestsTotal.WithLabelValues("error").Inc()
					return
				}
				h.forwardRequestsTotal.WithLabelValues("success").Inc()
			}()

			// Create a span to track the request made to another receive node.
			span, ctx := tracing.StartSpan(ctx, "thanos_receive_forward")
			defer span.Finish()

			// Actually make the request against the endpoint
			// we determined should handle these time series.
			var res *http.Response
			res, err = h.client.Do(req.WithContext(ctx))
			if err != nil {
				level.Error(h.logger).Log("msg", "forwarding request", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			if res.StatusCode != http.StatusOK {
				err = errors.New(strconv.Itoa(res.StatusCode))
				level.Error(h.logger).Log("msg", "forwarding returned non-200 status", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			ec <- nil
		}(endpoint)
	}

	// Collect any errors from forwarding the time series.
	// Rather than doing a wg.Wait here, we decrement a counter
	// for every error received on the chan. This simplifies
	// error collection and avoids data races with a separate
	// error collection goroutine.
	var errs terrors.MultiError
	for ; n > 0; n-- {
		if err := <-ec; err != nil {
			errs.Add(err)
		}
	}

	return errs.Err()
}

// replicate replicates a write request to (replication-factor) nodes
// selected by the tenant and time series.
// The function only returns when all replication requests have finished
// or the context is canceled.
func (h *Handler) replicate(ctx context.Context, tenant string, wreq *prompb.WriteRequest) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)
	var i uint64

	// It is possible that hashring is ready in testReady() but unready now,
	// so need to lock here.
	h.mtx.RLock()
	if h.hashring == nil {
		h.mtx.RUnlock()
		return errors.New("hashring is not ready")
	}

	for i = 0; i < h.options.ReplicationFactor; i++ {
		endpoint, err := h.hashring.GetN(tenant, &wreq.Timeseries[0], i)
		if err != nil {
			h.mtx.RUnlock()
			return err
		}
		wreqs[endpoint] = wreq
		replicas[endpoint] = replica{i, true}
	}
	h.mtx.RUnlock()

	err := h.parallelizeRequests(ctx, tenant, replicas, wreqs)
	if errs, ok := err.(terrors.MultiError); ok {
		if uint64(countCause(errs, isConflict)) >= (h.options.ReplicationFactor+1)/2 {
			return errors.Wrap(conflictErr, "did not meet replication threshold")
		}
		if uint64(len(errs)) >= (h.options.ReplicationFactor+1)/2 {
			return errors.Wrap(err, "did not meet replication threshold")
		}
		return nil
	}
	return errors.Wrap(err, "could not replicate write request")
}

// countCause counts the number of errors within the given error
// whose causes satisfy the given function.
// countCause will inspect the error's cause or, if the error is a MultiError,
// the cause of each contained error but will not traverse any deeper.
func countCause(err error, f func(error) bool) int {
	errs, ok := err.(terrors.MultiError)
	if !ok {
		errs = []error{err}
	}
	var n int
	for i := range errs {
		if f(errors.Cause(errs[i])) {
			n++
		}
	}
	return n
}

// isConflict returns whether or not the given error represents a conflict.
func isConflict(err error) bool {
	if err == nil {
		return false
	}
	return err == conflictErr || err == storage.ErrDuplicateSampleForTimestamp || err == storage.ErrOutOfOrderSample || err == storage.ErrOutOfBounds || err.Error() == strconv.Itoa(http.StatusConflict)
}
