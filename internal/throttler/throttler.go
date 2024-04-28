//go:generate mockgen -source throttler.go -destination ../mocks/mock_throttler.go -package mocks

package throttler

import (
	"context"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/telemetry"
)

type Throttler interface {
	Close()
	Throttle(context.Context)
}

type NoopThrottler struct{}

var _ Throttler = (*NoopThrottler)(nil)

func (r *NoopThrottler) Throttle(ctx context.Context) {
}

func (r *NoopThrottler) Close() {
}

// DispatchThrottler implements a throttling mechanism that can be used to control the rate of dispatched sub problems in FGA queries.
// Throttling will start to kick in when the dispatch count exceeds the configured dispatch threshold.
type DispatchThrottler struct {
	ticker          *time.Ticker
	throttlingQueue chan struct{}
	done            chan struct{}
}

// NewDispatchThrottler constructs a DispatchThrottler which can be used to control the rate of dispatched sub problems in FGA queries.
func NewDispatchThrottler(frequency time.Duration) Throttler {
	return newDispatchThrottler(frequency)
}

// Returns a DispatchThrottler instead of Throttler for testing purpose to be used internally.
func newDispatchThrottler(frequency time.Duration) *DispatchThrottler {
	dispatchThrottler := &DispatchThrottler{
		ticker:          time.NewTicker(frequency),
		throttlingQueue: make(chan struct{}),
		done:            make(chan struct{}),
	}
	go dispatchThrottler.runTicker()
	return dispatchThrottler
}

var (
	dispatchThrottlingResolverDelayMsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "dispatch_throttling_resolver_delay_ms",
		Help:                            "Time spent waiting for dispatch throttling resolver",
		Buckets:                         []float64{1, 3, 5, 10, 25, 50, 100, 1000, 5000}, // Milliseconds. Upper bound is config.UpstreamTimeout.
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"grpc_service", "grpc_method"})
)

func (r *DispatchThrottler) nonBlockingSend(signalChan chan struct{}) {
	select {
	case signalChan <- struct{}{}:
		// message sent
	default:
		// message dropped
	}
}

func (r *DispatchThrottler) runTicker() {
	for {
		select {
		case <-r.done:
			r.ticker.Stop()
			close(r.done)
			close(r.throttlingQueue)
			return
		case <-r.ticker.C:
			r.nonBlockingSend(r.throttlingQueue)
		}
	}
}

func (r *DispatchThrottler) Close() {
	r.done <- struct{}{}
}

// Throttle provides a synchronous blocking mechanism that will block if the currentNumDispatch exceeds the configured dispatch threshold.
// It will block until a value is produced on the underlying throttling queue channel,
// which is produced by periodically sending a value on the channel based on the configured ticker frequency.
func (r *DispatchThrottler) Throttle(ctx context.Context) {
	grpc_ctxtags.Extract(ctx).Set(telemetry.Throttled, true)

	start := time.Now()
	<-r.throttlingQueue
	end := time.Now()
	timeWaiting := end.Sub(start).Milliseconds()

	rpcInfo := telemetry.RPCInfoFromContext(ctx)
	dispatchThrottlingResolverDelayMsHistogram.WithLabelValues(
		rpcInfo.Service,
		rpcInfo.Method,
	).Observe(float64(timeWaiting))
}
