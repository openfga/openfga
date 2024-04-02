package throttler

import (
	"context"
	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"
)

// DispatchThrottlingConfig encapsulates configuration for dispatch throttling check resolver
type DispatchThrottlingConfig struct {
	Frequency time.Duration
	Threshold uint32
}

type Ticker interface {
}

// DispatchThrottler will prioritize requests with fewer dispatches over
// requests with more dispatches.
// Initially, request's dispatches will not be throttled and will be processed
// immediately. When the number of request dispatches is above the Threshold, the dispatches are placed
// in the throttling queue. One item form the throttling queue will be processed ticker.
// This allows a check / list objects request to be gradually throttled.
type DispatchThrottler struct {
	config          DispatchThrottlingConfig
	ticker          *time.Ticker
	throttlingQueue chan struct{}
	done            chan struct{}
}

func NewDispatchThrottlingCheckResolver(
	config DispatchThrottlingConfig) *DispatchThrottler {
	dispatchThrottler := &DispatchThrottler{
		config:          config,
		ticker:          time.NewTicker(config.Frequency),
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

// ReleaseDispatch is used for releasing a dispatch from the queue.
func (r *DispatchThrottler) ReleaseDispatch() {
	r.nonBlockingSend(r.throttlingQueue)
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

func (r *DispatchThrottler) Throttle(ctx context.Context, currentNumDispatch uint32) {
	if currentNumDispatch > r.config.Threshold {
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
}
