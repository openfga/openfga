package graph

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/telemetry"
)

// DispatchThrottlingCheckResolverConfig encapsulates configuration for rate limited check resolver
type DispatchThrottlingCheckResolverConfig struct {
	TimerTickerFrequency time.Duration
	Level                uint32
	Rate                 uint32
}

// DispatchThrottlingCheckResolver will prioritize requests with fewer dispatches over
// requests with more dispatches.
// Initially, request's dispatches will not be throttled and will be processed
// immediately. When the number of request dispatches is above the Level, the dispatches are placed
// in the throttling queue. One item form the throttling queue will be processed every Nth ticker
// (configured via config's Rate parameter).
// This allows a check / list objects request to be gradually throttled.
type DispatchThrottlingCheckResolver struct {
	delegate        CheckResolver
	config          DispatchThrottlingCheckResolverConfig
	ticker          *time.Ticker
	throttlingQueue chan bool
	done            chan bool
}

var _ CheckResolver = (*DispatchThrottlingCheckResolver)(nil)

var (
	rateLimitedCheckResolverDelayMsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "rate_limited_check_resolver_delay_ms",
		Help:                            "Time spent waiting for rate limited check resolver",
		Buckets:                         []float64{1, 3, 5, 10, 25, 50, 100, 1000, 5000}, // Milliseconds. Upper bound is config.UpstreamTimeout.
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"grpc_service", "grpc_method"})
)

func NewDispatchThrottlingCheckResolver(
	config DispatchThrottlingCheckResolverConfig) *DispatchThrottlingCheckResolver {
	ticker := time.NewTicker(config.TimerTickerFrequency)
	rateLimitedCheckResolver := &DispatchThrottlingCheckResolver{
		config:          config,
		ticker:          ticker,
		throttlingQueue: make(chan bool),
		done:            make(chan bool),
	}
	rateLimitedCheckResolver.delegate = rateLimitedCheckResolver
	go rateLimitedCheckResolver.runTicker()
	return rateLimitedCheckResolver
}

func (r *DispatchThrottlingCheckResolver) SetDelegate(delegate CheckResolver) {
	r.delegate = delegate
}

func (r *DispatchThrottlingCheckResolver) Close() {
	r.done <- true
}

func (r *DispatchThrottlingCheckResolver) nonBlockingSend(signalChan chan bool) {
	select {
	case signalChan <- true:
		// message sent
	default:
		// message dropped
	}
}

func (r *DispatchThrottlingCheckResolver) handleTimeTick(throttleCounter uint32) uint32 {
	throttleCounter++
	if throttleCounter >= r.config.Rate {
		throttleCounter = 0
		r.nonBlockingSend(r.throttlingQueue)
	}
	return throttleCounter
}

func (r *DispatchThrottlingCheckResolver) runTicker() {
	throttleCounter := uint32(0)
	for {
		select {
		case <-r.done:
			r.ticker.Stop()
			close(r.done)
			close(r.throttlingQueue)
			return
		case <-r.ticker.C:
			throttleCounter = r.handleTimeTick(throttleCounter)
		}
	}
}

func (r *DispatchThrottlingCheckResolver) ResolveCheck(ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	currentNumDispatch := req.DispatchCounter.Add(1)

	if currentNumDispatch > r.config.Level {
		start := time.Now()
		<-r.throttlingQueue
		end := time.Now()
		timeWaiting := end.Sub(start).Milliseconds()

		rpcInfo := telemetry.RPCInfoFromContext(ctx)
		rateLimitedCheckResolverDelayMsHistogram.WithLabelValues(
			rpcInfo.Service,
			rpcInfo.Method,
		).Observe(float64(timeWaiting))
	}

	return r.delegate.ResolveCheck(ctx, req)
}
