package graph

import (
	"context"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/dispatcher"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/telemetry"
)

// DispatchThrottlingCheckResolverConfig encapsulates configuration for dispatch throttling check resolver
type DispatchThrottlingCheckResolverConfig struct {
	Frequency time.Duration
	Threshold uint32
}

// DispatchThrottlingCheckResolver will prioritize requests with fewer dispatches over
// requests with more dispatches.
// Initially, request's dispatches will not be throttled and will be processed
// immediately. When the number of request dispatches is above the Threshold, the dispatches are placed
// in the throttling queue. One item form the throttling queue will be processed ticker.
// This allows a check / list objects request to be gradually throttled.
type DispatchThrottlingCheckResolver struct {
	delegate        dispatcher.Dispatcher
	config          DispatchThrottlingCheckResolverConfig
	ticker          *time.Ticker
	throttlingQueue chan struct{}
	done            chan struct{}
}

var _ dispatcher.Dispatcher = (*DispatchThrottlingCheckResolver)(nil)

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

func NewDispatchThrottlingCheckResolver(
	config DispatchThrottlingCheckResolverConfig) *DispatchThrottlingCheckResolver {
	dispatchThrottlingCheckResolver := &DispatchThrottlingCheckResolver{
		config:          config,
		ticker:          time.NewTicker(config.Frequency),
		throttlingQueue: make(chan struct{}),
		done:            make(chan struct{}),
	}
	dispatchThrottlingCheckResolver.delegate = dispatchThrottlingCheckResolver
	go dispatchThrottlingCheckResolver.runTicker()
	return dispatchThrottlingCheckResolver
}

func (r *DispatchThrottlingCheckResolver) SetDelegate(delegate dispatcher.Dispatcher) {
	r.delegate = delegate
}

func (r *DispatchThrottlingCheckResolver) GetDelegate() dispatcher.Dispatcher {
	return r.delegate
}

func (r *DispatchThrottlingCheckResolver) Close() {
	r.done <- struct{}{}
}

func (r *DispatchThrottlingCheckResolver) nonBlockingSend(signalChan chan struct{}) {
	select {
	case signalChan <- struct{}{}:
		// message sent
	default:
		// message dropped
	}
}

func (r *DispatchThrottlingCheckResolver) runTicker() {
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

func (r *DispatchThrottlingCheckResolver) Dispatch(ctx context.Context, req *openfgav1.BaseRequest, metadata *openfgav1.DispatchMetadata, additionalParameters any) (*openfgav1.BaseResponse, *openfgav1.DispatchMetadata, error) {
	log.Printf("Throttling Dispatcher - %s running in %s", req.GetDispatchedCheckRequest(), serverconfig.ServerName)
	currentNumDispatch := metadata.GetDispatchCount()

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

	return r.delegate.Dispatch(ctx, req, metadata, additionalParameters)
}
