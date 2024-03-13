package graph

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/telemetry"
)

// RateLimitedCheckResolverConfig encapsulates configuration for rate limited check resolver
type RateLimitedCheckResolverConfig struct {
	TimerTickerFrequency time.Duration
	MediumPriorityLevel  uint32
	MediumPriorityShaper uint32
	LowPriorityLevel     uint32
	LowPriorityShaper    uint32
}

// RateLimitedCheckResolver will prioritize requests with fewer dispatches over
// requests with more dispatches.
// Initially, request's dispatches will not be throttled and will be processed
// immediately. As the number of dispatches increases (to beyond the config's MediumPriorityLevel),
// selectively number (configure via config's MediumPriorityShaper) will be throttled by placing the
// dispatch in the medium priority queue. One item from the medium priority queue will be processed
// every ticker across the entire server. Request's dispatches will gradually be more frequently throttled as
// the number of dispatches increase until every dispatch will be throttled by being placed in the medium priority
// queue. When the number of request dispatches is above the LowPriorityLevel, the dispatches are placed
// in the low priority queue. One item form the low priority queue will be processed every Nth ticker
// (configured via config's LowPriorityShaper parameter).
// This allows a check / list objects request to be gradually throttled.
type RateLimitedCheckResolver struct {
	delegate         CheckResolver
	config           RateLimitedCheckResolverConfig
	ticker           *time.Ticker
	lowPriorityQueue chan bool
	medPriorityQueue chan bool
	done             chan bool

	// these are helper config value to reduce calculation needed for medium shaper
	mediumPriorityLevel2      uint32
	mediumPriorityLevel3      uint32
	mediumPriorityLevel4      uint32
	mediumPriorityShaperFreq2 uint32
	mediumPriorityShaperFreq3 uint32
}

var _ CheckResolver = (*RateLimitedCheckResolver)(nil)

var (
	rateLimitedCheckResolverDelayMsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "rate_limited_check_resolver_delay_ms",
		Help:                            "Time spent waiting for rate limited check resolver",
		Buckets:                         []float64{1, 3, 5, 10, 25, 50, 100, 1000, 5000}, // Milliseconds. Upper bound is config.UpstreamTimeout.
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"grpc_service", "grpc_method", "queue_name"})
)

func NewRateLimitedCheckResolver(
	config RateLimitedCheckResolverConfig) *RateLimitedCheckResolver {
	ticker := time.NewTicker(config.TimerTickerFrequency)
	rateLimitedCheckResolver := &RateLimitedCheckResolver{
		config:           config,
		ticker:           ticker,
		lowPriorityQueue: make(chan bool),
		medPriorityQueue: make(chan bool),
		done:             make(chan bool),
	}
	rateLimitedCheckResolver.setMediumConfig(config)
	rateLimitedCheckResolver.delegate = rateLimitedCheckResolver
	go rateLimitedCheckResolver.runTicker()
	return rateLimitedCheckResolver
}

func (r *RateLimitedCheckResolver) setMediumConfig(config RateLimitedCheckResolverConfig) {
	mediumRange := config.LowPriorityLevel - config.MediumPriorityLevel
	r.mediumPriorityLevel2 = config.MediumPriorityLevel + mediumRange/8
	r.mediumPriorityLevel3 = config.MediumPriorityLevel + mediumRange/4
	r.mediumPriorityLevel4 = config.MediumPriorityLevel + mediumRange/2
	r.mediumPriorityShaperFreq2 = config.MediumPriorityShaper / 2
	r.mediumPriorityShaperFreq3 = config.MediumPriorityShaper / 4
}

func (r *RateLimitedCheckResolver) SetDelegate(delegate CheckResolver) {
	r.delegate = delegate
}

func (r *RateLimitedCheckResolver) Close() {
	r.done <- true
}

func (r *RateLimitedCheckResolver) nonBlockingSend(signalChan chan bool) {
	select {
	case signalChan <- true:
		// message sent
	default:
		// message dropped
	}
}

func (r *RateLimitedCheckResolver) handleTimeTick(lowPriorityQueueCounter uint32) uint32 {
	lowPriorityQueueCounter++
	r.nonBlockingSend(r.medPriorityQueue)
	if lowPriorityQueueCounter >= r.config.LowPriorityShaper {
		lowPriorityQueueCounter = 0
		r.nonBlockingSend(r.lowPriorityQueue)
	}
	return lowPriorityQueueCounter
}

func (r *RateLimitedCheckResolver) runTicker() {
	lowPriorityQueueCounter := uint32(0)
	for {
		select {
		case <-r.done:
			r.ticker.Stop()
			close(r.done)
			close(r.medPriorityQueue)
			close(r.lowPriorityQueue)
			return
		case <-r.ticker.C:
			lowPriorityQueueCounter = r.handleTimeTick(lowPriorityQueueCounter)
		}
	}
}

func (r *RateLimitedCheckResolver) shouldWait(currentNumDispatch uint32) bool {
	delta := currentNumDispatch - r.config.MediumPriorityLevel
	if currentNumDispatch < r.mediumPriorityLevel2 {
		return delta%(r.config.MediumPriorityShaper) == 0
	}
	if currentNumDispatch < r.mediumPriorityLevel3 {
		return delta%(r.mediumPriorityShaperFreq2) == 0
	}
	if currentNumDispatch < r.mediumPriorityLevel4 {
		return delta%(r.mediumPriorityShaperFreq3) == 0
	}
	return true
}

func (r *RateLimitedCheckResolver) ResolveCheck(ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	start := time.Now()
	queueName := "high_priority"

	currentNumDispatch := req.DispatchCounter.Add(1)
	if currentNumDispatch > r.config.MediumPriorityLevel {
		if currentNumDispatch > r.config.LowPriorityLevel {
			queueName = "low_priority"
			<-r.lowPriorityQueue
		} else {
			if r.shouldWait(currentNumDispatch) {
				queueName = "medium_priority"
				<-r.medPriorityQueue
			}
		}
	}

	end := time.Now()
	timeWaiting := end.Sub(start).Milliseconds()
	rpcInfo := telemetry.RPCInfoFromContext(ctx)
	rateLimitedCheckResolverDelayMsHistogram.WithLabelValues(
		rpcInfo.Service,
		rpcInfo.Method,
		queueName,
	).Observe(float64(timeWaiting))

	return r.delegate.ResolveCheck(ctx, req)
}
