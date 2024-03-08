package graph

import (
	"context"
	"time"
)

type RateLimitedCheckResolverConfig struct {
	TimerTickerFrequency time.Duration
	MediumPriorityLevel  uint32
	MediumPriorityShaper uint32
	LowPriorityLevel     uint32
	LowPriorityShaper    uint32
}

// RateLimitedCheckResolver will prioritize
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

func NewRateLimitedCheckResolver(
	config RateLimitedCheckResolverConfig) *RateLimitedCheckResolver {
	ticker := time.NewTicker(config.TimerTickerFrequency)
	rateLimitedCheckResolver := &RateLimitedCheckResolver{
		config:           config,
		ticker:           ticker,
		lowPriorityQueue: make(chan bool, 1),
		medPriorityQueue: make(chan bool, 1),
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

func (r *RateLimitedCheckResolver) runTicker() {
	count := uint32(0)
	for {
		select {
		case <-r.done:
			r.ticker.Stop()
			close(r.done)
			close(r.medPriorityQueue)
			close(r.lowPriorityQueue)
			return
		case <-r.ticker.C:
			count++
			r.nonBlockingSend(r.medPriorityQueue)
			if count >= r.config.LowPriorityShaper {
				count = 0
				r.nonBlockingSend(r.lowPriorityQueue)
			}
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
	currentNumDispatch := req.DispatchCounter.Add(1)
	if currentNumDispatch > r.config.MediumPriorityLevel {
		if currentNumDispatch >= r.config.LowPriorityLevel {
			<-r.lowPriorityQueue
		} else {
			if r.shouldWait(currentNumDispatch) {
				<-r.medPriorityQueue
			}
		}
	}
	return r.delegate.ResolveCheck(ctx, req)
}
