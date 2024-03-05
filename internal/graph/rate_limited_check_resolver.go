package graph

import (
	"context"
	"time"
)

type RateLimitedCheckResolverConfig struct {
	NonImpedingDispatchNum uint32
	LowPriorityLevel       uint32
	LowPriorityWait        uint32
}

// RateLimitedCheckResolver will prioritize
type RateLimitedCheckResolver struct {
	delegate         CheckResolver
	config           RateLimitedCheckResolverConfig
	ticker           *time.Ticker
	lowPriorityQueue chan bool
	medPriorityQueue chan bool
	done             chan bool
}

var _ CheckResolver = (*RateLimitedCheckResolver)(nil)

func NewRateLimitedCheckResolver(
	config RateLimitedCheckResolverConfig) *RateLimitedCheckResolver {
	ticker := time.NewTicker(10 * time.Microsecond)
	rateLimitedCheckResolver := &RateLimitedCheckResolver{
		config:           config,
		ticker:           ticker,
		lowPriorityQueue: make(chan bool, 1),
		medPriorityQueue: make(chan bool, 1),
	}
	rateLimitedCheckResolver.delegate = rateLimitedCheckResolver
	go rateLimitedCheckResolver.Ticker()
	return rateLimitedCheckResolver
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

func (r *RateLimitedCheckResolver) Ticker() {
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
			if count >= r.config.LowPriorityWait {
				count = 0
				r.nonBlockingSend(r.lowPriorityQueue)
			}
		}
	}
}

func (r *RateLimitedCheckResolver) ResolveCheck(ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	currentNumDispatch := req.DispatchCounter.Add(1)
	if currentNumDispatch > r.config.NonImpedingDispatchNum {
		if currentNumDispatch >= r.config.LowPriorityLevel {
			<-r.lowPriorityQueue
		} else {
			<-r.medPriorityQueue
		}
	}
	return r.delegate.ResolveCheck(ctx, req)
}
