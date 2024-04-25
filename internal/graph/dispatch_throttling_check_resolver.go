package graph

import (
	"context"
	"github.com/openfga/openfga/internal/throttler"
)

type DispatchThrottlingCheckResolver struct {
	delegate  CheckResolver
	threshold uint32
	throttler throttler.Throttler
}

var _ CheckResolver = (*DispatchThrottlingCheckResolver)(nil)

func NewDispatchThrottlingCheckResolver(
	threshold uint32, throttler throttler.Throttler) *DispatchThrottlingCheckResolver {
	dispatchThrottlingCheckResolver := &DispatchThrottlingCheckResolver{
		threshold: threshold,
		throttler: throttler,
	}
	dispatchThrottlingCheckResolver.delegate = dispatchThrottlingCheckResolver
	return dispatchThrottlingCheckResolver
}

func (r *DispatchThrottlingCheckResolver) SetDelegate(delegate CheckResolver) {
	r.delegate = delegate
}

func (r *DispatchThrottlingCheckResolver) GetDelegate() CheckResolver {
	return r.delegate
}

func (r *DispatchThrottlingCheckResolver) Close() {
	r.throttler.Close()
}

func (r *DispatchThrottlingCheckResolver) ResolveCheck(ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	currentNumDispatch := req.GetRequestMetadata().DispatchCounter.Load()
	if currentNumDispatch > r.threshold {
		r.throttler.Throttle(ctx)
	}
	return r.delegate.ResolveCheck(ctx, req)
}
