package graph

import (
	"context"
	"github.com/openfga/openfga/internal/throttler"
)

type DispatchThrottlingCheckResolver struct {
	delegate  CheckResolver
	throttler *throttler.DispatchThrottler
}

var _ CheckResolver = (*DispatchThrottlingCheckResolver)(nil)

func NewDispatchThrottlingCheckResolver(
	throttler *throttler.DispatchThrottler) *DispatchThrottlingCheckResolver {
	dispatchThrottlingCheckResolver := &DispatchThrottlingCheckResolver{
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
	r.throttler.Throttle(ctx, currentNumDispatch)
	return r.delegate.ResolveCheck(ctx, req)
}
