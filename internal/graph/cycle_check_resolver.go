package graph

import (
	"context"

	"github.com/openfga/openfga/pkg/tuple"
)

type CycleDetectionCheckResolver struct {
	delegate CheckResolver
}

var _ CheckResolver = (*CycleDetectionCheckResolver)(nil)

// Close implements CheckResolver.
func (*CycleDetectionCheckResolver) Close() {}

func NewCycleDetectionCheckResolver() *CycleDetectionCheckResolver {
	c := &CycleDetectionCheckResolver{}
	c.delegate = c

	return c
}

// ResolveCheck implements CheckResolver.
func (c *CycleDetectionCheckResolver) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {

	key := tuple.TupleKeyToString(req.GetTupleKey())

	if req.VisitedPaths == nil {
		req.VisitedPaths = map[string]struct{}{}
	}

	if _, ok := req.VisitedPaths[key]; ok {
		return nil, ErrCycleDetected
	}

	req.VisitedPaths[key] = struct{}{}

	return c.delegate.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              req.GetStoreID(),
		AuthorizationModelID: req.GetAuthorizationModelID(),
		TupleKey:             req.GetTupleKey(),
		ContextualTuples:     req.GetContextualTuples(),
		ResolutionMetadata:   req.GetResolutionMetadata(),
		VisitedPaths:         req.VisitedPaths,
		Context:              req.GetContext(),
	})
}

func (c *CycleDetectionCheckResolver) SetDelegate(delegate CheckResolver) {
	c.delegate = delegate
}
