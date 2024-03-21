package graph

import (
	"context"
	"github.com/openfga/openfga/internal/dispatcher"

	"go.opentelemetry.io/otel/attribute"

	"github.com/openfga/openfga/pkg/tuple"
)

type CycleDetectionCheckResolver struct {
	delegate dispatcher.Dispatcher
}

var _ dispatcher.Dispatcher = (*CycleDetectionCheckResolver)(nil)

// Close implements CheckResolver.
func (*CycleDetectionCheckResolver) Close() {}

func NewCycleDetectionCheckResolver() *CycleDetectionCheckResolver {
	c := &CycleDetectionCheckResolver{}
	c.delegate = c

	return c
}

// ResolveCheck implements CheckResolver.
func (c CycleDetectionCheckResolver) Dispatch(ctx context.Context, request dispatcher.DispatchRequest) (dispatcher.DispatchResponse, error) {
	req := request.(*ResolveCheckRequest)
	ctx, span := tracer.Start(ctx, "ResolveCheck")
	defer span.End()
	span.SetAttributes(attribute.String("resolver_type", "CycleDetectionCheckResolver"))
	span.SetAttributes(attribute.String("tuple_key", req.GetTupleKey().String()))

	key := tuple.TupleKeyToString(req.GetTupleKey())

	if req.VisitedPaths == nil {
		req.VisitedPaths = map[string]struct{}{}
	}

	_, cycleDetected := req.VisitedPaths[key]
	span.SetAttributes(attribute.Bool("cycle_detected", cycleDetected))
	if cycleDetected {
		return nil, ErrCycleDetected
	}

	req.VisitedPaths[key] = struct{}{}

	return c.delegate.Dispatch(ctx, &ResolveCheckRequest{
		StoreID:              req.GetStoreID(),
		AuthorizationModelID: req.GetAuthorizationModelID(),
		TupleKey:             req.GetTupleKey(),
		ContextualTuples:     req.GetContextualTuples(),
		RequestMetadata:      req.GetRequestMetadata(),
		VisitedPaths:         req.VisitedPaths,
		Context:              req.GetContext(),
	})
}

func (c *CycleDetectionCheckResolver) SetDelegate(delegate dispatcher.Dispatcher) {
	c.delegate = delegate
}

func (c *CycleDetectionCheckResolver) GetDelegate() dispatcher.Dispatcher {
	return c.delegate
}
