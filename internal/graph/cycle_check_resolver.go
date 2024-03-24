package graph

import (
	"context"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/dispatcher"
	"google.golang.org/protobuf/types/known/anypb"
	"log"

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
func (c CycleDetectionCheckResolver) Dispatch(ctx context.Context, request *openfgav1.BaseRequest, metadata *openfgav1.DispatchMetadata) (*openfgav1.BaseResponse, *openfgav1.DispatchMetadata, error) {
	log.Printf("Cycle Check Dispatcher - %p", request.GetDispatchedCheckRequest())
	req := request.GetDispatchedCheckRequest()
	ctx, span := tracer.Start(ctx, "ResolveCheck")
	defer span.End()
	span.SetAttributes(attribute.String("resolver_type", "CycleDetectionCheckResolver"))
	span.SetAttributes(attribute.String("tuple_key", req.GetTupleKey().String()))

	key := tuple.TupleKeyToString(req.GetTupleKey())

	if req.VisitedPaths == nil {
		req.VisitedPaths = map[string]*anypb.Any{}
	}

	_, cycleDetected := req.VisitedPaths[key]
	span.SetAttributes(attribute.Bool("cycle_detected", cycleDetected))
	if cycleDetected {
		return nil, nil, ErrCycleDetected
	}

	req.VisitedPaths = map[string]*anypb.Any{}

	childRequest := &openfgav1.DispatchedCheckRequest{
		StoreId:              req.StoreId,
		AuthorizationModelId: req.AuthorizationModelId,
		TupleKey:             req.GetTupleKey(),
		ContextualTuples:     req.GetContextualTuples(),
		VisitedPaths:         req.VisitedPaths,
	}
	test := openfgav1.BaseRequest_DispatchedCheckRequest{DispatchedCheckRequest: childRequest}
	return c.delegate.Dispatch(ctx, &openfgav1.BaseRequest{BaseRequest: &test}, metadata)
}

func (c *CycleDetectionCheckResolver) SetDelegate(delegate dispatcher.Dispatcher) {
	c.delegate = delegate
}

func (c *CycleDetectionCheckResolver) GetDelegate() dispatcher.Dispatcher {
	return c.delegate
}
