package graph

import (
	"context"
	"fmt"

	"github.com/openfga/openfga/internal/dispatch"
	"github.com/openfga/openfga/internal/graph"
	dispatchpb "github.com/openfga/openfga/pkg/proto/dispatch/v1"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("openfga/internal/dispatch/local")

var emptyMetadata *dispatchpb.ResponseMeta = &dispatchpb.ResponseMeta{
	DispatchCount: 0,
}

var _ dispatch.Dispatcher = &localDispatcher{}

// localDispatcher provides an implementation of the dispatch.Dispatcher interface
// that delegates graph subproblems to local goroutines that are concurrently evaluated.
type localDispatcher struct {
	checker *graph.ConcurrentChecker
}

// NewLocalDispatcher creates a dispatcher that evaluates the relationship the graph by
// dispatching graph subproblems to local goroutines that are concurrently evaluated.
func NewLocalDispatcher() dispatch.Dispatcher {
	d := &localDispatcher{}

	d.checker = graph.NewConcurrentChecker(d)

	return d
}

// NewDispatcher creates a dispatcher that evalutes the relationship graph and redispatches
// subproblems to the provided redispatcher.
func NewDispatcher(redispatcher dispatch.Dispatcher) dispatch.Dispatcher {
	checker := graph.NewConcurrentChecker(redispatcher)

	return &localDispatcher{
		checker: checker,
	}
}

// DispatchCheck provides an implementation of the dispatch.Check interface and implements
// the Check API.
func (ld *localDispatcher) DispatchCheck(
	ctx context.Context,
	req *dispatchpb.DispatchCheckRequest,
) (*dispatchpb.DispatchCheckResponse, error) {

	ctx, span := tracer.Start(ctx, "DispatchCheck")
	defer span.End()

	err := dispatch.CheckDepth(ctx, req)
	if err != nil {
		//return serverErrors.AuthorizationModelResolutionTooComplex
		return &dispatchpb.DispatchCheckResponse{Metadata: emptyMetadata}, nil
	}

	// validate the request inputs

	// get type information for the relation
	var rewrite *openfgapb.Userset

	return ld.checker.Check(ctx, req, rewrite)
}

// DispatchExpand provides an implementation of the dispatch.Expand interface and implements
// the Expand API.
func (ld *localDispatcher) DispatchExpand(
	ctx context.Context,
	req *dispatchpb.DispatchExpandRequest,
) (*dispatchpb.DispatchExpandResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// IsReady reports whether this local dispatcher is ready to serve
// requests.
func (ld *localDispatcher) IsReady() bool {
	return true
}

// Close closes the local dispatcher by closing and cleaning up any residual resources.
func (ld *localDispatcher) Close(ctx context.Context) error {
	return nil
}
