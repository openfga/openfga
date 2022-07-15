package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/openfga/openfga/internal/dispatch"
	"github.com/openfga/openfga/internal/graph"
	datastoremw "github.com/openfga/openfga/internal/middleware/datastore"
	dispatchpb "github.com/openfga/openfga/pkg/proto/dispatch/v1"
	"github.com/openfga/openfga/pkg/tuple"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
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
		return &dispatchpb.DispatchCheckResponse{Metadata: emptyMetadata}, serverErrors.AuthorizationModelResolutionTooComplex
	}

	t := graph.NewNoopCheckResolutionTracer()
	if req.GetWrappedRequest().GetTrace() {
		// todo: initialize the resolution tracer with a concrete value
		// init with the value from the request ResolverMetadata (if present)
		t = graph.NewStringCheckResolutionTracer(req.GetMetadata().GetResolutionPath())
	}
	ctx = graph.WithCheckResolutionTraceContext(ctx, t)

	// todo: validate the request inputs
	tk := req.GetWrappedRequest().GetTupleKey()
	err = validateAndPreprocessTuples(tk, req.GetWrappedRequest().GetContextualTuples().GetTupleKeys())
	if err != nil {
		return nil, err
	}

	rewrite, err := ld.lookupRelation(ctx, req)
	if err != nil {
		return nil, err
	}

	return ld.checker.Check(ctx, req, rewrite)
}

func validateAndPreprocessTuples(keyToCheck *openfgapb.TupleKey, tupleKeys []*openfgapb.TupleKey) error {
	if keyToCheck.GetUser() == "" || keyToCheck.GetRelation() == "" || keyToCheck.GetObject() == "" {
		return serverErrors.InvalidCheckInput
	}
	if !tuple.IsValidUser(keyToCheck.GetUser()) {
		return serverErrors.InvalidUser(keyToCheck.GetUser())
	}

	tupleMap := map[string]struct{}{}
	usersets := map[string][]*openfgapb.TupleKey{}
	for _, tk := range tupleKeys {
		if _, ok := tupleMap[tk.String()]; ok {
			return serverErrors.DuplicateContextualTuple(tk)
		}
		tupleMap[tk.String()] = struct{}{}

		if tk.GetUser() == "" || tk.GetRelation() == "" || tk.GetObject() == "" {
			return serverErrors.InvalidContextualTuple(tk)
		}
		if !tuple.IsValidUser(tk.GetUser()) {
			return serverErrors.InvalidUser(tk.GetUser())
		}

		key := tuple.ToObjectRelationString(tk.GetObject(), tk.GetRelation())
		usersets[key] = append(usersets[key], tk)
	}

	return nil
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

// lookupRelation looks up the userset rewrite rule associated with the object and relation provided
// in the request.
func (ld *localDispatcher) lookupRelation(ctx context.Context, req *dispatchpb.DispatchCheckRequest) (*openfgapb.Userset, error) {
	datastore := datastoremw.MustFromContext(ctx)

	storeID := req.GetWrappedRequest().GetStoreId()
	modelID := req.GetWrappedRequest().GetAuthorizationModelId()
	tupleKey := req.GetWrappedRequest().GetTupleKey()
	objectType, _ := tuple.SplitObject(tupleKey.GetObject())
	relation := tupleKey.GetRelation()

	typeDef, err := datastore.ReadTypeDefinition(
		ctx,
		storeID,
		modelID,
		objectType,
	)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, NewTypeNotFoundErr(objectType)
		}
		return nil, err
	}

	rewrite, ok := typeDef.Relations[relation]
	if !ok {
		return nil, serverErrors.RelationNotFound(relation, objectType, tupleKey)
	}

	return rewrite, nil
}
