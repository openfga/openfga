package graph

import (
	"context"
	"errors"

	"github.com/openfga/openfga/internal/dispatch"
	datastoremw "github.com/openfga/openfga/internal/middleware/datastore"
	dispatchpb "github.com/openfga/openfga/pkg/proto/dispatch/v1"
	"github.com/openfga/openfga/pkg/tuple"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("openfga/internal/graph/check")

// ConcurrentChecker exposes a method to perform Check requests. A ConcurrentChecker
// delegates subproblems to the provided dispatch.Check dispatcher.
type ConcurrentChecker struct {
	dispatcher dispatch.Check
}

// NewConcurrentChecker creates an instance of a ConcurrentChecker.
func NewConcurrentChecker(d dispatch.Dispatcher) *ConcurrentChecker {
	return &ConcurrentChecker{dispatcher: d}
}

// Check performs a check of the provided request and context. It is assumed that the request
// is validated beforehand and the provided rewrite is looked up beforehand by fetching the
// type information from the authorization model.
func (cc *ConcurrentChecker) Check(ctx context.Context, req *dispatchpb.DispatchCheckRequest, rewrite *openfgapb.Userset) (*dispatchpb.DispatchCheckResponse, error) {

	reducerFunc := cc.reduce(ctx, req, rewrite)

	resolved, err := any(ctx, []ReduceableCheckFunc{reducerFunc})
	resolved.Metadata = addCallToResponseMetadata(resolved.GetMetadata())

	return resolved, err
}

// dispatch dispatches the provided request to the internal dispatcher and yields a value on
// the result channel if the subproblem is successfully resolved or an error otherwise.
func (cc *ConcurrentChecker) dispatch(ctx context.Context, req *dispatchpb.DispatchCheckRequest) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error {
		result, err := cc.dispatcher.DispatchCheck(ctx, req)
		if err != nil {
			return err
		}
		resultChan <- result
		return nil
	}
}

// reduce reduces the check request by breaking it into an appropriate subproblem that
// is resolved by evaluating the underlying userset rewrite rule.
func (cc *ConcurrentChecker) reduce(
	ctx context.Context,
	req *dispatchpb.DispatchCheckRequest,
	rewrite *openfgapb.Userset,
) ReduceableCheckFunc {
	ctx, span := tracer.Start(ctx, "reduce")
	defer span.End()

	checkTracer := CheckResolutionTracerFromContext(ctx)

	switch rw := rewrite.Userset.(type) {
	case nil, *openfgapb.Userset_This:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("this")})
		return cc.checkDirect(ctx, req)
	case *openfgapb.Userset_Union:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("union")})
		return cc.checkSetOperation(
			WithCheckResolutionTraceContext(ctx, checkTracer.AppendUnion()),
			req,
			rw.Union.GetChild(),
			any,
		)
	case *openfgapb.Userset_Intersection:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("intersection")})
		return cc.checkSetOperation(
			WithCheckResolutionTraceContext(ctx, checkTracer.AppendIntersection(checkTracer.CreateIntersectionTracer())),
			req,
			rw.Intersection.GetChild(),
			all,
		)
	case *openfgapb.Userset_Difference:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("difference")})

		children := []*openfgapb.Userset{
			rw.Difference.GetBase(),
			rw.Difference.GetSubtract(),
		}

		return cc.checkSetOperation(
			ctx,
			req,
			children,
			difference,
		)
	case *openfgapb.Userset_ComputedUserset:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("computed")})
		return cc.checkComputedUserset(ctx, req, rw)
	case *openfgapb.Userset_TupleToUserset:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("tuple-to-userset")})
		return cc.checkTupleToUserset(ctx, req, rw)
	default:
		return func(ctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error {
			return serverErrors.UnsupportedUserSet
		}
	}
}

func (cc *ConcurrentChecker) checkDirect(ctx context.Context, req *dispatchpb.DispatchCheckRequest) ReduceableCheckFunc {

	return func(cctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error {
		checkTracer := CheckResolutionTracerFromContext(ctx)

		datastore := datastoremw.MustFromContext(ctx)

		storeID := req.GetWrappedRequest().GetStoreId()
		modelID := req.GetWrappedRequest().GetAuthorizationModelId()
		tupleKey := req.GetWrappedRequest().GetTupleKey()

		resolved, err := any(cctx, []ReduceableCheckFunc{
			func(cctx context.Context, r chan<- *dispatchpb.DispatchCheckResponse) error {
				tuple, err := datastore.ReadUserTuple(ctx, storeID, tupleKey)
				if err != nil {
					if errors.Is(err, storage.ErrNotFound) {
						r <- checkResponse(false, emptyMetadata)
						return nil
					}

					return err
				}

				_ = tuple

				responseMetadata := ensureMetadata(nil)
				responseMetadata.ResolutionPath = checkTracer.AppendDirect().GetResolution()

				r <- checkResponse(true, responseMetadata)
				return nil
			},
			func(cctx context.Context, r chan<- *dispatchpb.DispatchCheckResponse) error {
				iter, err := datastore.ReadUsersetTuples(ctx, storeID, tupleKey)
				if err != nil {
					return err
				}
				defer iter.Stop()

				var requestsToDispatch []ReduceableCheckFunc
				for {
					t, err := iter.Next()
					if err != nil {
						if err == storage.TupleIteratorDone {
							break
						}

						return err
					}
					defer iter.Stop()

					tk := t.GetKey() // use other variable than tupleKey to avoid overlap

					if tk.GetUser() == "*" {

						responseMetadata := ensureMetadata(nil)
						responseMetadata.ResolutionPath = checkTracer.AppendDirect().GetResolution()

						r <- checkResponse(true, responseMetadata)
						return nil
					}

					object, relation := tuple.SplitObjectRelation(tk.GetUser())

					metadata := decrementDepth(req.GetMetadata())
					metadata.ResolutionPath = checkTracer.AppendDirect().AppendString(tk.GetUser()).GetResolution()

					requestsToDispatch = append(
						requestsToDispatch,
						cc.dispatch(
							ctx,
							&dispatchpb.DispatchCheckRequest{
								Metadata: metadata,
								WrappedRequest: &openfgapb.CheckRequest{
									StoreId:              storeID,
									AuthorizationModelId: modelID,
									TupleKey: &openfgapb.TupleKey{
										Object:   object,
										Relation: relation,
										User:     tupleKey.GetUser(),
									},
									Trace: req.GetWrappedRequest().GetTrace(),
								},
							}),
					)
				}

				resolved, err := any(ctx, requestsToDispatch)
				if err != nil {
					return err
				}

				r <- resolved
				return nil
			},
		})
		if err != nil {
			return err
		}

		resultChan <- resolved
		return nil
	}
}

func (cc *ConcurrentChecker) checkComputedUserset(ctx context.Context, req *dispatchpb.DispatchCheckRequest, rw *openfgapb.Userset_ComputedUserset) ReduceableCheckFunc {

	checkTracer := CheckResolutionTracerFromContext(ctx)

	object := req.GetWrappedRequest().GetTupleKey().GetObject()
	relation := rw.ComputedUserset.GetRelation()

	checkTracer = checkTracer.AppendComputed().AppendString(tuple.ToObjectRelationString(object, relation))

	// have to reinject because we have a new reference
	ctx = WithCheckResolutionTraceContext(ctx, checkTracer)

	metadata := decrementDepth(req.GetMetadata())
	metadata.ResolutionPath = checkTracer.GetResolution()

	return cc.dispatch(
		ctx,
		&dispatchpb.DispatchCheckRequest{
			Metadata: metadata,
			WrappedRequest: &openfgapb.CheckRequest{
				StoreId:              req.GetWrappedRequest().GetStoreId(),
				AuthorizationModelId: req.GetWrappedRequest().GetAuthorizationModelId(),
				TupleKey: &openfgapb.TupleKey{
					Object:   object,
					Relation: relation,
					User:     req.GetWrappedRequest().GetTupleKey().GetUser(),
				},
				Trace: req.GetWrappedRequest().GetTrace(),
			},
		})
}

func (cc *ConcurrentChecker) checkTupleToUserset(
	ctx context.Context,
	req *dispatchpb.DispatchCheckRequest,
	rw *openfgapb.Userset_TupleToUserset,
) ReduceableCheckFunc {

	checkTracer := CheckResolutionTracerFromContext(ctx)
	_ = checkTracer // todo: use checkTracer

	return func(ctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error {
		datastore := datastoremw.MustFromContext(ctx)

		relation := rw.TupleToUserset.GetTupleset().GetRelation()
		if relation == "" {
			relation = req.GetWrappedRequest().GetTupleKey().GetRelation()
		}

		object := req.GetWrappedRequest().GetTupleKey().GetObject()

		checkTracer = checkTracer.AppendTupleToUserset().AppendString(tuple.ToObjectRelationString(object, relation))

		iter, err := datastore.Read(
			ctx,
			req.GetWrappedRequest().GetStoreId(),
			&openfgapb.TupleKey{
				Object:   object,
				Relation: relation,
			},
		)
		if err != nil {
			return err
		}
		defer iter.Stop()

		var requestsToDispatch []ReduceableCheckFunc
		for {
			t, err := iter.Next()
			if err != nil {
				if err == storage.TupleIteratorDone {
					break
				}

				return err
			}

			tupleKey := t.GetKey()

			object, relation := tuple.SplitObjectRelation(tupleKey.GetUser())
			if relation == "" {
				relation = rw.TupleToUserset.GetComputedUserset().GetRelation()
			}
			if relation != rw.TupleToUserset.GetComputedUserset().GetRelation() {
				continue
			}

			checkTracer = checkTracer.AppendString(tuple.ToObjectRelationString(object, relation))

			// have to reinject because we have a new reference
			ctx = WithCheckResolutionTraceContext(ctx, checkTracer)

			metadata := decrementDepth(req.GetMetadata())
			metadata.ResolutionPath = checkTracer.GetResolution()

			requestsToDispatch = append(
				requestsToDispatch,
				cc.dispatch(
					ctx,
					&dispatchpb.DispatchCheckRequest{
						Metadata: metadata,
						WrappedRequest: &openfgapb.CheckRequest{
							StoreId:              req.GetWrappedRequest().GetStoreId(),
							AuthorizationModelId: req.GetWrappedRequest().GetAuthorizationModelId(),
							TupleKey: &openfgapb.TupleKey{
								Object:   object,
								Relation: relation,
								User:     req.GetWrappedRequest().GetTupleKey().GetUser(),
							},
							Trace: req.GetWrappedRequest().GetTrace(),
						},
					}),
			)
		}

		resolved, err := any(ctx, requestsToDispatch)
		if err != nil {
			return err
		}

		resultChan <- resolved
		return nil
	}
}

func (cc *ConcurrentChecker) checkSetOperation(
	ctx context.Context,
	req *dispatchpb.DispatchCheckRequest,
	children []*openfgapb.Userset,
	reducer CheckReducer,
) ReduceableCheckFunc {

	resolutionTracer := CheckResolutionTracerFromContext(ctx)

	return func(cctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error {

		var requestsToDispatch []ReduceableCheckFunc
		for i, child := range children {
			resolutionCtx := WithCheckResolutionTraceContext(cctx, resolutionTracer.AppendIndex(i))

			requestsToDispatch = append(requestsToDispatch, cc.reduce(resolutionCtx, req, child))
		}

		resolved, err := reducer(ctx, requestsToDispatch)
		if err != nil {
			return err
		}

		resultChan <- resolved
		return nil
	}
}

// all defines a CheckReducer that returns a check result indicating if all of the checks passed, and
// it is used to resolve intersection.
func all(ctx context.Context, requests []ReduceableCheckFunc) (*dispatchpb.DispatchCheckResponse, error) {

	if len(requests) == 0 {
		return checkResponse(false, emptyMetadata), nil
	}

	responseMetadata := emptyMetadata

	resultChan := make(chan *dispatchpb.DispatchCheckResponse, len(requests))
	errChan := make(chan error)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, r := range requests {
		go func(req ReduceableCheckFunc) {
			err := req(cctx, resultChan)
			if err != nil {
				errChan <- err
			}
		}(r)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Metadata)

			if !result.WrappedResponse.Allowed {
				return checkResponse(false, responseMetadata), nil
			}
		case err := <-errChan:
			return checkResponse(false, responseMetadata), err
		case <-ctx.Done():
			return checkResponse(false, responseMetadata), ctx.Err()
		}
	}

	return checkResponse(true, responseMetadata), nil
}

// any defines a CheckReducer that returns a check result indicating if any of the checks passed, and
// it is used to resolve union.
func any(ctx context.Context, requests []ReduceableCheckFunc) (*dispatchpb.DispatchCheckResponse, error) {

	if len(requests) == 0 {
		return checkResponse(false, emptyMetadata), nil
	}

	responseMetadata := emptyMetadata
	resultChan := make(chan *dispatchpb.DispatchCheckResponse, len(requests))
	errChan := make(chan error)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, r := range requests {

		go func(req ReduceableCheckFunc) {
			err := req(cctx, resultChan)
			if err != nil {
				errChan <- err
			}
		}(r)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.GetMetadata())

			if result.GetWrappedResponse().GetAllowed() {
				return checkResponse(true, responseMetadata), nil
			}
		case err := <-errChan:
			return checkResponse(false, responseMetadata), err
		case <-ctx.Done():
			return checkResponse(false, responseMetadata), ctx.Err()
		}
	}

	return checkResponse(false, responseMetadata), nil
}

// difference defines a CheckReducer that returns a check result indicating if the base check passes and none of
// the subsequent checks pass (the subtracted set).
func difference(ctx context.Context, requests []ReduceableCheckFunc) (*dispatchpb.DispatchCheckResponse, error) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if len(requests) < 2 {
		// todo: return a non-nil error, because you need a base and subtract reducer
	}

	responseMetadata := emptyMetadata
	baseChan := make(chan *dispatchpb.DispatchCheckResponse, 1)
	subtractChan := make(chan *dispatchpb.DispatchCheckResponse, len(requests)-1)
	errChan := make(chan error, 1)

	go func() {
		err := requests[0](cctx, baseChan)
		if err != nil {
			errChan <- err
		}
	}()

	for _, r := range requests[1:] {
		go func(req ReduceableCheckFunc) {
			err := req(cctx, subtractChan)
			if err != nil {
				errChan <- err
			}
		}(r)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case base := <-baseChan:
			responseMetadata = combineResponseMetadata(responseMetadata, base.Metadata)

			if !base.WrappedResponse.Allowed {
				return checkResponse(false, responseMetadata), nil
			}
		case sub := <-subtractChan:
			responseMetadata = combineResponseMetadata(responseMetadata, sub.Metadata)

			if sub.WrappedResponse.Allowed {
				return checkResponse(false, responseMetadata), nil
			}

		case err := <-errChan:
			return checkResponse(false, responseMetadata), err
		case <-ctx.Done():
			return checkResponse(false, responseMetadata), ctx.Err()
		}
	}

	return checkResponse(true, responseMetadata), nil
}

func checkResponse(allowed bool, responseMetadata *dispatchpb.ResponseMeta) *dispatchpb.DispatchCheckResponse {

	return &dispatchpb.DispatchCheckResponse{
		Metadata: ensureMetadata(responseMetadata),
		WrappedResponse: &openfgapb.CheckResponse{
			Allowed:    allowed,
			Resolution: responseMetadata.GetResolutionPath(),
		},
	}
}
