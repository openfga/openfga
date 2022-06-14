package graph

import (
	"context"
	"errors"

	"github.com/openfga/openfga/internal/dispatch"
	dispatchpb "github.com/openfga/openfga/pkg/proto/dispatch/v1"
	"github.com/openfga/openfga/pkg/tuple"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

var tracer = otel.Tracer("openfga/internal/graph/check")

// ConcurrentChecker exposes a method to perform Check requests. A ConcurrentChecker
// delegates subproblems to the provided dispatch.Check dispatcher.
type ConcurrentChecker struct {
	dispatcher dispatch.Check
	datastore  storage.OpenFGADatastore
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
	resolved.Metadata = addCallToResponseMetadata(resolved.Metadata)

	return resolved, err
}

// dispatch dispatches the provided request to the internal dispatcher and yields a value on
// the result channel if the subproblem is successfully resolved or an error otherwise.
func (cc *ConcurrentChecker) dispatch(req *dispatchpb.DispatchCheckRequest) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error {
		result, err := cc.dispatcher.DispatchCheck(ctx, req)
		resultChan <- result
		return err
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

	switch rw := rewrite.Userset.(type) {
	case nil, *openfgapb.Userset_This:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("this")})
		return cc.checkDirect(ctx, req)
	case *openfgapb.Userset_Union:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("union")})
		return cc.checkSetOperation(ctx, req, rw.Union.GetChild(), any)
	case *openfgapb.Userset_Intersection:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("intersection")})
		return cc.checkSetOperation(ctx, req, rw.Intersection.GetChild(), all)
	case *openfgapb.Userset_Difference:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("difference")})
		children := []*openfgapb.Userset{
			rw.Difference.GetBase(),
			rw.Difference.GetSubtract(),
		}
		return cc.checkSetOperation(ctx, req, children, difference)
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

	return func(ctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error {
		g := new(errgroup.Group)

		g.Go(func() error {

			tuple, err := cc.datastore.ReadUserTuple(ctx, req.WrappedRequest.StoreId, req.WrappedRequest.TupleKey)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					resultChan <- &dispatchpb.DispatchCheckResponse{
						Metadata: emptyMetadata,
						WrappedResponse: &openfgapb.CheckResponse{
							Allowed: false,
						},
					}

					return nil
				}

				return err
			}

			_ = tuple // todo: append the resolution path to the tracer

			resultChan <- &dispatchpb.DispatchCheckResponse{
				WrappedResponse: &openfgapb.CheckResponse{
					Allowed: true,
				},
			}

			return nil
		})

		g.Go(func() error {
			iter, err := cc.datastore.ReadUsersetTuples(ctx, req.WrappedRequest.StoreId, req.WrappedRequest.TupleKey)
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

				requestsToDispatch = append(
					requestsToDispatch,
					cc.dispatch(&dispatchpb.DispatchCheckRequest{
						Metadata: decrementDepth(req.GetMetadata()),
						WrappedRequest: &openfgapb.CheckRequest{
							StoreId: req.GetWrappedRequest().GetStoreId(),
							TupleKey: &openfgapb.TupleKey{
								Object:   object,
								Relation: relation,
								User:     req.GetWrappedRequest().GetTupleKey().GetUser(),
							},
						},
					}),
				)
			}
			iter.Stop()

			resolved, err := any(ctx, requestsToDispatch)
			if err != nil {
				return err
			}

			resultChan <- resolved
			return nil
		})

		return g.Wait()

	}
}

func (cc *ConcurrentChecker) checkComputedUserset(ctx context.Context, req *dispatchpb.DispatchCheckRequest, rw *openfgapb.Userset_ComputedUserset) ReduceableCheckFunc {
	return cc.dispatch(&dispatchpb.DispatchCheckRequest{
		Metadata: decrementDepth(req.GetMetadata()),
		WrappedRequest: &openfgapb.CheckRequest{
			StoreId: req.GetWrappedRequest().GetStoreId(),
			TupleKey: &openfgapb.TupleKey{
				Object:   req.GetWrappedRequest().GetTupleKey().GetObject(),
				Relation: rw.ComputedUserset.GetObject(),
				User:     req.GetWrappedRequest().GetTupleKey().GetUser(),
			},
		},
	})
}

func (cc *ConcurrentChecker) checkTupleToUserset(
	ctx context.Context,
	req *dispatchpb.DispatchCheckRequest,
	rw *openfgapb.Userset_TupleToUserset,
) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error {

		relation := rw.TupleToUserset.GetTupleset().GetRelation()
		if relation == "" {
			relation = req.GetWrappedRequest().GetTupleKey().GetRelation()
		}

		var requestsToDispatch []ReduceableCheckFunc
		for {
			iter, err := cc.datastore.Read(
				ctx,
				req.GetWrappedRequest().GetStoreId(),
				&openfgapb.TupleKey{
					Object:   req.GetWrappedRequest().GetTupleKey().GetObject(),
					Relation: relation,
				},
			)
			if err != nil {
				return err
			}
			defer iter.Stop()

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

			requestsToDispatch = append(
				requestsToDispatch,
				cc.dispatch(&dispatchpb.DispatchCheckRequest{
					Metadata: decrementDepth(req.GetMetadata()),
					WrappedRequest: &openfgapb.CheckRequest{
						StoreId: req.GetWrappedRequest().GetStoreId(),
						TupleKey: &openfgapb.TupleKey{
							Object:   object,
							Relation: relation,
							User:     req.GetWrappedRequest().GetTupleKey().GetUser(),
						},
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
	return func(ctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error {

		var requestsToDispatch []ReduceableCheckFunc
		for _, child := range children {
			requestsToDispatch = append(requestsToDispatch, cc.reduce(ctx, req, child))
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
		// return empty CheckResult
	}

	responseMetadata := emptyMetadata

	resultChan := make(chan *dispatchpb.DispatchCheckResponse, len(requests))
	errChan := make(chan error)
	defer close(resultChan)
	defer close(errChan)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g := new(errgroup.Group)

	for _, req := range requests {
		fn := func() error {
			err := req(cctx, resultChan)
			if err != nil {
				errChan <- err
			}

			return err
		}

		g.Go(fn)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Metadata)

			if !result.WrappedResponse.Allowed {
				return &dispatchpb.DispatchCheckResponse{
					Metadata: responseMetadata,
					WrappedResponse: &openfgapb.CheckResponse{
						Allowed: false,
					},
				}, nil
			}
		case err := <-errChan:
			return &dispatchpb.DispatchCheckResponse{}, err
		case <-ctx.Done():
			// return request cancelled error
		}
	}

	return &dispatchpb.DispatchCheckResponse{
		Metadata: responseMetadata,
		WrappedResponse: &openfgapb.CheckResponse{
			Allowed: true,
		},
	}, nil
}

// any defines a CheckReducer that returns a check result indicating if any of the checks passed, and
// it is used to resolve union.
func any(ctx context.Context, requests []ReduceableCheckFunc) (*dispatchpb.DispatchCheckResponse, error) {

	if len(requests) == 0 {
		// return empty CheckResult
	}

	responseMetadata := emptyMetadata
	resultChan := make(chan *dispatchpb.DispatchCheckResponse, len(requests))
	errChan := make(chan error)
	defer close(resultChan)
	defer close(errChan)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g := new(errgroup.Group)

	for _, req := range requests {
		fn := func() error {
			err := req(cctx, resultChan)
			if err != nil {
				errChan <- err
			}

			return err
		}

		g.Go(fn)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Metadata)

			if result.WrappedResponse.Allowed {
				return &dispatchpb.DispatchCheckResponse{
					Metadata: responseMetadata,
					WrappedResponse: &openfgapb.CheckResponse{
						Allowed: true,
					},
				}, nil
			}
		case err := <-errChan:
			return &dispatchpb.DispatchCheckResponse{}, err
		case <-ctx.Done():
			// return err
		}
	}

	return &dispatchpb.DispatchCheckResponse{
		Metadata: responseMetadata,
		WrappedResponse: &openfgapb.CheckResponse{
			Allowed: false,
		},
	}, nil
}

// difference defines a CheckReducer that returns a check result indicating if the base check passes and none of
// the subsequent checks pass (the subtracted set).
func difference(ctx context.Context, requests []ReduceableCheckFunc) (*dispatchpb.DispatchCheckResponse, error) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if len(requests) < 2 {
		// return a non-nil error, because you need a base and subtract reducer
	}

	responseMetadata := emptyMetadata
	baseChan := make(chan *dispatchpb.DispatchCheckResponse, 1)
	subtractChan := make(chan *dispatchpb.DispatchCheckResponse, len(requests)-1)
	errChan := make(chan error, 1)
	defer close(baseChan)
	defer close(subtractChan)
	defer close(errChan)

	g := new(errgroup.Group)

	g.Go(func() error {
		err := requests[0](cctx, baseChan)
		if err != nil {
			errChan <- err
		}

		return err
	})

	for _, req := range requests[1:] {
		fn := func() error {
			err := req(cctx, subtractChan)
			if err != nil {
				errChan <- err
			}

			return err
		}

		g.Go(fn)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case base := <-baseChan:
			responseMetadata = combineResponseMetadata(responseMetadata, base.Metadata)

			if !base.WrappedResponse.Allowed {
				return &dispatchpb.DispatchCheckResponse{
					Metadata: responseMetadata,
					WrappedResponse: &openfgapb.CheckResponse{
						Allowed: false,
					},
				}, nil
			}
		case sub := <-subtractChan:
			responseMetadata = combineResponseMetadata(responseMetadata, sub.Metadata)

			if sub.WrappedResponse.Allowed {
				return &dispatchpb.DispatchCheckResponse{
					Metadata: responseMetadata,
					WrappedResponse: &openfgapb.CheckResponse{
						Allowed: false,
					},
				}, nil
			}

		case err := <-errChan:
			return &dispatchpb.DispatchCheckResponse{
				Metadata: responseMetadata,
				WrappedResponse: &openfgapb.CheckResponse{
					Allowed: false,
				},
			}, err
		case <-ctx.Done():
			return &dispatchpb.DispatchCheckResponse{
				Metadata: responseMetadata,
				WrappedResponse: &openfgapb.CheckResponse{
					Allowed: false,
				},
			}, ctx.Err()
		}
	}

	return &dispatchpb.DispatchCheckResponse{
		Metadata: responseMetadata,
		WrappedResponse: &openfgapb.CheckResponse{
			Allowed: true,
		},
	}, nil
}
