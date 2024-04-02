// Package reverseexpand contains the code that handles the ReverseExpand API
package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"github.com/openfga/openfga/internal/dispatcher"
	"log"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/sourcegraph/conc/pool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/pkg/logger"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/graph"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var tracer = otel.Tracer("openfga/pkg/server/commands/reverse_expand")

type ReverseExpandAdditionalParameters struct {
	inter      bool
	User       IsUserRef
	resultChan chan<- *ReverseExpandResult
}

type IsUserRef interface {
	isUserRef()
	GetObjectType() string
	String() string
}

type UserRefObject struct {
	Object *openfgav1.Object
}

var _ IsUserRef = (*UserRefObject)(nil)

func (u *UserRefObject) isUserRef() {}

func (u *UserRefObject) GetObjectType() string {
	return u.Object.GetType()
}

func (u *UserRefObject) String() string {
	return tuple.BuildObject(u.Object.GetType(), u.Object.GetId())
}

type UserRefTypedWildcard struct {
	Type string
}

var _ IsUserRef = (*UserRefTypedWildcard)(nil)

func (*UserRefTypedWildcard) isUserRef() {}

func (u *UserRefTypedWildcard) GetObjectType() string {
	return u.Type
}

func (u *UserRefTypedWildcard) String() string {
	return fmt.Sprintf("%s:*", u.Type)
}

type UserRefObjectRelation struct {
	ObjectRelation *openfgav1.ObjectRelation
	Condition      *openfgav1.RelationshipCondition
}

func (*UserRefObjectRelation) isUserRef() {}

func (u *UserRefObjectRelation) GetObjectType() string {
	return tuple.GetType(u.ObjectRelation.GetObject())
}

func (u *UserRefObjectRelation) String() string {
	return tuple.ToObjectRelationString(
		u.ObjectRelation.GetObject(),
		u.ObjectRelation.GetRelation(),
	)
}

type UserRef struct {

	// Types that are assignable to Ref
	//  *UserRef_Object
	//  *UserRef_TypedWildcard
	//  *UserRef_ObjectRelation
	Ref IsUserRef
}

var _ dispatcher.Dispatcher = (*ReverseExpandQuery)(nil)

func (r *ReverseExpandQuery) SetDelegate(delegate dispatcher.Dispatcher) {
	r.delegate = delegate
}

type ReverseExpandQuery struct {
	delegate                dispatcher.Dispatcher
	logger                  logger.Logger
	datastore               storage.RelationshipTupleReader
	typesystem              *typesystem.TypeSystem
	resolveNodeLimit        uint32
	resolveNodeBreadthLimit uint32

	// visitedUsersetsMap map prevents visiting the same userset through the same edge twice
	visitedUsersetsMap *sync.Map
	// candidateObjectsMap map prevents returning the same object twice
	candidateObjectsMap *sync.Map
}

func (r *ReverseExpandQuery) Close() {
	//TODO implement me
	panic("implement me")
}

type ReverseExpandQueryOption func(d *ReverseExpandQuery)

func WithResolveNodeLimit(limit uint32) ReverseExpandQueryOption {
	return func(d *ReverseExpandQuery) {
		d.resolveNodeLimit = limit
	}
}

func WithResolveNodeBreadthLimit(limit uint32) ReverseExpandQueryOption {
	return func(d *ReverseExpandQuery) {
		d.resolveNodeBreadthLimit = limit
	}
}

func NewReverseExpandQuery(ds storage.RelationshipTupleReader, ts *typesystem.TypeSystem, opts ...ReverseExpandQueryOption) *ReverseExpandQuery {
	query := &ReverseExpandQuery{
		logger:                  logger.NewNoopLogger(),
		datastore:               ds,
		typesystem:              ts,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		candidateObjectsMap:     new(sync.Map),
		visitedUsersetsMap:      new(sync.Map),
	}

	for _, opt := range opts {
		opt(query)
	}

	return query
}

type ConditionalResultStatus int

const (
	RequiresFurtherEvalStatus ConditionalResultStatus = iota
	NoFurtherEvalStatus
)

type ReverseExpandResult struct {
	Object       string
	ResultStatus ConditionalResultStatus
}

type ResolutionMetadata struct {
	DatastoreQueryCount *uint32

	// The number of times we are expanding from each node to find set of objects
	DispatchCount *uint32
}

func NewResolutionMetadata() *ResolutionMetadata {
	return &ResolutionMetadata{
		DatastoreQueryCount: new(uint32),
		DispatchCount:       new(uint32),
	}
}

func WithLogger(logger logger.Logger) ReverseExpandQueryOption {
	return func(d *ReverseExpandQuery) {
		d.logger = logger
	}
}

// Execute yields all the objects of the provided objectType that the
// given user possibly has, a specific relation with and sends those
// objects to resultChan. It MUST guarantee no duplicate objects sent.
//
// This function respects context timeouts and cancellations. If an
// error is encountered (e.g. context timeout) before resolving all
// objects, then the provided channel will NOT be closed, and it will
// send the error through the channel.
//
// If no errors occur, then Execute will yield all of the objects on
// the provided channel and then close the channel to signal that it
// is done.
func (c *ReverseExpandQuery) Execute(
	ctx context.Context,
	req *openfgav1.ReverseExpandRequest,
	user IsUserRef,
	resultChan chan<- *ReverseExpandResult,
	resolutionMetadata *ResolutionMetadata,
) error {
	params := &ReverseExpandAdditionalParameters{
		resultChan: resultChan,
		User:       user,
	}
	request := openfgav1.BaseRequest{BaseRequest: &openfgav1.BaseRequest_ReverseExpandRequest{ReverseExpandRequest: req}}
	_, _, err := c.Dispatch(ctx, &request, &openfgav1.DispatchMetadata{}, params)
	if err != nil {
		return err
	}

	close(resultChan)
	return nil
}

func (c *ReverseExpandQuery) continueDispatch(ctx context.Context, request *openfgav1.BaseRequest, metadata *openfgav1.DispatchMetadata, additionalParameters any) (*openfgav1.BaseResponse, *openfgav1.DispatchMetadata, error) {
	var err error = nil
	atomic.AddUint32(&metadata.DispatchCount, 1)
	if c.delegate != nil {
		_, _, err = c.delegate.Dispatch(ctx, request, metadata, additionalParameters)
	} else {
		_, _, err = c.Dispatch(ctx, request, metadata, additionalParameters)
	}
	return nil, nil, err
}

func (c *ReverseExpandQuery) Dispatch(ctx context.Context, request *openfgav1.BaseRequest, metadata *openfgav1.DispatchMetadata, additionalParameters any) (*openfgav1.BaseResponse, *openfgav1.DispatchMetadata, error) {
	log.Printf("📋 Reverse Expand Dispatcher running in %s", serverconfig.ServerName)
	params := additionalParameters.(*ReverseExpandAdditionalParameters)
	req := request.GetReverseExpandRequest()
	if ctx.Err() != nil {
		return nil, metadata, ctx.Err()
	}

	ctx, span := tracer.Start(ctx, "reverseExpand.Execute", trace.WithAttributes(
		attribute.String("target_type", req.ObjectType),
		attribute.String("target_relation", req.Relation),
		attribute.String("source", params.User.String()),
	))
	defer span.End()

	if req.Edge != nil {
		span.SetAttributes(attribute.String("edge", req.Edge.String()))
	}

	depth, ok := graph.ResolutionDepthFromContext(ctx)
	if !ok {
		ctx = graph.ContextWithResolutionDepth(ctx, 0)
	} else {
		if depth >= c.resolveNodeLimit {
			return nil, nil, graph.ErrResolutionDepthExceeded
		}

		ctx = graph.ContextWithResolutionDepth(ctx, depth+1)
	}

	var sourceUserRef *openfgav1.RelationReference
	var sourceUserType, sourceUserObj string

	// e.g. 'user:bob'
	if val, ok := params.User.(*UserRefObject); ok {
		sourceUserType = val.Object.GetType()
		sourceUserObj = tuple.BuildObject(sourceUserType, val.Object.GetId())
		sourceUserRef = typesystem.DirectRelationReference(sourceUserType, "")
	}

	// e.g. 'user:*'
	if val, ok := params.User.(*UserRefTypedWildcard); ok {
		sourceUserType = val.Type
		sourceUserRef = typesystem.WildcardRelationReference(sourceUserType)
	}

	// e.g. 'group:eng#member'
	if val, ok := params.User.(*UserRefObjectRelation); ok {
		sourceUserType = tuple.GetType(val.ObjectRelation.GetObject())
		sourceUserObj = val.ObjectRelation.GetObject()
		sourceUserRef = typesystem.DirectRelationReference(sourceUserType, val.ObjectRelation.GetRelation())

		if req.Edge != nil {
			key := fmt.Sprintf("%s#%s", sourceUserObj, req.Edge.String())
			if _, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{}); loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle
				return nil, nil, nil
			}

			sourceUserRel := val.ObjectRelation.GetRelation()

			if sourceUserType == req.ObjectType && sourceUserRel == req.Relation {
				if err := c.trySendCandidate(ctx, req.IntersectionOrExclusionInPreviousEdges, sourceUserObj, params.resultChan); err != nil {
					return nil, nil, err
				}
			}
		}
	}

	targetObjRef := typesystem.DirectRelationReference(req.ObjectType, req.Relation)

	g := graph.New(c.typesystem)

	edges, err := g.GetPrunedRelationshipEdges(targetObjRef, sourceUserRef)
	if err != nil {
		return nil, nil, err
	}

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithFirstError()
	pool.WithMaxGoroutines(int(c.resolveNodeBreadthLimit))
	var errs *multierror.Error

LoopOnEdges:
	for _, edge := range edges {
		innerLoopEdge := edge
		var inner *openfgav1.RelationshipEdge = innerLoopEdge
		intersectionOrExclusionInPreviousEdges := req.IntersectionOrExclusionInPreviousEdges || inner.TargetReferenceInvolvesIntersectionOrExclusion
		r := &openfgav1.ReverseExpandRequest{
			StoreId:          req.StoreId,
			ObjectType:       req.ObjectType,
			Relation:         req.Relation,
			ContextualTuples: req.ContextualTuples,
			Context:          req.Context,
			Edge:             edge,
		}
		switch inner.Type {
		case int32(graph.DirectEdge):
			pool.Go(func(ctx context.Context) error {
				r.IntersectionOrExclusionInPreviousEdges = intersectionOrExclusionInPreviousEdges
				params := additionalParameters.(*ReverseExpandAdditionalParameters)
				return c.reverseExpandDirect(ctx, r, metadata, *params)
			})
		case int32(graph.ComputedUsersetEdge):
			// follow the computed_userset edge, no new goroutine needed since it's not I/O intensive
			params.User = &UserRefObjectRelation{
				ObjectRelation: &openfgav1.ObjectRelation{
					Object:   sourceUserObj,
					Relation: inner.TargetReference.GetRelation(),
				},
			}
			r.IntersectionOrExclusionInPreviousEdges = intersectionOrExclusionInPreviousEdges
			_, _, err = c.continueDispatch(ctx, &openfgav1.BaseRequest{BaseRequest: &openfgav1.BaseRequest_ReverseExpandRequest{ReverseExpandRequest: req}}, metadata, params)
			if err != nil {
				errs = multierror.Append(errs, err)
				break LoopOnEdges
			}
		case int32(graph.TupleToUsersetEdge):
			pool.Go(func(ctx context.Context) error {
				return c.reverseExpandTupleToUserset(ctx, r, metadata, *params)
			})
		default:
			panic("unsupported edge type")
		}
	}

	err = pool.Wait()
	if err != nil {
		errs = multierror.Append(errs, err)
	}
	if errs.ErrorOrNil() != nil {
		telemetry.TraceError(span, errs.ErrorOrNil())
		return nil, nil, errs.ErrorOrNil()
	}

	return nil, nil, nil
}

func (c *ReverseExpandQuery) reverseExpandTupleToUserset(
	ctx context.Context,
	req *openfgav1.ReverseExpandRequest,
	resolutionMetadata *openfgav1.DispatchMetadata,
	params ReverseExpandAdditionalParameters,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandTupleToUserset", trace.WithAttributes(
		attribute.String("edge", req.Edge.String()),
		attribute.String("source.user", params.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	err = c.readTuplesAndExecute(ctx, req, resolutionMetadata, params)
	return err
}

func (c *ReverseExpandQuery) reverseExpandDirect(
	ctx context.Context,
	req *openfgav1.ReverseExpandRequest,
	resolutionMetadata *openfgav1.DispatchMetadata,
	params ReverseExpandAdditionalParameters,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandDirect", trace.WithAttributes(
		attribute.String("edge", req.Edge.String()),
		attribute.String("source.user", params.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	err = c.readTuplesAndExecute(ctx, req, resolutionMetadata, params)
	return err
}

func (c *ReverseExpandQuery) readTuplesAndExecute(
	ctx context.Context,
	req *openfgav1.ReverseExpandRequest,
	resolutionMetadata *openfgav1.DispatchMetadata,
	params ReverseExpandAdditionalParameters,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ctx, span := tracer.Start(ctx, "readTuplesAndExecute")
	defer span.End()

	var userFilter []*openfgav1.ObjectRelation
	var relationFilter string

	switch req.Edge.Type {
	case int32(graph.DirectEdge):
		relationFilter = req.Edge.TargetReference.GetRelation()
		targetUserObjectType := params.User.GetObjectType()

		publiclyAssignable, err := c.typesystem.IsPubliclyAssignable(req.Edge.TargetReference, targetUserObjectType)
		if err != nil {
			return err
		}

		if publiclyAssignable {
			// e.g. 'user:*'
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: fmt.Sprintf("%s:*", targetUserObjectType),
			})
		}

		// e.g. 'user:bob'
		if val, ok := params.User.(*UserRefObject); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: tuple.BuildObject(val.Object.GetType(), val.Object.GetId()),
			})
		}

		// e.g. 'group:eng#member'
		if val, ok := params.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, val.ObjectRelation)
		}
	case int32(graph.TupleToUsersetEdge):
		relationFilter = req.Edge.TuplesetRelation
		// a TTU edge can only have a userset as a source node
		// e.g. 'group:eng#member'
		if val, ok := params.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: val.ObjectRelation.GetObject(),
			})
		} else {
			panic("unexpected source for reverse expansion of tuple to userset")
		}
	default:
		panic("unsupported edge type")
	}

	combinedTupleReader := storagewrappers.NewCombinedTupleReader(c.datastore, req.ContextualTuples)

	// find all tuples of the form req.edge.TargetReference.Type:...#relationFilter@userFilter
	iter, err := combinedTupleReader.ReadStartingWithUser(ctx, req.StoreId, storage.ReadStartingWithUserFilter{
		ObjectType: req.Edge.TargetReference.GetType(),
		Relation:   relationFilter,
		UserFilter: userFilter,
	})
	atomic.AddUint32(&resolutionMetadata.DatastoreQueryCount, 1)
	if err != nil {
		return err
	}

	// filter out invalid tuples yielded by the database iterator
	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		func(tupleKey *openfgav1.TupleKey) bool {
			return validation.ValidateCondition(c.typesystem, tupleKey) == nil
		},
	)
	defer filteredIter.Stop()

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithFirstError()
	pool.WithMaxGoroutines(int(c.resolveNodeBreadthLimit))

	var errs *multierror.Error

LoopOnIterator:
	for {
		tk, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			errs = multierror.Append(errs, err)
			break LoopOnIterator
		}

		condEvalResult, err := eval.EvaluateTupleCondition(ctx, tk, c.typesystem, req.Context)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		if !condEvalResult.ConditionMet {
			if len(condEvalResult.MissingParameters) > 0 {
				errs = multierror.Append(errs, condition.NewEvaluationError(
					tk.GetCondition().GetName(),
					fmt.Errorf("tuple '%s' is missing context parameters '%v'",
						tuple.TupleKeyToString(tk),
						condEvalResult.MissingParameters),
				))
			}

			continue
		}

		foundObject := tk.GetObject()
		var newRelation string

		switch req.Edge.Type {
		case int32(graph.DirectEdge):
			newRelation = tk.GetRelation()
		case int32(graph.TupleToUsersetEdge):
			newRelation = req.Edge.TargetReference.GetRelation()
		default:
			panic("unsupported edge type")
		}

		pool.Go(func(ctx context.Context) error {
			req := &openfgav1.ReverseExpandRequest{
				StoreId:                                req.StoreId,
				ObjectType:                             req.ObjectType,
				Relation:                               req.Relation,
				IntersectionOrExclusionInPreviousEdges: req.IntersectionOrExclusionInPreviousEdges,
				ContextualTuples:                       req.ContextualTuples,
				Context:                                req.Context,
				Edge:                                   req.Edge,
			}
			user := &UserRefObjectRelation{
				ObjectRelation: &openfgav1.ObjectRelation{
					Object:   foundObject,
					Relation: newRelation,
				},
				Condition: tk.GetCondition(),
			}
			additionalParams := &ReverseExpandAdditionalParameters{
				User:       user,
				resultChan: params.resultChan,
			}
			request := &openfgav1.BaseRequest{BaseRequest: &openfgav1.BaseRequest_ReverseExpandRequest{ReverseExpandRequest: req}}
			_, _, err = c.continueDispatch(ctx, request, resolutionMetadata, additionalParams)
			return err
		})
	}

	errs = multierror.Append(errs, pool.Wait())
	if errs.ErrorOrNil() != nil {
		telemetry.TraceError(span, errs.ErrorOrNil())
		return errs
	}

	return nil
}

func (c *ReverseExpandQuery) trySendCandidate(ctx context.Context, intersectionOrExclusionInPreviousEdges bool, candidateObject string, candidateChan chan<- *ReverseExpandResult) error {
	_, span := tracer.Start(ctx, "trySendCandidate", trace.WithAttributes(
		attribute.String("object", candidateObject),
		attribute.Bool("sent", false),
	))
	defer span.End()

	if _, ok := c.candidateObjectsMap.LoadOrStore(candidateObject, struct{}{}); !ok {
		resultStatus := NoFurtherEvalStatus
		if intersectionOrExclusionInPreviousEdges {
			span.SetAttributes(attribute.Bool("requires_further_eval", true))
			resultStatus = RequiresFurtherEvalStatus
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case candidateChan <- &ReverseExpandResult{
			Object:       candidateObject,
			ResultStatus: resultStatus,
		}:
			span.SetAttributes(attribute.Bool("sent", true))
		}
	}

	return nil
}
