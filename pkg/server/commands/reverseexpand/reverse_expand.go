// Package reverseexpand contains the code that handles the ReverseExpand API
package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/graph"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/sourcegraph/conc/pool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("openfga/pkg/server/commands/reverse_expand")

type ReverseExpandRequest struct {
	StoreID          string
	ObjectType       string
	Relation         string
	User             IsUserRef
	ContextualTuples []*openfgav1.TupleKey

	edge *graph.RelationshipEdge
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
	return u.Object.Type
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
}

func (*UserRefObjectRelation) isUserRef() {}

func (u *UserRefObjectRelation) GetObjectType() string {
	return tuple.GetType(u.ObjectRelation.Object)
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

type ReverseExpandQuery struct {
	datastore               storage.RelationshipTupleReader
	typesystem              *typesystem.TypeSystem
	resolveNodeLimit        uint32
	resolveNodeBreadthLimit uint32

	// visitedUsersetsMap map prevents visiting the same userset through the same edge twice
	visitedUsersetsMap *sync.Map
	// candidateObjectsMap map prevents returning the same object twice
	candidateObjectsMap *sync.Map
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
	Err          error
	Object       string
	ResultStatus ConditionalResultStatus
}

type ResolutionMetadata struct {
	QueryCount *uint32
}

func NewResolutionMetadata() *ResolutionMetadata {
	return &ResolutionMetadata{
		QueryCount: new(uint32),
	}
}

// Execute yields all the objects of the provided objectType that the given user has, possibly, a specific relation with
// and sends those objects to resultChan. It MUST guarantee no duplicate objects sent.
//
// If an error is encountered before resolving all objects: the provided channel will NOT be closed and
// - if the error is context cancellation or deadline: Execute may send the error through the channel
// - otherwise: Execute will send the error through the channel
// If no errors, Execute will yield all of the objects on the provided channel and then close the channel
// to signal that it is done.
func (c *ReverseExpandQuery) Execute(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	resolutionMetadata *ResolutionMetadata,
) {
	err := c.execute(ctx, req, resultChan, false, resolutionMetadata)
	if err != nil {
		select {
		case <-ctx.Done():
			return
		case resultChan <- &ReverseExpandResult{Err: err}:
			return
		}
	}

	close(resultChan)
}

func (c *ReverseExpandQuery) execute(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ctx, span := tracer.Start(ctx, "reverseExpand.Execute", trace.WithAttributes(
		attribute.String("target_type", req.ObjectType),
		attribute.String("target_relation", req.Relation),
		attribute.String("source", req.User.String()),
	))
	defer span.End()

	if req.edge != nil {
		span.SetAttributes(attribute.String("edge", req.edge.String()))
	}

	depth, ok := graph.ResolutionDepthFromContext(ctx)
	if !ok {
		ctx = graph.ContextWithResolutionDepth(ctx, 0)
	} else {
		if depth >= c.resolveNodeLimit {
			return graph.ErrResolutionDepthExceeded
		}

		ctx = graph.ContextWithResolutionDepth(ctx, depth+1)
	}

	var sourceUserRef *openfgav1.RelationReference
	var sourceUserType, sourceUserObj string

	// e.g. 'user:bob'
	if val, ok := req.User.(*UserRefObject); ok {
		sourceUserType = val.Object.GetType()
		sourceUserObj = tuple.BuildObject(sourceUserType, val.Object.GetId())
		sourceUserRef = typesystem.DirectRelationReference(sourceUserType, "")
	}

	// e.g. 'user:*'
	if val, ok := req.User.(*UserRefTypedWildcard); ok {
		sourceUserType = val.Type
		sourceUserRef = typesystem.WildcardRelationReference(sourceUserType)
	}

	// e.g. 'group:eng#member'
	if val, ok := req.User.(*UserRefObjectRelation); ok {
		sourceUserType = tuple.GetType(val.ObjectRelation.GetObject())
		sourceUserObj = val.ObjectRelation.Object
		sourceUserRef = typesystem.DirectRelationReference(sourceUserType, val.ObjectRelation.GetRelation())
		sourceUserRel := val.ObjectRelation.GetRelation()

		if req.edge != nil {
			key := fmt.Sprintf("%s#%s", sourceUserObj, req.edge.String())
			if _, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{}); loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle
				return nil
			}

			if sourceUserType == req.ObjectType && sourceUserRel == req.Relation {
				if err := c.trySendCandidate(ctx, intersectionOrExclusionInPreviousEdges, sourceUserObj, resultChan); err != nil {
					return err
				}
			}
		}
	}

	targetObjRef := typesystem.DirectRelationReference(req.ObjectType, req.Relation)

	g := graph.New(c.typesystem)

	edges, err := g.GetPrunedRelationshipEdges(targetObjRef, sourceUserRef)
	if err != nil {
		return err
	}

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithFirstError()
	pool.WithMaxGoroutines(int(c.resolveNodeBreadthLimit))

	for _, edge := range edges {
		innerLoopEdge := edge
		intersectionOrExclusionInPreviousEdges := intersectionOrExclusionInPreviousEdges || innerLoopEdge.TargetReferenceInvolvesIntersectionOrExclusion

		pool.Go(func(ctx context.Context) error {
			r := &ReverseExpandRequest{
				StoreID:          req.StoreID,
				ObjectType:       req.ObjectType,
				Relation:         req.Relation,
				User:             req.User,
				ContextualTuples: req.ContextualTuples,
				edge:             innerLoopEdge,
			}

			switch innerLoopEdge.Type {
			case graph.DirectEdge:
				return c.reverseExpandDirect(ctx, r, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
			case graph.ComputedUsersetEdge:
				// follow the computed_userset edge
				r.User = &UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   sourceUserObj,
						Relation: innerLoopEdge.TargetReference.GetRelation(),
					},
				}
				return c.execute(ctx, r, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
			case graph.TupleToUsersetEdge:
				return c.reverseExpandTupleToUserset(ctx, r, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
			default:
				return fmt.Errorf("unsupported edge type")
			}
		})
	}

	return pool.Wait()
}

func (c *ReverseExpandQuery) reverseExpandTupleToUserset(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandTupleToUserset", trace.WithAttributes(
		attribute.String("edge", req.edge.String()),
		attribute.String("source.user", req.User.String()),
	))
	defer span.End()

	return c.readTuplesAndExecute(ctx, req, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
}

func (c *ReverseExpandQuery) reverseExpandDirect(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandDirect", trace.WithAttributes(
		attribute.String("edge", req.edge.String()),
		attribute.String("source.user", req.User.String()),
	))
	defer span.End()

	return c.readTuplesAndExecute(ctx, req, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
}

func (c *ReverseExpandQuery) readTuplesAndExecute(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	var userFilter []*openfgav1.ObjectRelation
	var relationFilter string

	switch req.edge.Type {
	case graph.DirectEdge:
		relationFilter = req.edge.TargetReference.GetRelation()
		targetUserObjectType := req.User.GetObjectType()

		publiclyAssignable, err := c.typesystem.IsPubliclyAssignable(req.edge.TargetReference, targetUserObjectType)
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
		if val, ok := req.User.(*UserRefObject); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: tuple.BuildObject(val.Object.Type, val.Object.Id),
			})
		}

		// e.g. 'group:eng#member'
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, val.ObjectRelation)
		}
	case graph.TupleToUsersetEdge:
		relationFilter = req.edge.TuplesetRelation
		// a TTU edge can only have a userset as a source node
		// e.g. 'group:eng#member'
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: val.ObjectRelation.Object,
			})
		} else {
			panic("unexpected source for reverse expansion of tuple to userset")
		}
	default:
		return fmt.Errorf("unsupported edge type")
	}

	combinedTupleReader := storagewrappers.NewCombinedTupleReader(c.datastore, req.ContextualTuples)

	// find all tuples of the form req.edge.TargetReference.Type:...#relationFilter@userFilter
	iter, err := combinedTupleReader.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
		ObjectType: req.edge.TargetReference.GetType(),
		Relation:   relationFilter,
		UserFilter: userFilter,
	})
	atomic.AddUint32(resolutionMetadata.QueryCount, 1)
	if err != nil {
		return err
	}
	defer iter.Stop()

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithFirstError()
	pool.WithMaxGoroutines(int(c.resolveNodeBreadthLimit))

	for {
		t, err := iter.Next()
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			return err
		}

		tk := t.GetKey()

		foundObject := tk.GetObject()
		var newRelation string

		switch req.edge.Type {
		case graph.DirectEdge:
			newRelation = tk.GetRelation()
		case graph.TupleToUsersetEdge:
			newRelation = req.edge.TargetReference.GetRelation()
		default:
			return fmt.Errorf("unsupported edge type")
		}

		pool.Go(func(ctx context.Context) error {
			return c.execute(ctx, &ReverseExpandRequest{
				StoreID:    req.StoreID,
				ObjectType: req.ObjectType,
				Relation:   req.Relation,
				User: &UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   foundObject,
						Relation: newRelation,
					},
				},
				ContextualTuples: req.ContextualTuples,
				edge:             req.edge,
			}, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
		})
	}

	return pool.Wait()
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
