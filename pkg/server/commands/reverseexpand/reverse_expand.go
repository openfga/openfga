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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

var tracer = otel.Tracer("openfga/pkg/server/commands/reverse_expand")

type ReverseExpandRequest struct {
	StoreID          string
	ObjectType       string
	Relation         string
	User             IsUserRef
	ContextualTuples []*openfgav1.TupleKey
}

// TODO combine with above?
type reverseExpandRequest struct {
	storeID          string
	edge             *graph.RelationshipEdge
	targetObjectRef  *openfgav1.RelationReference
	sourceUserRef    IsUserRef
	contextualTuples []*openfgav1.TupleKey
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
func (c *ReverseExpandQuery) Execute(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	resolutionMetadata *ResolutionMetadata,
) error {
	return c.execute(ctx, req, resultChan, nil, resolutionMetadata)
}

func (c *ReverseExpandQuery) execute(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	currentEdge *graph.RelationshipEdge,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpand.Execute", trace.WithAttributes(
		attribute.String("target_type", req.ObjectType),
		attribute.String("target_relation", req.Relation),
		attribute.String("source", req.User.String()),
	))
	defer span.End()

	if currentEdge != nil {
		span.SetAttributes(attribute.String("edge", currentEdge.String()))
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

	storeID := req.StoreID

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

		if currentEdge != nil {
			key := fmt.Sprintf("%s#%s", sourceUserObj, currentEdge.String())
			if _, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{}); loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle
				return nil
			}

			if sourceUserType == req.ObjectType && sourceUserRel == req.Relation {
				c.trySendCandidate(ctx, currentEdge, sourceUserObj, resultChan)
			}
		}
	}

	targetObjRef := typesystem.DirectRelationReference(req.ObjectType, req.Relation)

	g := graph.New(c.typesystem)

	edges, err := g.GetPrunedRelationshipEdges(targetObjRef, sourceUserRef)
	if err != nil {
		return err
	}

	subg, subgctx := errgroup.WithContext(ctx)
	subg.SetLimit(int(c.resolveNodeBreadthLimit))

	for _, edge := range edges {
		innerLoopEdge := edge
		if currentEdge != nil && currentEdge.Condition == graph.RequiresFurtherEvalCondition {
			// propagate the condition to upcoming reverse expansions
			// TODO don't mutate the edge, keep track of the previous edge's condition and use it in trySendCandidate
			innerLoopEdge.Condition = graph.RequiresFurtherEvalCondition
		}
		subg.Go(func() error {
			r := &reverseExpandRequest{
				storeID:          storeID,
				edge:             innerLoopEdge,
				targetObjectRef:  targetObjRef,
				sourceUserRef:    req.User,
				contextualTuples: req.ContextualTuples,
			}

			switch innerLoopEdge.Type {
			case graph.DirectEdge:
				return c.reverseExpandDirect(subgctx, r, resultChan, resolutionMetadata)
			case graph.ComputedUsersetEdge:
				// lookup the rewritten target relation on the computed_userset ingress
				return c.execute(subgctx, &ReverseExpandRequest{
					StoreID:    storeID,
					ObjectType: req.ObjectType,
					Relation:   req.Relation,
					User: &UserRefObjectRelation{
						ObjectRelation: &openfgav1.ObjectRelation{
							Object:   sourceUserObj,
							Relation: innerLoopEdge.TargetReference.GetRelation(),
						},
					},
					ContextualTuples: r.contextualTuples,
				}, resultChan, innerLoopEdge, resolutionMetadata)
			case graph.TupleToUsersetEdge:
				return c.reverseExpandTupleToUserset(subgctx, r, resultChan, resolutionMetadata)
			default:
				return fmt.Errorf("unsupported edge type")
			}
		})
	}

	return subg.Wait()
}

func (c *ReverseExpandQuery) reverseExpandTupleToUserset(
	ctx context.Context,
	req *reverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandTupleToUserset", trace.WithAttributes(
		attribute.String("edge", req.edge.String()),
		attribute.String("source.user", req.sourceUserRef.String()),
	))
	defer span.End()

	store := req.storeID

	targetObjectType := req.targetObjectRef.GetType()
	targetObjectRel := req.targetObjectRef.GetRelation()

	var userFilter []*openfgav1.ObjectRelation

	// a TTU edge can only have a userset as a source node
	// e.g. 'group:eng#member'
	if val, ok := req.sourceUserRef.(*UserRefObjectRelation); ok {
		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object: val.ObjectRelation.Object,
		})
	} else {
		panic("unexpected source for reverse expansion of tuple to userset")
	}

	combinedTupleReader := storagewrappers.NewContextualTupleReader(c.datastore, req.contextualTuples)

	// find all tuples of the form req.edge.TargetReference.Type:...#req.edge.TuplesetRelation@req.sourceUserRef
	iter, err := combinedTupleReader.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: req.edge.TargetReference.GetType(),
		Relation:   req.edge.TuplesetRelation.GetRelation(),
		UserFilter: userFilter,
	})
	atomic.AddUint32(resolutionMetadata.QueryCount, 1)
	if err != nil {
		return err
	}
	defer iter.Stop()

	subg, subgctx := errgroup.WithContext(ctx)
	subg.SetLimit(int(c.resolveNodeBreadthLimit))

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

		sourceUserRef := &UserRefObjectRelation{
			ObjectRelation: &openfgav1.ObjectRelation{
				Object:   foundObject,
				Relation: req.edge.TargetReference.GetRelation(),
			},
		}

		subg.Go(func() error {
			return c.execute(subgctx, &ReverseExpandRequest{
				StoreID:          store,
				ObjectType:       targetObjectType,
				Relation:         targetObjectRel,
				User:             sourceUserRef,
				ContextualTuples: req.contextualTuples,
			}, resultChan, req.edge, resolutionMetadata)
		})
	}

	return subg.Wait()
}

func (c *ReverseExpandQuery) reverseExpandDirect(
	ctx context.Context,
	req *reverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandDirect", trace.WithAttributes(
		attribute.String("edge", req.edge.String()),
		attribute.String("source.user", req.sourceUserRef.String()),
	))
	defer span.End()

	store := req.storeID

	targetObjectType := req.targetObjectRef.GetType()
	targetObjectRel := req.targetObjectRef.GetRelation()

	var userFilter []*openfgav1.ObjectRelation

	targetUserObjectType := req.sourceUserRef.GetObjectType()

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

	sourceRelationRef := &openfgav1.RelationReference{
		Type: req.sourceUserRef.GetObjectType(),
	}

	// e.g. 'user:bob'
	if val, ok := req.sourceUserRef.(*UserRefObject); ok {
		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object: tuple.BuildObject(val.Object.Type, val.Object.Id),
		})
	}

	// e.g. 'group:eng#member'
	if val, ok := req.sourceUserRef.(*UserRefObjectRelation); ok {
		sourceRelationRef.RelationOrWildcard = &openfgav1.RelationReference_Relation{
			Relation: val.ObjectRelation.Relation,
		}

		userFilter = append(userFilter, val.ObjectRelation)
	}

	combinedTupleReader := storagewrappers.NewContextualTupleReader(c.datastore, req.contextualTuples)

	// find all tuples of the form req.edge.TargetReference.Type:...#req.edge.TargetReference.Relation@req.sourceUserRef
	iter, err := combinedTupleReader.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: req.edge.TargetReference.GetType(),
		Relation:   req.edge.TargetReference.GetRelation(),
		UserFilter: userFilter,
	})
	atomic.AddUint32(resolutionMetadata.QueryCount, 1)
	if err != nil {
		return err
	}
	defer iter.Stop()

	subg, subgctx := errgroup.WithContext(ctx)
	subg.SetLimit(int(c.resolveNodeBreadthLimit))

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

		sourceUserRef := &UserRefObjectRelation{
			ObjectRelation: &openfgav1.ObjectRelation{
				Object:   foundObject,
				Relation: tk.GetRelation(),
			},
		}

		subg.Go(func() error {
			return c.execute(subgctx, &ReverseExpandRequest{
				StoreID:          store,
				ObjectType:       targetObjectType,
				Relation:         targetObjectRel,
				User:             sourceUserRef,
				ContextualTuples: req.contextualTuples,
			}, resultChan, req.edge, resolutionMetadata)
		})
	}

	return subg.Wait()
}

func (c *ReverseExpandQuery) trySendCandidate(ctx context.Context, edge *graph.RelationshipEdge, candidateObject string, candidateChan chan<- *ReverseExpandResult) {
	_, span := tracer.Start(ctx, "trySendCandidate", trace.WithAttributes(
		attribute.String("object", candidateObject),
		attribute.Bool("sent", false),
	))
	defer span.End()
	if _, ok := c.candidateObjectsMap.LoadOrStore(candidateObject, struct{}{}); !ok {
		resultStatus := NoFurtherEvalStatus
		if edge != nil && edge.Condition == graph.RequiresFurtherEvalCondition {
			span.SetAttributes(attribute.Bool("requires_further_eval", true))
			resultStatus = RequiresFurtherEvalStatus
		}
		candidateChan <- &ReverseExpandResult{
			Object:       candidateObject,
			ResultStatus: resultStatus,
		}
		span.SetAttributes(attribute.Bool("sent", true))
	}
}
