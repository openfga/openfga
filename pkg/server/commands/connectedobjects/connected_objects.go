// Package connectedobjects contains the code that handles the ConnectedObjects API
package connectedobjects

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/graph"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

var tracer = otel.Tracer("openfga/pkg/server/commands/connected_objects")

const (
	// same values as run.DefaultConfig() (TODO break the import cycle, remove these hardcoded values and import those constants here)
	defaultResolveNodeLimit        = 25
	defaultResolveNodeBreadthLimit = 100
)

type ConnectedObjectsRequest struct {
	StoreID          string
	ObjectType       string
	Relation         string
	User             IsUserRef
	ContextualTuples []*openfgav1.TupleKey
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

type ConnectedObjectsQuery struct {
	datastore               storage.RelationshipTupleReader
	typesystem              *typesystem.TypeSystem
	resolveNodeLimit        uint32
	resolveNodeBreadthLimit uint32

	// visitedUsersetsMap map prevents visiting the same userset through the same ingress twice
	visitedUsersetsMap *sync.Map
	// candidateObjectsMap map prevents returning the same object twice
	candidateObjectsMap *sync.Map
}

type ConnectedObjectsQueryOption func(d *ConnectedObjectsQuery)

func WithResolveNodeLimit(limit uint32) ConnectedObjectsQueryOption {
	return func(d *ConnectedObjectsQuery) {
		d.resolveNodeLimit = limit
	}
}

func WithResolveNodeBreadthLimit(limit uint32) ConnectedObjectsQueryOption {
	return func(d *ConnectedObjectsQuery) {
		d.resolveNodeBreadthLimit = limit
	}
}

func NewConnectedObjectsQuery(ds storage.RelationshipTupleReader, ts *typesystem.TypeSystem, opts ...ConnectedObjectsQueryOption) *ConnectedObjectsQuery {
	query := &ConnectedObjectsQuery{
		datastore:               ds,
		typesystem:              ts,
		resolveNodeLimit:        defaultResolveNodeLimit,
		resolveNodeBreadthLimit: defaultResolveNodeBreadthLimit,
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

type ConnectedObjectsResult struct {
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
func (c *ConnectedObjectsQuery) Execute(
	ctx context.Context,
	req *ConnectedObjectsRequest,
	resultChan chan<- *ConnectedObjectsResult,
	resolutionMetadata *ResolutionMetadata,
) error {
	return c.execute(ctx, req, resultChan, nil, resolutionMetadata)
}

func (c *ConnectedObjectsQuery) execute(
	ctx context.Context,
	req *ConnectedObjectsRequest,
	resultChan chan<- *ConnectedObjectsResult,
	currentIngress *graph.RelationshipIngress,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "connectedObjects.Execute", trace.WithAttributes(
		attribute.String("target_type", req.ObjectType),
		attribute.String("target_relation", req.Relation),
		attribute.String("source", req.User.String()),
	))
	defer span.End()

	if currentIngress != nil {
		span.SetAttributes(attribute.String("ingress", currentIngress.String()))
	}

	depth, ok := graph.ResolutionDepthFromContext(ctx)
	if !ok {
		ctx = graph.ContextWithResolutionDepth(ctx, 0)
	} else {
		if depth >= c.resolveNodeLimit {
			return serverErrors.AuthorizationModelResolutionTooComplex
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

		if currentIngress != nil {
			key := fmt.Sprintf("%s#%s", sourceUserObj, currentIngress.String())
			if _, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{}); loaded {
				// we've already visited this userset through this ingress, exit to avoid an infinite cycle
				return nil
			}

			if sourceUserType == req.ObjectType && sourceUserRel == req.Relation {
				c.trySendCandidate(ctx, currentIngress, sourceUserObj, resultChan)
			}
		}
	}

	targetObjRef := typesystem.DirectRelationReference(req.ObjectType, req.Relation)

	// build the graph of possible edges between object types in the graph based on the authz model's type info
	g := graph.BuildConnectedObjectGraph(c.typesystem)

	// find all paths from target to source and then returns all the edges at distance 0 or 1 in those paths
	// if the target relation is an intersection or a difference, e.g. viewer: can_view but not blocked
	// it returns only one of those edges (the first one - e.g. can_view)
	ingresses, err := g.PrunedRelationshipIngresses(targetObjRef, sourceUserRef)
	if err != nil {
		return err
	}

	subg, subgctx := errgroup.WithContext(ctx)
	subg.SetLimit(int(c.resolveNodeBreadthLimit))

	for _, ingress := range ingresses {
		innerLoopIngress := ingress
		if currentIngress != nil && currentIngress.Condition == graph.RequiresFurtherEvalCondition {
			// propagate the condition to upcoming reverse expansions
			// TODO don't mutate the ingress, keep track of the previous ingress's condition and use it in trySendCandidate
			innerLoopIngress.Condition = graph.RequiresFurtherEvalCondition
		}
		subg.Go(func() error {
			r := &reverseExpandRequest{
				storeID:          storeID,
				ingress:          innerLoopIngress,
				targetObjectRef:  targetObjRef,
				sourceUserRef:    req.User,
				contextualTuples: req.ContextualTuples,
			}

			switch innerLoopIngress.Type {
			case graph.DirectIngress:
				return c.reverseExpandDirect(subgctx, r, resultChan, resolutionMetadata)
			case graph.ComputedUsersetIngress:
				// lookup the rewritten target relation on the computed_userset ingress
				return c.execute(subgctx, &ConnectedObjectsRequest{
					StoreID:    storeID,
					ObjectType: req.ObjectType,
					Relation:   req.Relation,
					User: &UserRefObjectRelation{
						ObjectRelation: &openfgav1.ObjectRelation{
							Object:   sourceUserObj,
							Relation: innerLoopIngress.Ingress.GetRelation(),
						},
					},
					ContextualTuples: r.contextualTuples,
				}, resultChan, innerLoopIngress, resolutionMetadata)
			case graph.TupleToUsersetIngress:
				return c.reverseExpandTupleToUserset(subgctx, r, resultChan, resolutionMetadata)
			default:
				return fmt.Errorf("unsupported ingress type")
			}
		})
	}

	return subg.Wait()
}

type reverseExpandRequest struct {
	storeID          string
	ingress          *graph.RelationshipIngress
	targetObjectRef  *openfgav1.RelationReference
	sourceUserRef    IsUserRef
	contextualTuples []*openfgav1.TupleKey
}

func (c *ConnectedObjectsQuery) reverseExpandTupleToUserset(
	ctx context.Context,
	req *reverseExpandRequest,
	resultChan chan<- *ConnectedObjectsResult,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandTupleToUserset", trace.WithAttributes(
		attribute.String("ingress", req.ingress.String()),
		attribute.String("source.user", req.sourceUserRef.String()),
	))
	defer span.End()

	store := req.storeID

	ingress := req.ingress.Ingress

	targetObjectType := req.targetObjectRef.GetType()
	targetObjectRel := req.targetObjectRef.GetRelation()

	var userFilter []*openfgav1.ObjectRelation

	// a TTU ingress can only have a userset as a source node
	// e.g. 'group:eng#member'
	if val, ok := req.sourceUserRef.(*UserRefObjectRelation); ok {
		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object: val.ObjectRelation.Object,
		})
	} else {
		panic("unexpected source for reverse expansion of tuple to userset")
	}

	combinedTupleReader := storagewrappers.NewCombinedTupleReader(c.datastore, req.contextualTuples)

	// find all tuples of the form req.ingress.Type:...#req.ingress.TuplesetRelation@req.sourceUserRef
	iter, err := combinedTupleReader.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: req.ingress.Ingress.GetType(),
		Relation:   req.ingress.TuplesetRelation.GetRelation(),
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
				Relation: ingress.GetRelation(),
			},
		}

		subg.Go(func() error {
			return c.execute(subgctx, &ConnectedObjectsRequest{
				StoreID:          store,
				ObjectType:       targetObjectType,
				Relation:         targetObjectRel,
				User:             sourceUserRef,
				ContextualTuples: req.contextualTuples,
			}, resultChan, req.ingress, resolutionMetadata)
		})
	}

	return subg.Wait()
}

func (c *ConnectedObjectsQuery) reverseExpandDirect(
	ctx context.Context,
	req *reverseExpandRequest,
	resultChan chan<- *ConnectedObjectsResult,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandDirect", trace.WithAttributes(
		attribute.String("ingress", req.ingress.String()),
		attribute.String("source.user", req.sourceUserRef.String()),
	))
	defer span.End()

	store := req.storeID

	ingress := req.ingress.Ingress

	targetObjectType := req.targetObjectRef.GetType()
	targetObjectRel := req.targetObjectRef.GetRelation()

	var userFilter []*openfgav1.ObjectRelation

	targetUserObjectType := req.sourceUserRef.GetObjectType()

	publiclyAssignable, err := c.typesystem.IsPubliclyAssignable(ingress, targetUserObjectType)
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

	combinedTupleReader := storagewrappers.NewCombinedTupleReader(c.datastore, req.contextualTuples)

	// find all tuples of the form req.ingress.Type:...#req.ingress.Relation@req.sourceUserRef
	iter, err := combinedTupleReader.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: ingress.GetType(),
		Relation:   ingress.GetRelation(),
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
			return c.execute(subgctx, &ConnectedObjectsRequest{
				StoreID:          store,
				ObjectType:       targetObjectType,
				Relation:         targetObjectRel,
				User:             sourceUserRef,
				ContextualTuples: req.contextualTuples,
			}, resultChan, req.ingress, resolutionMetadata)
		})
	}

	return subg.Wait()
}

func (c *ConnectedObjectsQuery) trySendCandidate(ctx context.Context, ingress *graph.RelationshipIngress, candidateObject string, candidateChan chan<- *ConnectedObjectsResult) {
	_, span := tracer.Start(ctx, "trySendCandidate", trace.WithAttributes(
		attribute.String("object", candidateObject),
		attribute.Bool("sent", false),
	))
	defer span.End()
	if _, ok := c.candidateObjectsMap.LoadOrStore(candidateObject, struct{}{}); !ok {
		resultStatus := NoFurtherEvalStatus
		if ingress != nil && ingress.Condition == graph.RequiresFurtherEvalCondition {
			span.SetAttributes(attribute.Bool("requires_further_eval", true))
			resultStatus = RequiresFurtherEvalStatus
		}
		candidateChan <- &ConnectedObjectsResult{
			Object:       candidateObject,
			ResultStatus: resultStatus,
		}
		span.SetAttributes(attribute.Bool("sent", true))
	}
}
