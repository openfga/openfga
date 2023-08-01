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
	defaultMaxResults              = 1000
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
	maxResults              uint32
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

func WithMaxResults(maxResults uint32) ConnectedObjectsQueryOption {
	return func(d *ConnectedObjectsQuery) {
		d.maxResults = maxResults
	}
}

func NewConnectedObjectsQuery(ds storage.RelationshipTupleReader, ts *typesystem.TypeSystem, opts ...ConnectedObjectsQueryOption) *ConnectedObjectsQuery {
	query := &ConnectedObjectsQuery{
		datastore:               ds,
		typesystem:              ts,
		resolveNodeLimit:        defaultResolveNodeLimit,
		resolveNodeBreadthLimit: defaultResolveNodeBreadthLimit,
		maxResults:              defaultMaxResults,
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

func (c *ConnectedObjectsQuery) execute(
	ctx context.Context,
	req *ConnectedObjectsRequest,
	resultChan chan<- *ConnectedObjectsResult,
	foundObjectsMap *sync.Map,
	foundCount *uint32,
) error {
	ctx, span := tracer.Start(ctx, "connectedObjects.execute", trace.WithAttributes(
		attribute.String("object_type", req.ObjectType),
		attribute.String("relation", req.Relation),
		attribute.String("user", req.User.String()),
	))
	defer span.End()

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
	}

	targetObjRef := typesystem.DirectRelationReference(req.ObjectType, req.Relation)

	// build the graph of possible edges between object types in the graph based on the authz model's type info
	g := graph.BuildConnectedObjectGraph(c.typesystem)

	// find the possible incoming edges (ingresses) between the target user reference and the source (object, relation) reference
	span.SetAttributes(
		attribute.String("_sourceUserRef", sourceUserRef.String()),
		attribute.String("_targetObjRef", targetObjRef.String()))
	ingresses, err := g.PrunedRelationshipIngresses(targetObjRef, sourceUserRef)
	if err != nil {
		return err
	}

	subg, subgctx := errgroup.WithContext(ctx)
	subg.SetLimit(int(c.resolveNodeBreadthLimit))

	for i, ingress := range ingresses {
		span.SetAttributes(attribute.String(fmt.Sprintf("_ingress %d", i), ingress.String()))
		innerLoopIngress := ingress
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
				return c.reverseExpandDirect(subgctx, r, resultChan, foundObjectsMap, foundCount)
			case graph.ComputedUsersetIngress:

				// lookup the rewritten target relation on the computed_userset ingress
				return c.execute(ctx, &ConnectedObjectsRequest{
					StoreID:    storeID,
					ObjectType: req.ObjectType,
					Relation:   req.Relation,
					User: &UserRefObjectRelation{
						ObjectRelation: &openfgav1.ObjectRelation{
							Object:   sourceUserObj,
							Relation: innerLoopIngress.Ingress.GetRelation(),
						},
					},
					ContextualTuples: req.ContextualTuples,
				}, resultChan, foundObjectsMap, foundCount)

			case graph.TupleToUsersetIngress:
				return c.reverseExpandTupleToUserset(subgctx, r, resultChan, foundObjectsMap, foundCount)
			default:
				return fmt.Errorf("unsupported ingress type")
			}
		})
	}

	return subg.Wait()
}

// Execute yields all the objects of the provided objectType that
// the given user has a specific relation with. The results will be limited by the request
// maxResults. If a 0 maxResults is provided then all objects of the provided objectType will be
// returned.
func (c *ConnectedObjectsQuery) Execute(
	ctx context.Context,
	req *ConnectedObjectsRequest,
	resultChan chan<- *ConnectedObjectsResult, // object string (e.g. document:1)
) error {
	ctx, span := tracer.Start(ctx, "connectedObjects.Execute", trace.WithAttributes(
		attribute.String("object_type", req.ObjectType),
		attribute.String("relation", req.Relation),
		attribute.String("user", req.User.String()),
	))
	defer span.End()

	var foundCount *uint32
	if c.maxResults > 0 {
		foundCount = new(uint32)
	}

	var foundObjects sync.Map
	return c.execute(ctx, req, resultChan, &foundObjects, foundCount)
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
	foundObjectsMap *sync.Map,
	foundCount *uint32,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandTupleToUserset", trace.WithAttributes(
		attribute.String("target.object_type", req.targetObjectRef.GetType()),
		attribute.String("target.relation", req.targetObjectRef.GetRelation()),
		attribute.String("ingress.object_type", req.ingress.Ingress.GetType()),
		attribute.String("ingress.relation", req.ingress.Ingress.GetRelation()),
		attribute.String("ingress.type", req.ingress.Type.String()),
		attribute.String("source.user", req.sourceUserRef.String()),
	))
	defer span.End()

	store := req.storeID

	ingress := req.ingress.Ingress

	targetObjectType := req.targetObjectRef.GetType()
	targetObjectRel := req.targetObjectRef.GetRelation()

	tuplesetRelation := req.ingress.TuplesetRelation.GetRelation()

	var userFilter []*openfgav1.ObjectRelation

	// e.g. 'user:bob'
	if val, ok := req.sourceUserRef.(*UserRefObject); ok {
		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object: tuple.BuildObject(val.Object.Type, val.Object.Id),
		})
	}

	// e.g. 'group:eng#member'
	if val, ok := req.sourceUserRef.(*UserRefObjectRelation); ok {
		userFilter = append(userFilter, &openfgav1.ObjectRelation{
			Object: val.ObjectRelation.Object,
		})
	}

	combinedTupleReader := storagewrappers.NewCombinedTupleReader(c.datastore, req.contextualTuples)

	iter, err := combinedTupleReader.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: req.ingress.Ingress.GetType(),
		Relation:   tuplesetRelation,
		UserFilter: userFilter,
	})
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
		foundObjectType, foundObjectID := tuple.SplitObject(foundObject)

		if _, ok := foundObjectsMap.LoadOrStore(foundObject, struct{}{}); ok {
			// todo(jon-whit): we could optimize this by avoiding reading this
			// from the database in the first place

			// if we've already evaluated/found the object, then continue
			continue
		}

		if foundObjectType == targetObjectType {
			if foundCount != nil && atomic.AddUint32(foundCount, 1) > c.maxResults {
				break
			}

			resultStatus := NoFurtherEvalStatus
			if req.ingress.Condition == graph.RequiresFurtherEvalCondition {
				resultStatus = RequiresFurtherEvalStatus
			}

			resultChan <- &ConnectedObjectsResult{
				Object:       foundObject,
				ResultStatus: resultStatus,
			}
		}

		var sourceUserRef IsUserRef
		sourceUserRef = &UserRefObject{
			Object: &openfgav1.Object{
				Type: foundObjectType,
				Id:   foundObjectID,
			},
		}

		if _, ok := req.sourceUserRef.(*UserRefTypedWildcard); ok {
			sourceUserRef = &UserRefTypedWildcard{Type: foundObjectType}
		}

		if _, ok := req.sourceUserRef.(*UserRefObjectRelation); ok {
			sourceUserRef = &UserRefObjectRelation{
				ObjectRelation: &openfgav1.ObjectRelation{
					Object:   foundObject,
					Relation: ingress.GetRelation(),
				},
			}
		}

		subg.Go(func() error {
			return c.execute(subgctx, &ConnectedObjectsRequest{
				StoreID:          store,
				ObjectType:       targetObjectType,
				Relation:         targetObjectRel,
				User:             sourceUserRef,
				ContextualTuples: req.contextualTuples,
			}, resultChan, foundObjectsMap, foundCount)
		})
	}

	return subg.Wait()
}

func (c *ConnectedObjectsQuery) reverseExpandDirect(
	ctx context.Context,
	req *reverseExpandRequest,
	resultChan chan<- *ConnectedObjectsResult,
	foundObjectsMap *sync.Map,
	foundCount *uint32,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandDirect", trace.WithAttributes(
		attribute.String("target.object_type", req.targetObjectRef.GetType()),
		attribute.String("target.relation", req.targetObjectRef.GetRelation()),
		attribute.String("ingress.object_type", req.ingress.Ingress.GetType()),
		attribute.String("ingress.relation", req.ingress.Ingress.GetRelation()),
		attribute.String("ingress.type", req.ingress.Type.String()),
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

	iter, err := combinedTupleReader.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: ingress.GetType(),
		Relation:   ingress.GetRelation(),
		UserFilter: userFilter,
	})
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
		foundObjectType, _ := tuple.SplitObject(foundObject)

		if _, ok := foundObjectsMap.LoadOrStore(foundObject, struct{}{}); ok {
			// todo(jon-whit): we could optimize this by avoiding reading this
			// from the database in the first place

			// if we've already evaluated/found the object, then continue
			continue
		}

		if foundObjectType == targetObjectType {
			if foundCount != nil && atomic.AddUint32(foundCount, 1) > c.maxResults {
				break
			}

			resultStatus := NoFurtherEvalStatus
			if req.ingress.Condition == graph.RequiresFurtherEvalCondition {
				resultStatus = RequiresFurtherEvalStatus
			}

			resultChan <- &ConnectedObjectsResult{
				Object:       foundObject,
				ResultStatus: resultStatus,
			}
		}

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
			}, resultChan, foundObjectsMap, foundCount)
		})
	}

	return subg.Wait()
}
