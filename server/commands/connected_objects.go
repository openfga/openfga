package commands

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	serverErrors "github.com/openfga/openfga/server/errors"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"golang.org/x/sync/errgroup"
)

type ConnectedObjectsRequest struct {
	StoreID          string
	ObjectType       string
	Relation         string
	User             isUserRef
	ContextualTuples []*openfgapb.TupleKey
}

type isUserRef interface {
	isUserRef()
	GetObjectType() string
}

type UserRefObject struct {
	Object *openfgapb.Object
}

var _ isUserRef = (*UserRefObject)(nil)

func (u *UserRefObject) isUserRef() {}

func (u *UserRefObject) GetObjectType() string {
	return u.Object.Type
}

type UserRefTypedWildcard struct {
	Type string
}

var _ isUserRef = (*UserRefTypedWildcard)(nil)

func (*UserRefTypedWildcard) isUserRef() {}

func (u *UserRefTypedWildcard) GetObjectType() string {
	return u.Type
}

type UserRefObjectRelation struct {
	ObjectRelation *openfgapb.ObjectRelation
}

func (*UserRefObjectRelation) isUserRef() {}

func (u *UserRefObjectRelation) GetObjectType() string {
	return tuple.GetType(u.ObjectRelation.Object)
}

type UserRef struct {

	// Types that are assignable to Ref
	//  *UserRef_Object
	//  *UserRef_TypedWildcard
	//  *UserRef_ObjectRelation
	Ref isUserRef
}

type ConnectedObjectsCommand struct {
	Datastore        storage.OpenFGADatastore
	Typesystem       *typesystem.TypeSystem
	ResolveNodeLimit uint32

	// Limit limits the results yielded by the ConnectedObjects API.
	Limit uint32
}

func (c *ConnectedObjectsCommand) streamedConnectedObjects(
	ctx context.Context,
	req *ConnectedObjectsRequest,
	resultChan chan<- string,
	foundObjectsMap *sync.Map,
	foundCount *uint32,
) error {

	depth, ok := graph.ResolutionDepthFromContext(ctx)
	if !ok {
		ctx = graph.ContextWithResolutionDepth(ctx, 0)
	} else {
		if depth >= c.ResolveNodeLimit {
			return serverErrors.AuthorizationModelResolutionTooComplex
		}

		ctx = graph.ContextWithResolutionDepth(ctx, depth+1)
	}

	storeID := req.StoreID

	var targetUserRef *openfgapb.RelationReference
	var targetUserType, targetUserObj string

	// e.g. 'user:bob'
	if val, ok := req.User.(*UserRefObject); ok {
		targetUserType = val.Object.GetType()
		targetUserObj = tuple.BuildObject(targetUserType, val.Object.GetId())
		targetUserRef = typesystem.DirectRelationReference(targetUserType, "")
	}

	// e.g. 'user:*'
	if val, ok := req.User.(*UserRefTypedWildcard); ok {
		targetUserType = val.Type
		targetUserRef = typesystem.WildcardRelationReference(targetUserType)
	}

	// e.g. 'group:eng#member'
	if val, ok := req.User.(*UserRefObjectRelation); ok {
		targetUserType = tuple.GetType(val.ObjectRelation.GetObject())
		targetUserObj = val.ObjectRelation.Object
		targetUserRef = typesystem.DirectRelationReference(targetUserType, val.ObjectRelation.GetRelation())
	}

	sourceObjRef := typesystem.DirectRelationReference(req.ObjectType, req.Relation)

	// build the graph of possible edges between object types in the graph based on the authz model's type info
	g := graph.BuildConnectedObjectGraph(c.Typesystem)

	// find the possible incoming edges (ingresses) between the target user reference and the source (object, relation) reference
	ingresses, err := g.RelationshipIngresses(sourceObjRef, targetUserRef)
	if err != nil {
		return err
	}

	subg, subgctx := errgroup.WithContext(ctx)
	subg.SetLimit(maximumConcurrentChecks)

	for _, ingress := range ingresses {
		innerLoopIngress := ingress
		subg.Go(func() error {
			r := &reverseExpandRequest{
				storeID:          storeID,
				ingress:          innerLoopIngress,
				sourceObjectRef:  sourceObjRef,
				targetUserRef:    req.User,
				contextualTuples: req.ContextualTuples,
			}

			switch innerLoopIngress.Type {
			case graph.DirectIngress:
				return c.reverseExpandDirect(subgctx, r, resultChan, foundObjectsMap, foundCount)
			case graph.ComputedUsersetIngress:

				// lookup the rewritten target relation on the computed_userset ingress
				return c.streamedConnectedObjects(ctx, &ConnectedObjectsRequest{
					StoreID:    storeID,
					ObjectType: req.ObjectType,
					Relation:   req.Relation,
					User: &UserRefObjectRelation{
						ObjectRelation: &openfgapb.ObjectRelation{
							Object:   targetUserObj,
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

// StreamedConnectedObjects yields all of the objects of the provided objectType that
// the given user has a specific relation with. The results will be limited by the request
// limit. If a 0 limit is provided then all objects of the provided objectType will be
// returned.
func (c *ConnectedObjectsCommand) StreamedConnectedObjects(
	ctx context.Context,
	req *ConnectedObjectsRequest,
	resultChan chan<- string, // object string (e.g. document:1)
) error {

	var foundCount *uint32
	if c.Limit > 0 {
		foundCount = new(uint32)
	}

	var foundObjects sync.Map
	return c.streamedConnectedObjects(ctx, req, resultChan, &foundObjects, foundCount)
}

type reverseExpandRequest struct {
	storeID          string
	ingress          *graph.RelationshipIngress
	sourceObjectRef  *openfgapb.RelationReference
	targetUserRef    isUserRef
	contextualTuples []*openfgapb.TupleKey
}

func (c *ConnectedObjectsCommand) reverseExpandTupleToUserset(
	ctx context.Context,
	req *reverseExpandRequest,
	resultChan chan<- string,
	foundObjectsMap *sync.Map,
	foundCount *uint32,
) error {

	store := req.storeID

	ingress := req.ingress.Ingress

	sourceObjectType := req.sourceObjectRef.GetType()
	sourceObjectRel := req.sourceObjectRef.GetRelation()

	tuplesetRelation := req.ingress.TuplesetRelation.GetRelation()

	var tuples []*openfgapb.Tuple
	for _, t := range req.contextualTuples {

		object := t.GetObject()
		objectType, _ := tuple.SplitObject(object)
		if objectType != ingress.GetType() {
			continue
		}

		if t.GetRelation() != tuplesetRelation {
			continue
		}

		user := t.GetUser()

		var targetUserStr string
		if val, ok := req.targetUserRef.(*UserRefTypedWildcard); ok {
			targetUserStr = fmt.Sprintf("%s:*", val.Type)
		}

		if val, ok := req.targetUserRef.(*UserRefObjectRelation); ok {
			targetUserStr = val.ObjectRelation.Object
		}

		if val, ok := req.targetUserRef.(*UserRefObject); ok {
			targetUserStr = fmt.Sprintf("%s:%s", val.Object.Type, val.Object.Id)
		}

		if tuple.IsTypedWildcard(user) && tuple.GetType(user) == req.targetUserRef.GetObjectType() {
			tuples = append(tuples, &openfgapb.Tuple{Key: t})
			continue
		}

		if t.GetUser() == targetUserStr {
			tuples = append(tuples, &openfgapb.Tuple{Key: t})
		}
	}
	iter1 := storage.NewStaticTupleIterator(tuples)

	var userFilter []*openfgapb.ObjectRelation

	// e.g. 'user:bob'
	if val, ok := req.targetUserRef.(*UserRefObject); ok {
		userFilter = append(userFilter, &openfgapb.ObjectRelation{
			Object: tuple.BuildObject(val.Object.Type, val.Object.Id),
		})
	}

	// e.g. 'group:eng#member'
	if val, ok := req.targetUserRef.(*UserRefObjectRelation); ok {
		userFilter = append(userFilter, &openfgapb.ObjectRelation{
			Object: val.ObjectRelation.Object,
		})
	}

	iter2, err := c.Datastore.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: req.ingress.Ingress.GetType(),
		Relation:   tuplesetRelation,
		UserFilter: userFilter,
	})
	if err != nil {
		iter1.Stop()
		return err
	}

	iter := storage.NewCombinedIterator(iter1, iter2)
	defer iter.Stop()

	subg, subgctx := errgroup.WithContext(ctx)
	subg.SetLimit(maximumConcurrentChecks)

	for {
		t, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			return err
		}

		tk := t.GetKey()

		foundObject := tk.GetObject()
		foundObjectType, foundObjectID := tuple.SplitObject(foundObject)

		if _, ok := foundObjectsMap.Load(foundObject); ok {
			// todo(jon-whit): we could optimize this by avoiding reading this
			// from the database in the first place

			// if we've already evaluated/found the object, then continue
			continue
		}

		if foundObjectType == sourceObjectType {
			if foundCount != nil && atomic.AddUint32(foundCount, 1) > c.Limit {
				break
			}

			resultChan <- foundObject
			foundObjectsMap.Store(foundObject, struct{}{})
		}

		var targetUserRef isUserRef
		targetUserRef = &UserRefObject{
			Object: &openfgapb.Object{
				Type: foundObjectType,
				Id:   foundObjectID,
			},
		}

		if _, ok := req.targetUserRef.(*UserRefTypedWildcard); ok {
			targetUserRef = &UserRefTypedWildcard{Type: foundObjectType}
		}

		if _, ok := req.targetUserRef.(*UserRefObjectRelation); ok {
			targetUserRef = &UserRefObjectRelation{
				ObjectRelation: &openfgapb.ObjectRelation{
					Object:   foundObject,
					Relation: ingress.GetRelation(),
				},
			}
		}

		subg.Go(func() error {
			return c.streamedConnectedObjects(subgctx, &ConnectedObjectsRequest{
				StoreID:          store,
				ObjectType:       sourceObjectType,
				Relation:         sourceObjectRel,
				User:             targetUserRef,
				ContextualTuples: req.contextualTuples,
			}, resultChan, foundObjectsMap, foundCount)
		})
	}

	return subg.Wait()
}

func (c *ConnectedObjectsCommand) reverseExpandDirect(
	ctx context.Context,
	req *reverseExpandRequest,
	resultChan chan<- string,
	foundObjectsMap *sync.Map,
	foundCount *uint32,
) error {

	store := req.storeID

	ingress := req.ingress.Ingress

	sourceObjectType := req.sourceObjectRef.GetType()
	sourceObjectRel := req.sourceObjectRef.GetRelation()

	var tuples []*openfgapb.Tuple
	for _, t := range req.contextualTuples {

		object := t.GetObject()
		objectType, _ := tuple.SplitObject(object)
		if objectType != ingress.GetType() {
			continue
		}

		if t.GetRelation() != ingress.GetRelation() {
			continue
		}

		user := t.GetUser()

		var targetUserStr string
		if val, ok := req.targetUserRef.(*UserRefTypedWildcard); ok {
			targetUserStr = fmt.Sprintf("%s:*", val.Type)
		}

		if val, ok := req.targetUserRef.(*UserRefObjectRelation); ok {
			targetUserStr = fmt.Sprintf("%s#%s", val.ObjectRelation.Object, val.ObjectRelation.Relation)
		}

		if val, ok := req.targetUserRef.(*UserRefObject); ok {
			targetUserStr = fmt.Sprintf("%s:%s", val.Object.Type, val.Object.Id)
		}

		if tuple.IsTypedWildcard(user) && tuple.GetType(user) == req.targetUserRef.GetObjectType() {
			tuples = append(tuples, &openfgapb.Tuple{Key: t})
			continue
		}

		if t.GetUser() == targetUserStr {
			tuples = append(tuples, &openfgapb.Tuple{Key: t})
		}
	}
	iter1 := storage.NewStaticTupleIterator(tuples)

	var userFilter []*openfgapb.ObjectRelation

	targetUserObjectType := req.targetUserRef.GetObjectType()

	publiclyAssignable, err := c.Typesystem.IsPubliclyAssignable(ingress, targetUserObjectType)
	if err != nil {
		return err
	}

	if publiclyAssignable {
		userFilter = append(userFilter, &openfgapb.ObjectRelation{
			Object: fmt.Sprintf("%s:*", targetUserObjectType),
		})
	}

	targetRelationRef := &openfgapb.RelationReference{
		Type: req.targetUserRef.GetObjectType(),
	}

	// e.g. 'user:bob'
	if val, ok := req.targetUserRef.(*UserRefObject); ok {
		userFilter = append(userFilter, &openfgapb.ObjectRelation{
			Object: tuple.BuildObject(val.Object.Type, val.Object.Id),
		})
	}

	// e.g. 'user:*'
	if val, ok := req.targetUserRef.(*UserRefTypedWildcard); ok {
		targetRelationRef.RelationOrWildcard = &openfgapb.RelationReference_Wildcard{}

		userFilter = append(userFilter, &openfgapb.ObjectRelation{
			Object: tuple.BuildObject(val.Type, "*"),
		})
	}

	// e.g. 'group:eng#member'
	if val, ok := req.targetUserRef.(*UserRefObjectRelation); ok {
		targetRelationRef.RelationOrWildcard = &openfgapb.RelationReference_Relation{
			Relation: val.ObjectRelation.Relation,
		}

		userFilter = append(userFilter, val.ObjectRelation)
	}

	iter2, err := c.Datastore.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: ingress.GetType(),
		Relation:   ingress.GetRelation(),
		UserFilter: userFilter,
	})
	if err != nil {
		iter1.Stop()
		return err
	}

	iter := storage.NewCombinedIterator(iter1, iter2)
	defer iter.Stop()

	subg, subgctx := errgroup.WithContext(ctx)
	subg.SetLimit(maximumConcurrentChecks)

	for {
		t, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			return err
		}

		tk := t.GetKey()

		foundObject := tk.GetObject()
		foundObjectType, foundObjectID := tuple.SplitObject(foundObject)

		if _, ok := foundObjectsMap.Load(foundObject); ok {
			// todo(jon-whit): we could optimize this by avoiding reading this
			// from the database in the first place

			// if we've already evaluated/found the object, then continue
			continue
		}

		if foundObjectType == sourceObjectType {
			if foundCount != nil && atomic.AddUint32(foundCount, 1) > c.Limit {
				break
			}

			resultChan <- foundObject
			foundObjectsMap.Store(foundObject, struct{}{})
		}

		var targetUserRef isUserRef
		targetUserRef = &UserRefObject{
			Object: &openfgapb.Object{
				Type: foundObjectType,
				Id:   foundObjectID,
			},
		}

		if tuple.IsTypedWildcard(foundObject) {
			targetUserRef = &UserRefTypedWildcard{Type: foundObjectType}
		}

		if tk.GetRelation() != "" {
			targetUserRef = &UserRefObjectRelation{
				ObjectRelation: &openfgapb.ObjectRelation{
					Object:   foundObject,
					Relation: tk.GetRelation(),
				},
			}
		}

		subg.Go(func() error {
			return c.streamedConnectedObjects(subgctx, &ConnectedObjectsRequest{
				StoreID:          store,
				ObjectType:       sourceObjectType,
				Relation:         sourceObjectRel,
				User:             targetUserRef,
				ContextualTuples: req.contextualTuples,
			}, resultChan, foundObjectsMap, foundCount)
		})
	}

	return subg.Wait()
}
