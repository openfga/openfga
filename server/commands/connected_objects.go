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
	User             *openfgapb.ObjectRelation
	ContextualTuples []*openfgapb.TupleKey
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

	targetUserType, _ := tuple.SplitObject(req.User.GetObject())
	targetUserRef := typesystem.DirectRelationReference(targetUserType, req.User.GetRelation())
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
					User: &openfgapb.ObjectRelation{
						Object:   req.User.Object,
						Relation: innerLoopIngress.Ingress.GetRelation(),
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
	targetUserRef    *openfgapb.ObjectRelation
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

		targetUserStr := req.targetUserRef.GetObject()

		userObj, _ := tuple.SplitObjectRelation(t.GetUser())
		if userObj == targetUserStr || userObj == tuple.Wildcard {
			tuples = append(tuples, &openfgapb.Tuple{Key: t})
		}
	}
	iter1 := storage.NewStaticTupleIterator(tuples)

	iter2, err := c.Datastore.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: req.ingress.Ingress.GetType(),
		Relation:   tuplesetRelation,
		UserFilter: []*openfgapb.ObjectRelation{
			{Object: req.targetUserRef.Object},
			{Object: tuple.Wildcard},
		},
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
		foundObjectType, _ := tuple.SplitObject(foundObject)

		userObj, _ := tuple.SplitObjectRelation(tk.GetUser())

		if userObj == tuple.Wildcard {

			return serverErrors.InvalidTuple(
				fmt.Sprintf("unexpected wildcard evaluated on relation '%s#%s'", foundObjectType, tuplesetRelation),
				tuple.NewTupleKey(foundObject, tuplesetRelation, tuple.Wildcard),
			)
		}

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

		subg.Go(func() error {
			return c.streamedConnectedObjects(subgctx, &ConnectedObjectsRequest{
				StoreID:          store,
				ObjectType:       sourceObjectType,
				Relation:         sourceObjectRel,
				User:             &openfgapb.ObjectRelation{Object: foundObject, Relation: req.targetUserRef.Relation},
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

		targetUserStr := req.targetUserRef.GetObject()
		if req.targetUserRef.GetRelation() != "" {
			targetUserStr = fmt.Sprintf("%s#%s", targetUserStr, req.targetUserRef.GetRelation())
		}

		if t.GetUser() == targetUserStr || t.GetUser() == tuple.Wildcard {
			tuples = append(tuples, &openfgapb.Tuple{Key: t})
		}
	}
	iter1 := storage.NewStaticTupleIterator(tuples)

	iter2, err := c.Datastore.ReadStartingWithUser(ctx, store, storage.ReadStartingWithUserFilter{
		ObjectType: ingress.GetType(),
		Relation:   ingress.GetRelation(),
		UserFilter: []*openfgapb.ObjectRelation{
			req.targetUserRef,
			{Object: tuple.Wildcard},
		},
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
		foundObjectType, _ := tuple.SplitObject(foundObject)

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

		user := &openfgapb.ObjectRelation{Object: foundObject}
		if tk.GetRelation() != "" {
			user.Relation = tk.GetRelation()
		}

		subg.Go(func() error {
			return c.streamedConnectedObjects(subgctx, &ConnectedObjectsRequest{
				StoreID:          store,
				ObjectType:       sourceObjectType,
				Relation:         sourceObjectRel,
				User:             user,
				ContextualTuples: req.contextualTuples,
			}, resultChan, foundObjectsMap, foundCount)
		})
	}

	return subg.Wait()
}
