package listusers

import (
	"context"
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/sourcegraph/conc/pool"
)

type listUsersQuery struct {
	ds                      storage.RelationshipTupleReader
	typesystemResolver      typesystem.TypesystemResolverFunc
	resolveNodeBreadthLimit uint32
}

/*
 - Optimize entrypoint pruning
 - Intersection, exclusion, etc. (see: listobjects)
 - Max results
 - BCTR
 - Contextual tuples
 -
*/

type ListUsersQueryOption func(l *listUsersQuery)

func NewListUsersQuery(ds storage.RelationshipTupleReader, opts ...ListUsersQueryOption) *listUsersQuery {

	l := &listUsersQuery{
		ds: ds,
		typesystemResolver: func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			typesys, exists := typesystem.TypesystemFromContext(ctx)
			if !exists {
				return nil, fmt.Errorf("typesystem not provided in context")
			}

			return typesys, nil
		},
		resolveNodeBreadthLimit: 20,
	}

	for _, opt := range opts {
		opt(l)
	}

	return l
}

func (l *listUsersQuery) ListUsers(
	ctx context.Context,
	req *openfgav1.ListUsersRequest,
) (*openfgav1.ListUsersResponse, error) {

	foundUsersCh := make(chan *openfgav1.Object, 1)
	expandErrCh := make(chan error, 1)

	var foundUsers []*openfgav1.Object
	done := make(chan struct{}, 1)
	go func() {
		for foundObject := range foundUsersCh {
			foundUsers = append(foundUsers, foundObject)
		}

		done <- struct{}{}
	}()

	go func() {
		if err := l.expand(ctx, req, foundUsersCh); err != nil {
			expandErrCh <- err
			return
		}

		close(foundUsersCh)
	}()

	select {
	case err := <-expandErrCh:
		return nil, err
	case <-done:
		break
	}

	return &openfgav1.ListUsersResponse{
		Users: foundUsers,
	}, nil
}

// func (l *listUsersQuery) StreamedListUsers(
// 	ctx context.Context,
// 	req *openfgav1.StreamedListUsersRequest,
// 	srv openfgav1.OpenFGAService_StreamedListUsersServer,
// ) error {
// 	foundObjectsCh := make(chan *openfgav1.Object, 1)
// 	expandErrCh := make(chan error, 1)

// 	done := make(chan struct{}, 1)
// 	go func() {
// 		for foundObject := range foundObjectsCh {
// 			log.Printf("foundObject '%v'\n", foundObject)
// 			if err := srv.Send(&openfgav1.StreamedListUsersResponse{
// 				UserObject: foundObject,
// 			}); err != nil {
// 				// handle error
// 			}
// 		}

// 		done <- struct{}{}
// 		log.Printf("ListUsers expand is done\n")
// 	}()

// 	go func() {
// 		if err := l.expand(ctx, req, foundObjectsCh); err != nil {
// 			expandErrCh <- err
// 			return
// 		}

// 		close(foundObjectsCh)
// 		log.Printf("foundObjectsCh is closed\n")
// 	}()

// 	select {
// 	case err := <-expandErrCh:
// 		return err
// 	case <-done:
// 		break
// 	}

// 	return nil
// }

func (l *listUsersQuery) expand(
	ctx context.Context,
	req listUsersRequest,
	foundObjectsChan chan<- *openfgav1.Object,
) error {

	if req.GetObject().GetType() == req.GetTargetUserObjectTypes()[0] {
		foundObjectsChan <- req.GetObject()
	}

	typesys, err := l.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	targetObjectType := req.GetObject().GetType()
	targetRelation := req.GetRelation()

	relation, err := typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		return err
	}

	relationRewrite := relation.GetRewrite()
	return l.expandRewrite(ctx, req, relationRewrite, foundObjectsChan)
}

func (l *listUsersQuery) expandRewrite(
	ctx context.Context,
	req listUsersRequest,
	rewrite *openfgav1.Userset,
	foundObjectsChan chan<- *openfgav1.Object,
) error {
	switch rewrite := rewrite.Userset.(type) {
	case *openfgav1.Userset_This:
		return l.expandDirect(ctx, req, foundObjectsChan)
	case *openfgav1.Userset_ComputedUserset:
		return l.expand(ctx, &openfgav1.ListUsersRequest{
			StoreId:               req.GetStoreId(),
			AuthorizationModelId:  req.GetAuthorizationModelId(),
			Object:                req.GetObject(),
			Relation:              rewrite.ComputedUserset.GetRelation(),
			TargetUserObjectTypes: req.GetTargetUserObjectTypes(),
			ContextualTuples:      req.GetContextualTuples(),
		}, foundObjectsChan)
	case *openfgav1.Userset_TupleToUserset:
		return l.expandTTU(ctx, req, rewrite, foundObjectsChan)
	case *openfgav1.Userset_Union:

		pool := pool.New().WithContext(ctx)
		pool.WithCancelOnError()
		pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

		children := rewrite.Union.GetChild()
		for _, childRewrite := range children {
			pool.Go(func(ctx context.Context) error {
				return l.expandRewrite(ctx, req, childRewrite, foundObjectsChan)
			})
		}

		return pool.Wait()
	default:
		panic("unexpected userset rewrite encountered")
	}
}

func (l *listUsersQuery) expandDirect(
	ctx context.Context,
	req listUsersRequest,
	foundObjectsChan chan<- *openfgav1.Object,
) error {

	typesys, err := l.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	iter, err := l.ds.Read(ctx, req.GetStoreId(), &openfgav1.TupleKey{
		Object:   tuple.ObjectKey(req.GetObject()),
		Relation: req.GetRelation(),
	})
	if err != nil {
		return err
	}
	defer iter.Stop()

	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(typesys), // why filter invalid here?
	)
	defer filteredIter.Stop()

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			return err
		}

		tupleKeyUser := tupleKey.GetUser()

		userObject, userRelation := tuple.SplitObjectRelation(tupleKeyUser)

		userObjectType, userObjectID := tuple.SplitObject(userObject)

		if userRelation == "" {
			if req.GetTargetUserObjectTypes()[0] == userObjectType {
				// we found one, time to return it!
				foundObjectsChan <- &openfgav1.Object{Type: userObjectType, Id: userObjectID}
			}

			continue
		}

		pool.Go(func(ctx context.Context) error {

			return l.expand(ctx, &openfgav1.ListUsersRequest{
				StoreId:               req.GetStoreId(),
				AuthorizationModelId:  req.GetAuthorizationModelId(),
				Object:                &openfgav1.Object{Type: userObjectType, Id: userObjectID},
				Relation:              userRelation,
				TargetUserObjectTypes: req.GetTargetUserObjectTypes(),
				ContextualTuples:      req.GetContextualTuples(),
			}, foundObjectsChan)
		})

	}

	return pool.Wait()
}

func (l *listUsersQuery) expandTTU(
	ctx context.Context,
	req listUsersRequest,
	rewrite *openfgav1.Userset_TupleToUserset,
	foundObjectsChan chan<- *openfgav1.Object,
) error {
	tuplesetRelation := rewrite.TupleToUserset.GetTupleset().GetRelation()
	computedRelation := rewrite.TupleToUserset.ComputedUserset.GetRelation()

	typesys, err := l.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	iter, err := l.ds.Read(ctx, req.GetStoreId(), &openfgav1.TupleKey{
		Object:   tuple.ObjectKey(req.GetObject()),
		Relation: tuplesetRelation,
	})
	if err != nil {
		return err
	}
	defer iter.Stop()

	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(typesys),
	)
	defer filteredIter.Stop()

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			return err
		}

		userObject := tupleKey.GetUser()
		userObjectType, userObjectID := tuple.SplitObject(userObject)

		pool.Go(func(ctx context.Context) error {
			return l.expand(ctx, &openfgav1.ListUsersRequest{
				StoreId:               req.GetStoreId(),
				AuthorizationModelId:  req.GetAuthorizationModelId(),
				Object:                &openfgav1.Object{Type: userObjectType, Id: userObjectID},
				Relation:              computedRelation,
				TargetUserObjectTypes: req.GetTargetUserObjectTypes(),
			}, foundObjectsChan)
		})
	}

	return pool.Wait()
}
