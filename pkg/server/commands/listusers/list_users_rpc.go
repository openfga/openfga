package listusers

import (
	"context"
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
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

	foundUsersCh := make(chan *openfgav1.User, 1)
	expandErrCh := make(chan error, 1)

	foundUsers := make([]*openfgav1.User, 0)
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
	foundUsersChan chan<- *openfgav1.User,
) error {
	for _, f := range req.GetUserFilters() {
		if req.GetObject().GetType() == f.GetType() {
			foundUsersChan <- &openfgav1.User{
				User: &openfgav1.User_Object{
					Object: req.GetObject(),
				},
			}
		}
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
	return l.expandRewrite(ctx, req, relationRewrite, foundUsersChan)
}

func (l *listUsersQuery) expandRewrite(
	ctx context.Context,
	req listUsersRequest,
	rewrite *openfgav1.Userset,
	foundUsersChan chan<- *openfgav1.User,
) error {
	switch rewrite := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return l.expandDirect(ctx, req, foundUsersChan)
	case *openfgav1.Userset_ComputedUserset:
		return l.expand(ctx, &openfgav1.ListUsersRequest{
			StoreId:              req.GetStoreId(),
			AuthorizationModelId: req.GetAuthorizationModelId(),
			Object:               req.GetObject(),
			Relation:             rewrite.ComputedUserset.GetRelation(),
			UserFilters:          req.GetUserFilters(),
			ContextualTuples:     req.GetContextualTuples(),
		}, foundUsersChan)
	case *openfgav1.Userset_TupleToUserset:
		return l.expandTTU(ctx, req, rewrite, foundUsersChan)
	case *openfgav1.Userset_Intersection:
		return l.expandIntersection(ctx, req, rewrite, foundUsersChan)
	case *openfgav1.Userset_Difference:
		return l.expandExclusion(ctx, req, rewrite, foundUsersChan)
	case *openfgav1.Userset_Union:

		pool := pool.New().WithContext(ctx)
		pool.WithCancelOnError()
		pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

		children := rewrite.Union.GetChild()
		for _, childRewrite := range children {
			pool.Go(func(ctx context.Context) error {
				return l.expandRewrite(ctx, req, childRewrite, foundUsersChan)
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
	foundUsersChan chan<- *openfgav1.User,
) error {

	typesys, err := l.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	ds := storagewrappers.NewCombinedTupleReader(l.ds, req.GetContextualTuples().GetTupleKeys())
	iter, err := ds.Read(ctx, req.GetStoreId(), &openfgav1.TupleKey{
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
			for _, f := range req.GetUserFilters() {
				if f.GetType() == userObjectType {
					// we found one, time to return it!
					foundUsersChan <- &openfgav1.User{
						User: &openfgav1.User_Object{
							Object: &openfgav1.Object{
								Type: userObjectType,
								Id:   userObjectID,
							},
						},
					}
				}
			}
			continue
		}

		pool.Go(func(ctx context.Context) error {

			return l.expand(ctx, &openfgav1.ListUsersRequest{
				StoreId:              req.GetStoreId(),
				AuthorizationModelId: req.GetAuthorizationModelId(),
				Object:               &openfgav1.Object{Type: userObjectType, Id: userObjectID},
				Relation:             userRelation,
				UserFilters:          req.GetUserFilters(),
				ContextualTuples:     req.GetContextualTuples(),
			}, foundUsersChan)
		})

	}

	return pool.Wait()
}

func (l *listUsersQuery) expandIntersection(
	ctx context.Context,
	req listUsersRequest,
	rewrite *openfgav1.Userset_Intersection,
	foundUsersChan chan<- *openfgav1.User,
) error {
	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

	intersectionFoundUsersChan := make(chan *openfgav1.User, 1)

	children := rewrite.Intersection.GetChild()
	for _, childRewrite := range children {
		copyChildRewrite := childRewrite
		pool.Go(func(ctx context.Context) error {
			return l.expandRewrite(ctx, req, copyChildRewrite, intersectionFoundUsersChan)
		})
	}

	errChan := make(chan error, 1)
	defer close(errChan)
	go func() {
		err := pool.Wait()
		close(intersectionFoundUsersChan)
		errChan <- err
	}()

	foundUsersCountMap := make(map[string]uint32, 0)
	for fu := range intersectionFoundUsersChan {
		key := tuple.UserProtoToString(fu)
		foundUsersCountMap[key]++
	}

	for key, c := range foundUsersCountMap {
		// Compare the specific user's count, or the number of times
		// the user was returned for all intersection clauses.
		// If this count equals the number of clauses, the user satisfies
		// the intersection expression and can be sent on `foundUsersChan`
		if c == uint32(len(children)) {
			foundUsersChan <- tuple.StringToUserProto(key)
		}
	}

	return <-errChan
}

func (l *listUsersQuery) expandExclusion(
	ctx context.Context,
	req listUsersRequest,
	rewrite *openfgav1.Userset_Difference,
	foundUsersChan chan<- *openfgav1.User,
) error {
	baseFoundUsersCh := make(chan *openfgav1.User, 1)
	subtractFoundUsersCh := make(chan *openfgav1.User, 1)

	errChan := make(chan error, 2)
	go func() {
		errChan <- l.expandRewrite(ctx, req, rewrite.Difference.GetBase(), baseFoundUsersCh)
		close(baseFoundUsersCh)

		errChan <- l.expandRewrite(ctx, req, rewrite.Difference.GetSubtract(), subtractFoundUsersCh)
		close(subtractFoundUsersCh)
	}()

	baseFoundUsersCountMap := make(map[string]struct{}, 0)
	for fu := range baseFoundUsersCh {
		key := tuple.UserProtoToString(fu)
		baseFoundUsersCountMap[key] = struct{}{}
	}
	subtractFoundUsersCountMap := make(map[string]struct{}, len(baseFoundUsersCountMap))
	for fu := range subtractFoundUsersCh {
		key := tuple.UserProtoToString(fu)
		subtractFoundUsersCountMap[key] = struct{}{}
	}

	for key := range baseFoundUsersCountMap {
		if _, isSubtracted := subtractFoundUsersCountMap[key]; !isSubtracted {
			// Iterate over base users because at minimum they need to pass
			// but then they are further compared to the subtracted users map.
			// If users exist in both maps, they are excluded. Only users that exist
			// solely in the base map will be returned.
			foundUsersChan <- tuple.StringToUserProto(key)
		}
	}

	return <-errChan
}

func (l *listUsersQuery) expandTTU(
	ctx context.Context,
	req listUsersRequest,
	rewrite *openfgav1.Userset_TupleToUserset,
	foundUsersChan chan<- *openfgav1.User,
) error {
	tuplesetRelation := rewrite.TupleToUserset.GetTupleset().GetRelation()
	computedRelation := rewrite.TupleToUserset.GetComputedUserset().GetRelation()

	typesys, err := l.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	ds := storagewrappers.NewCombinedTupleReader(l.ds, req.GetContextualTuples().GetTupleKeys())
	iter, err := ds.Read(ctx, req.GetStoreId(), &openfgav1.TupleKey{
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
				StoreId:              req.GetStoreId(),
				AuthorizationModelId: req.GetAuthorizationModelId(),
				Object:               &openfgav1.Object{Type: userObjectType, Id: userObjectID},
				Relation:             computedRelation,
				UserFilters:          req.GetUserFilters(),
				ContextualTuples:     req.GetContextualTuples(),
			}, foundUsersChan)
		})
	}

	return pool.Wait()
}
