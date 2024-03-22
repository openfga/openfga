package listusers

import (
	"context"
	"errors"
	"fmt"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/graph"
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
	typesys, err := l.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	hasPossibleEdges, err := doesHavePossibleEdges(typesys, req)
	if err != nil {
		return nil, err
	}
	if !hasPossibleEdges {
		return &openfgav1.ListUsersResponse{
			Users: []*openfgav1.User{},
		}, nil
	}

	foundUsersCh := make(chan *openfgav1.User, 1)
	expandErrCh := make(chan error, 1)

	foundUsersUnique := make(map[tuple.UserString]struct{}, 1000)
	done := make(chan struct{}, 1)
	go func() {
		for foundObject := range foundUsersCh {
			foundUsersUnique[tuple.UserProtoToString(foundObject)] = struct{}{}
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
	foundUsers := make([]*openfgav1.User, 0, len(foundUsersUnique))
	for foundUser := range foundUsersUnique {
		foundUsers = append(foundUsers, tuple.StringToUserProto(foundUser))
	}
	return &openfgav1.ListUsersResponse{
		Users: foundUsers,
	}, nil
}

func doesHavePossibleEdges(typesys *typesystem.TypeSystem, req *openfgav1.ListUsersRequest) (bool, error) {
	g := graph.New(typesys)

	userFilters := req.GetUserFilters()
	source := typesystem.DirectRelationReference(userFilters[0].GetType(), userFilters[0].GetRelation())
	target := typesystem.DirectRelationReference(req.GetObject().GetType(), req.GetRelation())

	edges, err := g.GetPrunedRelationshipEdges(target, source)
	if err != nil {
		return false, err
	}

	return len(edges) > 0, err
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
	case *openfgav1.Userset_Union:

		pool := pool.New().WithContext(ctx)
		pool.WithCancelOnError()
		pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

		children := rewrite.Union.GetChild()
		for _, childRewrite := range children {
			childRewriteCopy := childRewrite
			pool.Go(func(ctx context.Context) error {
				return l.expandRewrite(ctx, req, childRewriteCopy, foundUsersChan)
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
					user := tuple.StringToUserProto(tuple.BuildObject(userObjectType, userObjectID))
					// we found one, time to return it!
					foundUsersChan <- user
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

	clauses := rewrite.Intersection.GetChild()
	intersectionFoundUsersChans := make([]chan *openfgav1.User, len(clauses))

	for i, rw := range clauses {
		index := i
		rewrite := rw
		intersectionFoundUsersChans[i] = make(chan *openfgav1.User, 1)
		pool.Go(func(ctx context.Context) error {
			return l.expandRewrite(ctx, req, rewrite, intersectionFoundUsersChans[index])
		})
	}

	go func() {
		err := pool.Wait()
		for i := range intersectionFoundUsersChans {
			close(intersectionFoundUsersChans[i])
		}
		if err != nil {
			fmt.Println("ERRROR:", err)
		}
	}()

	foundUsersCountMapAgg := make([]map[string]uint32, 0)

	var wg sync.WaitGroup
	wg.Add(len(clauses))
	for i := range intersectionFoundUsersChans {
		foundUsersCountMap := make(map[string]uint32, 0)
		go func(index int) {
			for fu := range intersectionFoundUsersChans[index] {
				key := tuple.UserProtoToString(fu)
				foundUsersCountMap[key]++
			}
			wg.Done()
		}(i)
		foundUsersCountMapAgg = append(foundUsersCountMapAgg, foundUsersCountMap)
	}
	wg.Wait()

	largestMap := 0
	for i := 1; i > len(foundUsersCountMapAgg); i++ {
		currentLargestLength := len(foundUsersCountMapAgg[largestMap])
		if _, currentHasWildcard := foundUsersCountMapAgg[largestMap]["user:*"]; currentHasWildcard {
			currentLargestLength--
		}

		compareLength := len(foundUsersCountMapAgg[i])
		if _, compareHasWildcard := foundUsersCountMapAgg[i]["user:*"]; compareHasWildcard {
			compareLength--
		}

		if compareLength > currentLargestLength {
			largestMap = i
		}
	}

OuterLoop:
	for key := range foundUsersCountMapAgg[largestMap] {
		_, wildcardExists := foundUsersCountMapAgg[largestMap]["user:*"]

		for i := 0; i < len(foundUsersCountMapAgg); i++ {
			if i == largestMap && !wildcardExists {
				continue
			}

			_, exists := foundUsersCountMapAgg[i][key]
			_, hasWildcard := foundUsersCountMapAgg[i]["user:*"]

			if !exists && !hasWildcard {
				continue OuterLoop
			}
		}

		// Compare the specific user's count, or the number of times
		// the user was returned for all intersection clauses.
		// If this count equals the number of clauses, the user satisfies
		// the intersection expression and can be sent on `foundUsersChan`
		foundUsersChan <- tuple.StringToUserProto(key)
	}

	return nil
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
