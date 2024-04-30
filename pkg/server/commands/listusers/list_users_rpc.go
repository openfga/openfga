package listusers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/sourcegraph/conc/pool"

	"github.com/openfga/openfga/pkg/storage/storagewrappers"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type listUsersQuery struct {
	ds                      storage.RelationshipTupleReader
	typesystemResolver      typesystem.TypesystemResolverFunc
	resolveNodeBreadthLimit uint32
}

type foundUser struct {
	user          *openfgav1.User
	excludedUsers []*openfgav1.User
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

// NewListUsersQuery is not meant to be shared.
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

// ListUsers assumes that the typesystem is in the context.
func (l *listUsersQuery) ListUsers(
	ctx context.Context,
	req *openfgav1.ListUsersRequest,
) (*openfgav1.ListUsersResponse, error) {
	l.ds = storagewrappers.NewCombinedTupleReader(l.ds, req.GetContextualTuples())
	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("typesystem missing in context")
	}

	userFilter := req.GetUserFilters()[0]
	isReflexiveUserset := userFilter.GetType() == req.GetObject().GetType() && userFilter.GetRelation() == req.GetRelation()

	if !isReflexiveUserset {
		hasPossibleEdges, err := doesHavePossibleEdges(typesys, req)
		if err != nil {
			return nil, err
		}
		if !hasPossibleEdges {
			return &openfgav1.ListUsersResponse{
				Users: []*openfgav1.User{},
			}, nil
		}
	}

	foundUsersCh := make(chan foundUser, 1)
	expandErrCh := make(chan error, 1)

	foundUsersUnique := make(map[tuple.UserString]struct{}, 1000)
	excludedUsersUnique := make(map[tuple.UserString]struct{}, 1000)
	done := make(chan struct{}, 1)
	go func() {
		for foundObject := range foundUsersCh {
			foundUsersUnique[tuple.UserProtoToString(foundObject.user)] = struct{}{}
			for _, e := range foundObject.excludedUsers {
				excludedUsersUnique[tuple.UserProtoToString(e)] = struct{}{}
			}
		}

		done <- struct{}{}
	}()

	go func() {
		defer close(foundUsersCh)
		internalRequest := fromListUsersRequest(req)
		if err := l.expand(ctx, internalRequest, foundUsersCh); err != nil {
			expandErrCh <- err
			return
		}
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

	excludedUsers := make([]*openfgav1.ObjectOrUserset, 0, len(excludedUsersUnique))
	for e := range excludedUsersUnique {
		excludedUsers = append(excludedUsers, tuple.StringToObjectOrUserset(e))
	}

	return &openfgav1.ListUsersResponse{
		Users:         foundUsers,
		ExcludedUsers: excludedUsers,
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
	req *internalListUsersRequest,
	foundUsersChan chan<- foundUser,
) error {
	if enteredCycle(req) {
		return nil
	}

	reqObjectType := req.GetObject().GetType()
	reqObjectID := req.GetObject().GetId()
	reqRelation := req.GetRelation()

	for _, userFilter := range req.GetUserFilters() {
		if reqObjectType == userFilter.GetType() && reqRelation == userFilter.GetRelation() {
			foundUsersChan <- foundUser{
				user: &openfgav1.User{
					User: &openfgav1.User_Userset{
						Userset: &openfgav1.UsersetUser{
							Type:     reqObjectType,
							Id:       reqObjectID,
							Relation: reqRelation,
						},
					},
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
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset,
	foundUsersChan chan<- foundUser,
) error {
	switch rewrite := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return l.expandDirect(ctx, req, foundUsersChan)
	case *openfgav1.Userset_ComputedUserset:
		rewrittenReq := req.clone()
		rewrittenReq.Relation = rewrite.ComputedUserset.GetRelation()
		return l.expand(ctx, rewrittenReq, foundUsersChan)
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
	req *internalListUsersRequest,
	foundUsersChan chan<- foundUser,
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
			for _, f := range req.GetUserFilters() {
				if f.GetType() == userObjectType {
					user := tuple.StringToUserProto(tuple.BuildObject(userObjectType, userObjectID))
					// we found one, time to return it!
					foundUsersChan <- foundUser{
						user: user,
					}
				}
			}
			continue
		}

		pool.Go(func(ctx context.Context) error {
			rewrittenReq := req.clone()
			rewrittenReq.Object = &openfgav1.Object{Type: userObjectType, Id: userObjectID}
			rewrittenReq.Relation = userRelation
			return l.expand(ctx, rewrittenReq, foundUsersChan)
		})
	}

	return pool.Wait()
}

func (l *listUsersQuery) expandIntersection(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_Intersection,
	foundUsersChan chan<- foundUser,
) error {
	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

	childOperands := rewrite.Intersection.GetChild()
	intersectionFoundUsersChans := make([]chan foundUser, len(childOperands))
	for i, rewrite := range childOperands {
		i := i
		rewrite := rewrite
		intersectionFoundUsersChans[i] = make(chan foundUser, 1)
		pool.Go(func(ctx context.Context) error {
			return l.expandRewrite(ctx, req, rewrite, intersectionFoundUsersChans[i])
		})
	}

	errChan := make(chan error, 1)

	go func() {
		err := pool.Wait()
		for i := range intersectionFoundUsersChans {
			close(intersectionFoundUsersChans[i])
		}
		errChan <- err
		close(errChan)
	}()

	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(childOperands))

	wildcardCount := atomic.Uint32{}
	wildcardKey := tuple.TypedPublicWildcard(req.GetUserFilters()[0].GetType())
	foundUsersCountMap := make(map[string]uint32, 0)
	for _, foundUsersChan := range intersectionFoundUsersChans {
		go func(foundUsersChan chan foundUser) {
			defer wg.Done()
			foundUsersMap := make(map[string]uint32, 0)
			for foundUser := range foundUsersChan {
				key := tuple.UserProtoToString(foundUser.user)
				foundUsersMap[key]++
			}

			_, wildcardExists := foundUsersMap[wildcardKey]
			if wildcardExists {
				wildcardCount.Add(1)
			}
			for userKey := range foundUsersMap {
				mu.Lock()
				// Increment the count for a user but decrement if a wildcard
				// also exists to prevent double counting. This ensures accurate
				// tracking for intersection criteria, avoiding inflated counts
				// when both a user and a wildcard are present.
				foundUsersCountMap[userKey]++
				if wildcardExists {
					foundUsersCountMap[userKey]--
				}
				mu.Unlock()
			}
		}(foundUsersChan)
	}
	wg.Wait()

	for key, count := range foundUsersCountMap {
		// Compare the number of times the specific user was returned for
		// all intersection operands plus the number of wildcards.
		// If this summed value equals the number of operands, the user satisfies
		// the intersection expression and can be sent on `foundUsersChan`
		if (count + wildcardCount.Load()) == uint32(len(childOperands)) {
			foundUsersChan <- foundUser{
				user: tuple.StringToUserProto(key),
			}
		}
	}

	return <-errChan
}

func (l *listUsersQuery) expandExclusion(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_Difference,
	foundUsersChan chan<- foundUser,
) error {
	baseFoundUsersCh := make(chan foundUser, 1)
	subtractFoundUsersCh := make(chan foundUser, 1)

	var errs error
	go func() {
		err := l.expandRewrite(ctx, req, rewrite.Difference.GetBase(), baseFoundUsersCh)
		if err != nil {
			errs = multierror.Append(errs, err)
		}
		close(baseFoundUsersCh)
	}()
	go func() {
		err := l.expandRewrite(ctx, req, rewrite.Difference.GetSubtract(), subtractFoundUsersCh)
		if err != nil {
			errs = multierror.Append(errs, err)
		}
		close(subtractFoundUsersCh)
	}()

	baseFoundUsersMap := make(map[string]struct{}, 0)
	for fu := range baseFoundUsersCh {
		key := tuple.UserProtoToString(fu.user)
		baseFoundUsersMap[key] = struct{}{}
	}
	subtractFoundUsersMap := make(map[string]struct{}, len(baseFoundUsersMap))
	negatedSubtractionMap := make(map[string]struct{})
	for fu := range subtractFoundUsersCh {
		for _, s := range fu.excludedUsers {
			negatedSubtractionMap[tuple.UserProtoToString(s)] = struct{}{}
		}
		key := tuple.UserProtoToString(fu.user)
		subtractFoundUsersMap[key] = struct{}{}
	}

	wildcardKey := tuple.TypedPublicWildcard(req.GetUserFilters()[0].GetType())

	_, baseWildcardExists := baseFoundUsersMap[wildcardKey]
	_, subtractWildcardExists := subtractFoundUsersMap[wildcardKey]

	if baseWildcardExists && !subtractWildcardExists {
		// In cases where there is a public-typed wildcard (ex: user:*) returned
		// as the base case of a negation operator ("but not") and there are subjects
		// that are negated, it is not correct to purely return the public typed
		// wildcard. We also need to keep track of the excluded users of this wildcard.
		subtractedUsers := make([]*openfgav1.User, 0, len(subtractFoundUsersMap))
		for key := range subtractFoundUsersMap {
			subtractedUsers = append(subtractedUsers, tuple.StringToUserProto(key))
		}
		foundUsersChan <- foundUser{
			user:          tuple.StringToUserProto(wildcardKey),
			excludedUsers: subtractedUsers,
		}
	}

	for key := range baseFoundUsersMap {
		_, isSubtracted := subtractFoundUsersMap[key]
		_, isSubtractWildcardNegated := negatedSubtractionMap[key]

		if !isSubtracted && !subtractWildcardExists || (subtractWildcardExists && isSubtractWildcardNegated) {
			// Iterate over base users because at minimum they need to pass
			// but then they are further compared to the subtracted users map.
			// If users exist in both maps, they are excluded. Only users that exist
			// solely in the base map will be returned.
			foundUsersChan <- foundUser{
				user: tuple.StringToUserProto(key),
			}
		}
	}

	return errs
}

func (l *listUsersQuery) expandTTU(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_TupleToUserset,
	foundUsersChan chan<- foundUser,
) error {
	tuplesetRelation := rewrite.TupleToUserset.GetTupleset().GetRelation()
	computedRelation := rewrite.TupleToUserset.GetComputedUserset().GetRelation()

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
			rewrittenReq := req.clone()
			rewrittenReq.Object = &openfgav1.Object{Type: userObjectType, Id: userObjectID}
			rewrittenReq.Relation = computedRelation
			return l.expand(ctx, rewrittenReq, foundUsersChan)
		})
	}

	return pool.Wait()
}

func enteredCycle(req *internalListUsersRequest) bool {
	key := fmt.Sprintf("%s#%s", tuple.ObjectKey(req.GetObject()), req.Relation)
	if _, loaded := req.visitedUsersetsMap[key]; loaded {
		return true
	}
	req.visitedUsersetsMap[key] = struct{}{}
	return false
}
