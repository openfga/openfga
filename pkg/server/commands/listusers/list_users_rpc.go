package listusers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/sourcegraph/conc/pool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/openfga/openfga/pkg/telemetry"

	"github.com/openfga/openfga/pkg/logger"

	"github.com/openfga/openfga/pkg/storage/storagewrappers"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var tracer = otel.Tracer("openfga/pkg/server/commands/list_users")

type listUsersQuery struct {
	logger                  logger.Logger
	ds                      storage.RelationshipTupleReader
	typesystemResolver      typesystem.TypesystemResolverFunc
	resolveNodeBreadthLimit uint32
	resolveNodeLimit        uint32
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

func WithListUsersQueryLogger(l logger.Logger) ListUsersQueryOption {
	return func(rq *listUsersQuery) {
		rq.logger = l
	}
}

// NewListUsersQuery is not meant to be shared.
func NewListUsersQuery(ds storage.RelationshipTupleReader, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger: logger.NewNoopLogger(),
		ds:     ds,
		typesystemResolver: func(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
			typesys, exists := typesystem.TypesystemFromContext(ctx)
			if !exists {
				return nil, fmt.Errorf("typesystem not provided in context")
			}

			return typesys, nil
		},
		resolveNodeBreadthLimit: 20,
		resolveNodeLimit:        25,
	}

	for _, opt := range opts {
		opt(l)
	}

	return l
}

// WithResolveNodeLimit see server.WithResolveNodeLimit
func WithResolveNodeLimit(limit uint32) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.resolveNodeLimit = limit
	}
}

// ListUsers assumes that the typesystem is in the context.
func (l *listUsersQuery) ListUsers(
	ctx context.Context,
	req *openfgav1.ListUsersRequest,
) (*openfgav1.ListUsersResponse, error) {
	ctx, span := tracer.Start(ctx, "ListUsers")
	defer span.End()

	l.ds = storagewrappers.NewCombinedTupleReader(l.ds, req.GetContextualTuples().GetTupleKeys())
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
			span.SetAttributes(attribute.Bool("no_possible_edges", true))
			return &openfgav1.ListUsersResponse{
				Users: []*openfgav1.User{},
			}, nil
		}
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
		defer close(foundUsersCh)
		internalRequest := fromListUsersRequest(req)
		if err := l.expand(ctx, internalRequest, foundUsersCh); err != nil {
			expandErrCh <- err
			return
		}
	}()

	select {
	case err := <-expandErrCh:
		telemetry.TraceError(span, err)
		return nil, err
	case <-done:
		break
	}
	foundUsers := make([]*openfgav1.User, 0, len(foundUsersUnique))
	for foundUser := range foundUsersUnique {
		foundUsers = append(foundUsers, tuple.StringToUserProto(foundUser))
	}
	span.SetAttributes(attribute.Int("result_count", len(foundUsers)))
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
	req *internalListUsersRequest,
	foundUsersChan chan<- *openfgav1.User,
) error {
	ctx, span := tracer.Start(ctx, "expand")
	defer span.End()
	span.SetAttributes(attribute.Int("depth", int(req.depth)))
	if req.depth >= l.resolveNodeLimit {
		return graph.ErrResolutionDepthExceeded
	}
	req.depth++

	if enteredCycle(req) {
		span.SetAttributes(attribute.Bool("cycle_detected", true))
		return nil
	}

	reqObjectType := req.GetObject().GetType()
	reqObjectID := req.GetObject().GetId()
	reqRelation := req.GetRelation()

	for _, userFilter := range req.GetUserFilters() {
		if reqObjectType == userFilter.GetType() && reqRelation == userFilter.GetRelation() {
			foundUsersChan <- &openfgav1.User{
				User: &openfgav1.User_Userset{
					Userset: &openfgav1.UsersetUser{
						Type:     reqObjectType,
						Id:       reqObjectID,
						Relation: reqRelation,
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
	err = l.expandRewrite(ctx, req, relationRewrite, foundUsersChan)
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}
	return nil
}

func (l *listUsersQuery) expandRewrite(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset,
	foundUsersChan chan<- *openfgav1.User,
) error {
	ctx, span := tracer.Start(ctx, "expandRewrite")
	defer span.End()

	var err error
	switch rewrite := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		err = l.expandDirect(ctx, req, foundUsersChan)
	case *openfgav1.Userset_ComputedUserset:
		rewrittenReq := req.clone()
		rewrittenReq.Relation = rewrite.ComputedUserset.GetRelation()
		err = l.expand(ctx, rewrittenReq, foundUsersChan)
	case *openfgav1.Userset_TupleToUserset:
		err = l.expandTTU(ctx, req, rewrite, foundUsersChan)
	case *openfgav1.Userset_Intersection:
		err = l.expandIntersection(ctx, req, rewrite, foundUsersChan)
	case *openfgav1.Userset_Difference:
		err = l.expandExclusion(ctx, req, rewrite, foundUsersChan)
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

		err = pool.Wait()
	default:
		panic("unexpected userset rewrite encountered")
	}

	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}
	return nil
}

func (l *listUsersQuery) expandDirect(
	ctx context.Context,
	req *internalListUsersRequest,
	foundUsersChan chan<- *openfgav1.User,
) error {
	ctx, span := tracer.Start(ctx, "expandDirect")
	defer span.End()
	typesys, err := l.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	iter, err := l.ds.Read(ctx, req.GetStoreId(), &openfgav1.TupleKey{
		Object:   tuple.ObjectKey(req.GetObject()),
		Relation: req.GetRelation(),
	})
	if err != nil {
		telemetry.TraceError(span, err)
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

	var errs error
	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			return err
		}

		condEvalResult, err := eval.EvaluateTupleCondition(ctx, tupleKey, typesys, req.GetContext())
		if err != nil {
			telemetry.TraceError(span, err)
			return err
		}

		if len(condEvalResult.MissingParameters) > 0 {
			err := condition.NewEvaluationError(
				tupleKey.GetCondition().GetName(),
				fmt.Errorf("context is missing parameters '%v'", condEvalResult.MissingParameters),
			)
			telemetry.TraceError(span, err)
			errs = errors.Join(errs, err)
		}

		if !condEvalResult.ConditionMet {
			continue
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
			rewrittenReq := req.clone()
			rewrittenReq.Object = &openfgav1.Object{Type: userObjectType, Id: userObjectID}
			rewrittenReq.Relation = userRelation
			return l.expand(ctx, rewrittenReq, foundUsersChan)
		})
	}

	err = pool.Wait()
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}

	if errs != nil {
		telemetry.TraceError(span, errs)
		return errs
	}
	return nil
}

func (l *listUsersQuery) expandIntersection(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_Intersection,
	foundUsersChan chan<- *openfgav1.User,
) error {
	ctx, span := tracer.Start(ctx, "expandIntersection")
	defer span.End()
	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

	childOperands := rewrite.Intersection.GetChild()
	intersectionFoundUsersChans := make([]chan *openfgav1.User, len(childOperands))
	for i, rewrite := range childOperands {
		i := i
		rewrite := rewrite
		intersectionFoundUsersChans[i] = make(chan *openfgav1.User, 1)
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
		go func(foundUsersChan chan *openfgav1.User) {
			defer wg.Done()
			foundUsersMap := make(map[string]uint32, 0)
			for foundUser := range foundUsersChan {
				key := tuple.UserProtoToString(foundUser)
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
			foundUsersChan <- tuple.StringToUserProto(key)
		}
	}

	return <-errChan
}

func (l *listUsersQuery) expandExclusion(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_Difference,
	foundUsersChan chan<- *openfgav1.User,
) error {
	ctx, span := tracer.Start(ctx, "expandExclusion")
	defer span.End()
	baseFoundUsersCh := make(chan *openfgav1.User, 1)
	subtractFoundUsersCh := make(chan *openfgav1.User, 1)

	var errs error
	go func() {
		err := l.expandRewrite(ctx, req, rewrite.Difference.GetBase(), baseFoundUsersCh)
		if err != nil {
			errs = errors.Join(errs, err)
		}
		close(baseFoundUsersCh)
	}()
	go func() {
		err := l.expandRewrite(ctx, req, rewrite.Difference.GetSubtract(), subtractFoundUsersCh)
		if err != nil {
			errs = errors.Join(errs, err)
		}
		close(subtractFoundUsersCh)
	}()

	baseFoundUsersMap := make(map[string]struct{}, 0)
	for fu := range baseFoundUsersCh {
		key := tuple.UserProtoToString(fu)
		baseFoundUsersMap[key] = struct{}{}
	}
	subtractFoundUsersMap := make(map[string]struct{}, len(baseFoundUsersMap))
	for fu := range subtractFoundUsersCh {
		key := tuple.UserProtoToString(fu)
		subtractFoundUsersMap[key] = struct{}{}
	}

	wildcardKey := tuple.TypedPublicWildcard(req.GetUserFilters()[0].GetType())
	_, subtractWildcardExists := subtractFoundUsersMap[wildcardKey]
	for key := range baseFoundUsersMap {
		if _, isSubtracted := subtractFoundUsersMap[key]; !isSubtracted && !subtractWildcardExists {
			// Iterate over base users because at minimum they need to pass
			// but then they are further compared to the subtracted users map.
			// If users exist in both maps, they are excluded. Only users that exist
			// solely in the base map will be returned.
			foundUsersChan <- tuple.StringToUserProto(key)
		}
	}

	if errs != nil {
		telemetry.TraceError(span, errs)
		return errs
	}
	return nil
}

func (l *listUsersQuery) expandTTU(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_TupleToUserset,
	foundUsersChan chan<- *openfgav1.User,
) error {
	ctx, span := tracer.Start(ctx, "expandTTU")
	defer span.End()
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
		telemetry.TraceError(span, err)
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

	var errs error
	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			return err
		}

		condEvalResult, err := eval.EvaluateTupleCondition(ctx, tupleKey, typesys, req.GetContext())
		if err != nil {
			telemetry.TraceError(span, err)
			return err
		}

		if len(condEvalResult.MissingParameters) > 0 {
			err := condition.NewEvaluationError(
				tupleKey.GetCondition().GetName(),
				fmt.Errorf("context is missing parameters '%v'", condEvalResult.MissingParameters),
			)
			telemetry.TraceError(span, err)
			errs = errors.Join(errs, err)
		}

		if !condEvalResult.ConditionMet {
			continue
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

	err = pool.Wait()
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}

	if errs != nil {
		telemetry.TraceError(span, errs)
		return errs
	}
	return nil
}

func enteredCycle(req *internalListUsersRequest) bool {
	key := fmt.Sprintf("%s#%s", tuple.ObjectKey(req.GetObject()), req.Relation)
	if _, loaded := req.visitedUsersetsMap[key]; loaded {
		return true
	}
	req.visitedUsersetsMap[key] = struct{}{}
	return false
}
