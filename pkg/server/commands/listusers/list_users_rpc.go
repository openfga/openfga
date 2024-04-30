package listusers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/sourcegraph/conc/pool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	serverconfig "github.com/openfga/openfga/internal/server/config"

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
	maxResults              uint32
	maxConcurrentReads      uint32
	deadline                time.Duration
}

type ListUsersQueryOption func(l *listUsersQuery)

func WithListUsersQueryLogger(l logger.Logger) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.logger = l
	}
}

// WithListUsersMaxResults see server.WithListUsersMaxResults.
func WithListUsersMaxResults(max uint32) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.maxResults = max
	}
}

// WithListUsersDeadline see server.WithListUsersDeadline.
func WithListUsersDeadline(t time.Duration) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.deadline = t
	}
}

// WithResolveNodeLimit see server.WithResolveNodeLimit.
func WithResolveNodeLimit(limit uint32) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.resolveNodeLimit = limit
	}
}

// WithResolveNodeBreadthLimit see server.WithResolveNodeBreadthLimit.
func WithResolveNodeBreadthLimit(limit uint32) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.resolveNodeBreadthLimit = limit
	}
}

// WithListUsersMaxConcurrentReads see server.WithMaxConcurrentReadsForListUsers.
func WithListUsersMaxConcurrentReads(limit uint32) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.maxConcurrentReads = limit
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
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
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
) (*listUsersResponse, error) {
	ctx, span := tracer.Start(ctx, "ListUsers")
	defer span.End()

	cancellableCtx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()
	if l.deadline != 0 {
		cancellableCtx, cancelCtx = context.WithTimeout(cancellableCtx, l.deadline)
		defer cancelCtx()
	}
	l.ds = storagewrappers.NewCombinedTupleReader(
		storagewrappers.NewBoundedConcurrencyTupleReader(l.ds, l.maxConcurrentReads),
		req.GetContextualTuples(),
	)
	typesys, ok := typesystem.TypesystemFromContext(cancellableCtx)
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
			return &listUsersResponse{
				Users: []*openfgav1.User{},
				Metadata: listUsersResponseMetadata{
					DatastoreQueryCount: 0,
				},
			}, nil
		}
	}

	datastoreQueryCount := atomic.Uint32{}

	foundUsersCh := l.buildResultsChannel()
	expandErrCh := make(chan error, 1)

	foundUsersUnique := make(map[tuple.UserString]struct{}, 1000)
	done := make(chan struct{}, 1)
	go func() {
		for foundObject := range foundUsersCh {
			foundUsersUnique[tuple.UserProtoToString(foundObject)] = struct{}{}
			if l.maxResults > 0 {
				if uint32(len(foundUsersUnique)) >= l.maxResults {
					span.SetAttributes(attribute.Bool("max_results_found", true))
					break
				}
			}
		}

		done <- struct{}{}
	}()

	go func() {
		internalRequest := fromListUsersRequest(req, &datastoreQueryCount)
		err := l.expand(cancellableCtx, internalRequest, foundUsersCh)
		close(foundUsersCh)
		expandErrCh <- err
	}()

	select {
	case err := <-expandErrCh:
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}
	case <-cancellableCtx.Done():
		break
	}

	<-done
	cancelCtx()

	foundUsers := make([]*openfgav1.User, 0, len(foundUsersUnique))
	for foundUser := range foundUsersUnique {
		foundUsers = append(foundUsers, tuple.StringToUserProto(foundUser))
	}
	span.SetAttributes(attribute.Int("result_count", len(foundUsers)))

	return &listUsersResponse{
		Users: foundUsers,
		Metadata: listUsersResponseMetadata{
			DatastoreQueryCount: datastoreQueryCount.Load(),
		},
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
			user := &openfgav1.User{
				User: &openfgav1.User_Userset{
					Userset: &openfgav1.UsersetUser{
						Type:     reqObjectType,
						Id:       reqObjectID,
						Relation: reqRelation,
					},
				},
			}
			trySendResult(ctx, user, foundUsersChan)
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
	req.datastoreQueryCount.Add(1)

	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(typesys),
	)
	defer filteredIter.Stop()

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

	var errs error

LoopOnIterator:
	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if !errors.Is(err, storage.ErrIteratorDone) {
				errs = errors.Join(errs, err)
			}

			break LoopOnIterator
		}

		condEvalResult, err := eval.EvaluateTupleCondition(ctx, tupleKey, typesys, req.GetContext())
		if err != nil {
			errs = errors.Join(errs, err)
			break LoopOnIterator
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
					trySendResult(ctx, user, foundUsersChan)
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

	errs = errors.Join(pool.Wait(), errs)
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
			trySendResult(ctx, tuple.StringToUserProto(key), foundUsersChan)
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

	var baseError, substractError error
	go func() {
		err := l.expandRewrite(ctx, req, rewrite.Difference.GetBase(), baseFoundUsersCh)
		if err != nil {
			baseError = err
		}
		close(baseFoundUsersCh)
	}()
	go func() {
		err := l.expandRewrite(ctx, req, rewrite.Difference.GetSubtract(), subtractFoundUsersCh)
		if err != nil {
			substractError = err
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
			trySendResult(ctx, tuple.StringToUserProto(key), foundUsersChan)
		}
	}

	errs := errors.Join(baseError, substractError)
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
	req.datastoreQueryCount.Add(1)

	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(typesys),
	)
	defer filteredIter.Stop()

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithMaxGoroutines(int(l.resolveNodeBreadthLimit))

	var errs error

LoopOnIterator:
	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if !errors.Is(err, storage.ErrIteratorDone) {
				errs = errors.Join(errs, err)
			}

			break LoopOnIterator
		}

		condEvalResult, err := eval.EvaluateTupleCondition(ctx, tupleKey, typesys, req.GetContext())
		if err != nil {
			errs = errors.Join(errs, err)
			break LoopOnIterator
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

	errs = errors.Join(pool.Wait(), errs)
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

func (l *listUsersQuery) buildResultsChannel() chan *openfgav1.User {
	foundUsersCh := make(chan *openfgav1.User, serverconfig.DefaultListUsersMaxResults)
	maxResults := l.maxResults
	if maxResults > 0 {
		foundUsersCh = make(chan *openfgav1.User, maxResults)
	}
	return foundUsersCh
}

func trySendResult(ctx context.Context, user *openfgav1.User, foundUsersCh chan<- *openfgav1.User) {
	select {
	case <-ctx.Done():
		return
	case foundUsersCh <- user:
		return
	}
}
