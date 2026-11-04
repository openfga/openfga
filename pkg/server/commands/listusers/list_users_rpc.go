package listusers

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sourcegraph/conc/panics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	openfgaErrors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/telemetry"
	"github.com/openfga/openfga/internal/throttler/threshold"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var (
	tracer   = otel.Tracer("openfga/pkg/server/commands/list_users")
	ErrPanic = errors.New("panic captured")
)

type listUsersQuery struct {
	logger                     logger.Logger
	datastore                  *storagewrappers.RequestStorageWrapper
	resolveNodeBreadthLimit    uint32
	resolveNodeLimit           uint32
	maxResults                 uint32
	maxConcurrentReads         uint32
	deadline                   time.Duration
	dispatchThrottlerConfig    threshold.Config
	wasDispatchThrottled       *atomic.Bool
	wasDatastoreThrottled      *atomic.Bool
	expandDirectDispatch       expandDirectDispatchHandler
	datastoreThrottlingEnabled bool
	datastoreThrottleThreshold int
	datastoreThrottleDuration  time.Duration
}

type expandResponse struct {
	hasCycle bool
	err      error
}

// userRelationshipStatus represents the status of a relationship that a given user/subject has with respect to a specific relation.
//
// A user/subject either does or does not have a relationship, which represents that
// they either explicitly do have a relationship or explicitly do not.
type userRelationshipStatus int

type expandDirectDispatchHandler func(ctx context.Context, listUsersQuery *listUsersQuery, req *internalListUsersRequest, userObjectType string, userObjectID string, userRelation string, resp expandResponse, foundUsersChan chan<- foundUser, hasCycle *atomic.Bool) expandResponse

const (
	HasRelationship userRelationshipStatus = iota
	NoRelationship
)

type foundUser struct {
	user          *openfgav1.User
	excludedUsers []*openfgav1.User

	// relationshipStatus indicates whether the user explicitly does or does not have
	// a specific relationship with respect to the relation being expanded. It almost
	// exclusively applies to behavior stemming from exclusion rewrite rules.
	//
	// As users/subjects are being expanded we propagate the relationship status with
	// respect to the relation being evaluated so that we can handle subjects which
	// have been explicitly excluded from a relationship and where that relation is
	// contained under the subtracted branch of another exclusion. This allows us to
	// bubble up the subject from the subtracted branch of the exclusion.
	relationshipStatus userRelationshipStatus
}

type ListUsersQueryOption func(l *listUsersQuery)

func WithListUsersQueryLogger(l logger.Logger) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.logger = l
	}
}

// WithListUsersMaxResults see server.WithListUsersMaxResults.
func WithListUsersMaxResults(maxResults uint32) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.maxResults = maxResults
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

func WithListUsersDatastoreThrottler(enabled bool, threshold int, duration time.Duration) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.datastoreThrottlingEnabled = enabled
		d.datastoreThrottleThreshold = threshold
		d.datastoreThrottleDuration = duration
	}
}

func (l *listUsersQuery) throttle(ctx context.Context, currentNumDispatch uint32) {
	span := trace.SpanFromContext(ctx)

	shouldThrottle := threshold.ShouldThrottle(
		ctx,
		currentNumDispatch,
		l.dispatchThrottlerConfig.Threshold,
		l.dispatchThrottlerConfig.MaxThreshold,
	)

	span.SetAttributes(
		attribute.Int("dispatch_count", int(currentNumDispatch)),
		attribute.Bool("is_throttled", shouldThrottle))

	if shouldThrottle {
		l.wasDispatchThrottled.Store(true)
		l.dispatchThrottlerConfig.Throttler.Throttle(ctx)
	}
}

func WithDispatchThrottlerConfig(config threshold.Config) ListUsersQueryOption {
	return func(d *listUsersQuery) {
		d.dispatchThrottlerConfig = config
	}
}

// TODO accept ListUsersRequest instead of contextualTuples.
func NewListUsersQuery(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                  logger.NewNoopLogger(),
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasDispatchThrottled:    new(atomic.Bool),
		wasDatastoreThrottled:   new(atomic.Bool),
		expandDirectDispatch:    expandDirectDispatch,
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, &storagewrappers.Operation{
		Method:            apimethod.ListUsers,
		Concurrency:       l.maxConcurrentReads,
		ThrottlingEnabled: l.datastoreThrottlingEnabled,
		ThrottleThreshold: l.datastoreThrottleThreshold,
		ThrottleDuration:  l.datastoreThrottleDuration,
	})

	return l
}

// ListUsers assumes that the typesystem is in the context and that the request is valid.
func (l *listUsersQuery) ListUsers(
	ctx context.Context,
	req *openfgav1.ListUsersRequest,
) (*listUsersResponse, error) {
	ctx, span := tracer.Start(ctx, "ListUsers", trace.WithAttributes(
		attribute.String("store_id", req.GetStoreId()),
	))
	defer span.End()

	cancellableCtx, cancelCtx := context.WithCancel(ctx)
	if l.deadline != 0 {
		cancellableCtx, cancelCtx = context.WithTimeout(cancellableCtx, l.deadline)
		defer cancelCtx()
	}
	defer cancelCtx()

	typesys, ok := typesystem.TypesystemFromContext(cancellableCtx)
	if !ok {
		return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
	}

	userFilter := req.GetUserFilters()[0]
	userset := tuple.ToObjectRelationString(tuple.ObjectKey(req.GetObject()), req.GetRelation())

	if !tuple.UsersetMatchTypeAndRelation(userset, userFilter.GetRelation(), userFilter.GetType()) {
		hasPossibleEdges, err := doesHavePossibleEdges(typesys, req)
		if err != nil {
			return nil, err
		}
		if !hasPossibleEdges {
			span.SetAttributes(attribute.Bool("no_possible_edges", true))
			return &listUsersResponse{
				Users: []*openfgav1.User{},
				Metadata: listUsersResponseMetadata{
					DispatchCounter:       new(atomic.Uint32),
					WasDispatchThrottled:  new(atomic.Bool),
					WasDatastoreThrottled: new(atomic.Bool),
				},
			}, nil
		}
	}

	dispatchCount := atomic.Uint32{}

	foundUsersCh := l.buildResultsChannel()
	expandErrCh := make(chan error, 1)

	foundUsersUnique := make(map[tuple.UserString]foundUser, 1000)

	doneWithFoundUsersCh := make(chan struct{}, 1)
	go func() {
		for foundUser := range foundUsersCh {
			// A user may arrive from several sources, and has the relationship if any
			// of them grants it.
			key := tuple.UserProtoToString(foundUser.user)
			if existing, ok := foundUsersUnique[key]; !ok || existing.relationshipStatus == NoRelationship {
				foundUsersUnique[key] = foundUser
			}

			if l.maxResults > 0 {
				if uint32(len(foundUsersUnique)) >= l.maxResults {
					span.SetAttributes(attribute.Bool("max_results_found", true))
					break
				}
			}
		}

		doneWithFoundUsersCh <- struct{}{}
	}()

	go func() {
		internalRequest := fromListUsersRequest(req, &dispatchCount)
		resp := l.expand(cancellableCtx, internalRequest, foundUsersCh)
		if resp.err != nil {
			expandErrCh <- resp.err
		}
		close(foundUsersCh)
	}()

	deadlineExceeded := false

	select {
	case <-doneWithFoundUsersCh:
		break
	case <-cancellableCtx.Done():
		deadlineExceeded = true
		// to avoid a race on the 'foundUsersUnique' map below, wait for the range over the channel to close
		<-doneWithFoundUsersCh
		break
	}

	select {
	case err := <-expandErrCh:
		if (deadlineExceeded || errors.Is(err, context.DeadlineExceeded)) && isOnlyContextError(err) {
			// We skip the error because we want to send at least partial results to the user (but we should probably set response headers).
			// Any other error may have truncated an exclusion's subtract operand, so the partial results could over-grant.
			break
		}
		telemetry.TraceError(span, err)
		return nil, err
	default:
		break
	}

	cancelCtx()

	foundUsers := make([]*openfgav1.User, 0, len(foundUsersUnique))
	for foundUserKey, foundUser := range foundUsersUnique {
		if foundUser.relationshipStatus == NoRelationship {
			continue
		}

		foundUsers = append(foundUsers, tuple.StringToUserProto(foundUserKey))
	}

	span.SetAttributes(attribute.Int("result_count", len(foundUsers)))

	dsMeta := l.datastore.GetMetadata()
	l.wasDatastoreThrottled.Store(dsMeta.WasThrottled)
	return &listUsersResponse{
		Users: foundUsers,
		Metadata: listUsersResponseMetadata{
			DatastoreQueryCount:   dsMeta.DatastoreQueryCount,
			DatastoreItemCount:    dsMeta.DatastoreItemCount,
			DispatchCounter:       &dispatchCount,
			WasDispatchThrottled:  l.wasDispatchThrottled,
			WasDatastoreThrottled: l.wasDatastoreThrottled,
		},
	}, nil
}

// isOnlyContextError reports whether every error within err, which may join
// several, is a context cancellation or deadline error.
func isOnlyContextError(err error) bool {
	switch wrapped := err.(type) { //nolint:errorlint // unwraps each layer itself
	case interface{ Unwrap() []error }:
		for _, e := range wrapped.Unwrap() {
			if !isOnlyContextError(e) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return isOnlyContextError(wrapped.Unwrap())
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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

func (l *listUsersQuery) dispatch(
	ctx context.Context,
	req *internalListUsersRequest,
	foundUsersChan chan<- foundUser,
) expandResponse {
	newcount := req.dispatchCount.Add(1)
	if l.dispatchThrottlerConfig.Enabled {
		l.throttle(ctx, newcount)
	}

	return l.expand(ctx, req, foundUsersChan)
}

func (l *listUsersQuery) expand(
	ctx context.Context,
	req *internalListUsersRequest,
	foundUsersChan chan<- foundUser,
) expandResponse {
	ctx, span := tracer.Start(ctx, "expand")
	defer span.End()
	span.SetAttributes(attribute.Int("depth", int(req.depth)))
	if req.depth >= l.resolveNodeLimit {
		return expandResponse{
			err: graph.ErrResolutionDepthExceeded,
		}
	}
	req.depth++

	if enteredCycle(req) {
		span.SetAttributes(attribute.Bool("cycle_detected", true))
		return expandResponse{
			hasCycle: true,
		}
	}

	reqObjectType := req.GetObject().GetType()
	reqObjectID := req.GetObject().GetId()
	reqRelation := req.GetRelation()

	for _, userFilter := range req.GetUserFilters() {
		if reqObjectType == userFilter.GetType() && reqRelation == userFilter.GetRelation() {
			concurrency.TrySendThroughChannel(ctx, foundUser{
				user: &openfgav1.User{
					User: &openfgav1.User_Userset{
						Userset: &openfgav1.UsersetUser{
							Type:     reqObjectType,
							Id:       reqObjectID,
							Relation: reqRelation,
						},
					},
				},
			}, foundUsersChan)
		}
	}

	typesys, _ := typesystem.TypesystemFromContext(ctx)

	targetObjectType := req.GetObject().GetType()
	targetRelation := req.GetRelation()

	relation, err := typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		if _, ok := errors.AsType[*typesystem.RelationUndefinedError](err); ok {
			return expandResponse{}
		}
		return expandResponse{
			err: err,
		}
	}

	relationRewrite := relation.GetRewrite()
	resp := l.expandRewrite(ctx, req, relationRewrite, foundUsersChan)
	if resp.err != nil {
		telemetry.TraceError(span, resp.err)
	}
	return resp
}

func (l *listUsersQuery) expandRewrite(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset,
	foundUsersChan chan<- foundUser,
) expandResponse {
	ctx, span := tracer.Start(ctx, "expandRewrite")
	defer span.End()

	var resp expandResponse
	switch rewrite := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		resp = l.expandDirect(ctx, req, foundUsersChan)
	case *openfgav1.Userset_ComputedUserset:
		rewrittenReq := req.clone()
		rewrittenReq.Relation = rewrite.ComputedUserset.GetRelation()
		resp = l.dispatch(ctx, rewrittenReq, foundUsersChan)
	case *openfgav1.Userset_TupleToUserset:
		resp = l.expandTTU(ctx, req, rewrite, foundUsersChan)
	case *openfgav1.Userset_Intersection:
		resp = l.expandIntersection(ctx, req, rewrite, foundUsersChan)
	case *openfgav1.Userset_Difference:
		resp = l.expandExclusion(ctx, req, rewrite, foundUsersChan)
	case *openfgav1.Userset_Union:
		resp = l.expandUnion(ctx, req, rewrite, foundUsersChan)
	default:
		panic("unexpected userset rewrite encountered")
	}

	if resp.err != nil {
		telemetry.TraceError(span, resp.err)
	}
	return resp
}

func (l *listUsersQuery) expandDirect(
	ctx context.Context,
	req *internalListUsersRequest,
	foundUsersChan chan<- foundUser,
) expandResponse {
	ctx, span := tracer.Start(ctx, "expandDirect")
	defer span.End()
	typesys, _ := typesystem.TypesystemFromContext(ctx)

	opts := storage.ReadOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}
	iter, err := l.datastore.Read(ctx, req.GetStoreId(), storage.ReadFilter{
		Object:   tuple.ObjectKey(req.GetObject()),
		Relation: req.GetRelation(),
	}, opts)
	if err != nil {
		telemetry.TraceError(span, err)
		// Once the context is done, report its error instead: the cancellation may
		// have caused this one, and the request then returns partial results.
		return expandResponse{
			err: cmp.Or(ctx.Err(), err),
		}
	}
	defer iter.Stop()

	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(typesys),
	)
	defer filteredIter.Stop()

	pool := concurrency.NewPool(ctx, int(l.resolveNodeBreadthLimit))

	var errs error
	var hasCycle atomic.Bool
LoopOnIterator:
	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if !errors.Is(err, storage.ErrIteratorDone) {
				errs = errors.Join(errs, cmp.Or(ctx.Err(), err))
			}

			break LoopOnIterator
		}

		var condMet bool
		if condition.IsInlineExpression(tupleKey.GetCondition().GetName()) {
			condMet, err = eval.EvaluateInlineExpression(ctx, tupleKey, req.Context)
		} else {
			cond, _ := typesys.GetCondition(tupleKey.GetCondition().GetName())
			condMet, err = eval.EvaluateTupleCondition(ctx, tupleKey, cond, req.Context)
		}
		if err != nil {
			errs = errors.Join(errs, cmp.Or(ctx.Err(), err))
			if !errors.Is(err, condition.ErrEvaluationFailed) {
				break LoopOnIterator
			}
			telemetry.TraceError(span, err)
		}

		if !condMet {
			continue
		}

		tupleKeyUser := tupleKey.GetUser()
		userObject, userRelation := tuple.SplitObjectRelation(tupleKeyUser)
		userObjectType, userObjectID := tuple.SplitObject(userObject)

		if userRelation == "" {
			for _, f := range req.GetUserFilters() {
				if f.GetType() == userObjectType {
					user := tuple.StringToUserProto(tuple.BuildObject(userObjectType, userObjectID))

					concurrency.TrySendThroughChannel(ctx, foundUser{
						user: user,
					}, foundUsersChan)
				}
			}
			continue
		}

		pool.Go(func(ctx context.Context) error {
			var resp expandResponse
			recoveredError := panics.Try(func() {
				resp = l.expandDirectDispatch(ctx, l, req, userObjectType, userObjectID, userRelation, resp, foundUsersChan, &hasCycle)
			})
			if recoveredError != nil {
				resp = panicExpanseResponse(recoveredError)
			}
			return resp.err
		})
	}

	errs = errors.Join(errs, pool.Wait())
	if errs != nil {
		telemetry.TraceError(span, errs)
	}
	return expandResponse{
		err:      errs,
		hasCycle: hasCycle.Load(),
	}
}

func expandDirectDispatch(ctx context.Context, l *listUsersQuery, req *internalListUsersRequest, userObjectType string, userObjectID string, userRelation string, resp expandResponse, foundUsersChan chan<- foundUser, hasCycle *atomic.Bool) expandResponse {
	rewrittenReq := req.clone()
	rewrittenReq.Object = &openfgav1.Object{Type: userObjectType, Id: userObjectID}
	rewrittenReq.Relation = userRelation
	resp = l.dispatch(ctx, rewrittenReq, foundUsersChan)
	if resp.hasCycle {
		hasCycle.Store(true)
	}
	return resp
}

func (l *listUsersQuery) expandIntersection(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_Intersection,
	foundUsersChan chan<- foundUser,
) expandResponse {
	ctx, span := tracer.Start(ctx, "expandIntersection")
	defer span.End()
	pool := concurrency.NewPool(ctx, int(l.resolveNodeBreadthLimit))

	childOperands := rewrite.Intersection.GetChild()
	intersectionFoundUsersChans := make([]chan foundUser, len(childOperands))
	for i := range childOperands {
		intersectionFoundUsersChans[i] = make(chan foundUser, 1)
	}

	var wg sync.WaitGroup
	wg.Add(len(childOperands))

	wildcardKey := tuple.TypedPublicWildcard(req.GetUserFilters()[0].GetType())
	// Per operand, the users it grants and the users excluded from each wildcard it
	// found. Several sources may feed one operand, so an exclusion from one source
	// must not cancel a grant from another.
	operandFoundUsers := make([]map[string]struct{}, len(childOperands))
	operandWildcardExclusions := make([][]map[string]struct{}, len(childOperands))
	// Consumers must start before the producer pool below: a producer blocks on
	// its second send into a size-1 channel until drained, so if the bounded
	// pool fills before consumers run, the submit loop deadlocks.
	for i, foundUsersChan := range intersectionFoundUsersChans {
		operandFoundUsers[i] = make(map[string]struct{})
		go func(i int, foundUsersChan chan foundUser) {
			defer wg.Done()
			for foundUser := range foundUsersChan {
				if foundUser.relationshipStatus == NoRelationship {
					continue
				}
				key := tuple.UserProtoToString(foundUser.user)
				operandFoundUsers[i][key] = struct{}{}
				if key == wildcardKey {
					excluded := make(map[string]struct{}, len(foundUser.excludedUsers))
					for _, excludedUser := range foundUser.excludedUsers {
						excluded[tuple.UserProtoToString(excludedUser)] = struct{}{}
					}
					operandWildcardExclusions[i] = append(operandWildcardExclusions[i], excluded)
				}
			}
		}(i, foundUsersChan)
	}

	for i, rewrite := range childOperands {
		pool.Go(func(ctx context.Context) error {
			resp := l.expandRewrite(ctx, req, rewrite, intersectionFoundUsersChans[i])
			return resp.err
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

	wg.Wait()

	// An operand grants a user directly or through a wildcard that does not exclude them.
	operandGrants := func(i int, key string) bool {
		if _, ok := operandFoundUsers[i][key]; ok {
			return true
		}
		for _, excluded := range operandWildcardExclusions[i] {
			if _, ok := excluded[key]; !ok {
				return true
			}
		}
		return false
	}
	intersectionGrants := func(key string) bool {
		for i := range childOperands {
			if !operandGrants(i, key) {
				return false
			}
		}
		return true
	}

	// Users excluded from some operand's wildcards stay excluded from the
	// intersection's wildcard unless every operand grants them.
	excludedUsersMap := make(map[string]struct{}, 0)
	for _, wildcardExclusions := range operandWildcardExclusions {
		for _, excluded := range wildcardExclusions {
			for key := range excluded {
				excludedUsersMap[key] = struct{}{}
			}
		}
	}
	excludedUsers := []*openfgav1.User{}
	for key := range excludedUsersMap {
		if !intersectionGrants(key) {
			excludedUsers = append(excludedUsers, tuple.StringToUserProto(key))
		}
	}

	candidates := make(map[string]struct{}, 0)
	for _, found := range operandFoundUsers {
		for key := range found {
			candidates[key] = struct{}{}
		}
	}
	for key := range candidates {
		if intersectionGrants(key) {
			fu := foundUser{
				user:          tuple.StringToUserProto(key),
				excludedUsers: excludedUsers,
			}
			concurrency.TrySendThroughChannel(ctx, fu, foundUsersChan)
		}
	}

	return expandResponse{
		err: <-errChan,
	}
}

func (l *listUsersQuery) expandUnion(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_Union,
	foundUsersChan chan<- foundUser,
) expandResponse {
	ctx, span := tracer.Start(ctx, "expandUnion")
	defer span.End()
	pool := concurrency.NewPool(ctx, int(l.resolveNodeBreadthLimit))

	childOperands := rewrite.Union.GetChild()
	unionFoundUsersChans := make([]chan foundUser, len(childOperands))
	for i := range childOperands {
		unionFoundUsersChans[i] = make(chan foundUser, 1)
	}

	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(childOperands))

	wildcardKey := tuple.TypedPublicWildcard(req.GetUserFilters()[0].GetType())
	foundUsersMap := make(map[string]struct{}, 0)
	excludedUsersMap := make(map[string]struct{}, 0)
	// The users excluded from each wildcard found, so that a wildcard only re-grants
	// the users it does not itself exclude.
	wildcardExclusions := []map[string]struct{}{}
	// Consumers must start before the producer pool below: a producer blocks on
	// its second send into a size-1 channel until drained, so if the bounded
	// pool fills before consumers run, the submit loop deadlocks.
	for _, foundUsersChan := range unionFoundUsersChans {
		go func(foundUsersChan chan foundUser) {
			defer wg.Done()

			for foundUser := range foundUsersChan {
				key := tuple.UserProtoToString(foundUser.user)
				excluded := make(map[string]struct{}, len(foundUser.excludedUsers))
				for _, excludedUser := range foundUser.excludedUsers {
					excluded[tuple.UserProtoToString(excludedUser)] = struct{}{}
				}
				mu.Lock()
				for excludedKey := range excluded {
					excludedUsersMap[excludedKey] = struct{}{}
				}
				if foundUser.relationshipStatus == HasRelationship {
					foundUsersMap[key] = struct{}{}
					if key == wildcardKey {
						wildcardExclusions = append(wildcardExclusions, excluded)
					}
				}
				mu.Unlock()
			}
		}(foundUsersChan)
	}

	for i, rewrite := range childOperands {
		pool.Go(func(ctx context.Context) error {
			resp := l.expandRewrite(ctx, req, rewrite, unionFoundUsersChans[i])
			return resp.err
		})
	}

	errChan := make(chan error, 1)

	go func() {
		err := pool.Wait()
		for i := range unionFoundUsersChans {
			close(unionFoundUsersChans[i])
		}
		errChan <- err
		close(errChan)
	}()

	wg.Wait()

	// A user excluded anywhere in the union stays excluded unless the union grants
	// them, either directly or through a wildcard that does not exclude them.
	// Otherwise a wildcard forwarded by the union would re-grant them.
	excludedUsers := []*openfgav1.User{}
	for key := range excludedUsersMap {
		_, granted := foundUsersMap[key]
		for _, excluded := range wildcardExclusions {
			if _, ok := excluded[key]; !ok {
				granted = true
				break
			}
		}
		if !granted {
			excludedUsers = append(excludedUsers, tuple.StringToUserProto(key))
		}
	}

	for key := range foundUsersMap {
		fu := foundUser{
			user:          tuple.StringToUserProto(key),
			excludedUsers: excludedUsers,
		}
		concurrency.TrySendThroughChannel(ctx, fu, foundUsersChan)
	}

	return expandResponse{
		err: <-errChan,
	}
}

func (l *listUsersQuery) expandExclusion(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_Difference,
	foundUsersChan chan<- foundUser,
) expandResponse {
	ctx, span := tracer.Start(ctx, "expandExclusion")
	defer span.End()
	baseFoundUsersCh := make(chan foundUser, 1)
	subtractFoundUsersCh := make(chan foundUser, 1)

	var baseError error
	go func() {
		resp := l.expandRewrite(ctx, req, rewrite.Difference.GetBase(), baseFoundUsersCh)
		baseError = resp.err
		close(baseFoundUsersCh)
	}()

	var subtractError error
	var subtractHasCycle bool
	go func() {
		resp := l.expandRewrite(ctx, req, rewrite.Difference.GetSubtract(), subtractFoundUsersCh)
		subtractError = resp.err
		subtractHasCycle = resp.hasCycle
		close(subtractFoundUsersCh)
	}()

	wildcardKey := tuple.TypedPublicWildcard(req.GetUserFilters()[0].GetType())

	baseFoundUsersMap := make(map[string]foundUser, 0)
	// The base wildcard may arrive from several sources, and it only excludes the
	// users that every copy excludes.
	var baseWildcardExcluded map[string]struct{}
	for fu := range baseFoundUsersCh {
		key := tuple.UserProtoToString(fu.user)
		// A user may also arrive from several base sources, and has the relationship
		// if any of them grants it.
		if existing, ok := baseFoundUsersMap[key]; !ok || existing.relationshipStatus == NoRelationship {
			baseFoundUsersMap[key] = fu
		}
		if key == wildcardKey && fu.relationshipStatus == HasRelationship {
			baseWildcardExcluded = intersectUserKeys(baseWildcardExcluded, fu.excludedUsers)
		}
	}

	subtractFoundUsersMap := make(map[string]foundUser, len(baseFoundUsersMap))
	// As with base, the subtracted wildcard only excludes the users that every copy excludes.
	var subtractWildcardExcluded map[string]struct{}

	for fu := range subtractFoundUsersCh {
		key := tuple.UserProtoToString(fu.user)
		// As with base, a subtracted user has the relationship if any source grants it.
		if existing, ok := subtractFoundUsersMap[key]; !ok || existing.relationshipStatus == NoRelationship {
			subtractFoundUsersMap[key] = fu
		}
		if key == wildcardKey && fu.relationshipStatus == HasRelationship {
			subtractWildcardExcluded = intersectUserKeys(subtractWildcardExcluded, fu.excludedUsers)
		}
	}

	// A cancelled subtract operand may be missing users, and subtracting an
	// incomplete set would grant them. Sends can still succeed after cancellation,
	// so emit nothing.
	if ctx.Err() != nil {
		errs := errors.Join(ctx.Err(), baseError, subtractError)
		telemetry.TraceError(span, errs)
		return expandResponse{err: errs}
	}

	if subtractHasCycle {
		// Because exclusion contains the only bespoke treatment of
		// cycle, everywhere else we consider it a falsey outcome.
		// Once we make a determination within the exclusion handler, we're
		// able to properly handle the case and do not need to propagate
		// the existence of a cycle to an upstream handler.
		return expandResponse{
			err: nil,
		}
	}

	_, baseWildcardExists := baseFoundUsersMap[wildcardKey]
	_, subtractWildcardExists := subtractFoundUsersMap[wildcardKey]

	// The users carved out of a base wildcard, attached to the wildcard itself so that
	// a union combining it with other sources knows which users it does not grant.
	var wildcardExcludedUsers []*openfgav1.User
	if baseWildcardExists {
		for excludedKey := range baseWildcardExcluded {
			if baseFu, ok := baseFoundUsersMap[excludedKey]; ok && baseFu.relationshipStatus == HasRelationship {
				continue
			}
			wildcardExcludedUsers = append(wildcardExcludedUsers, tuple.StringToUserProto(excludedKey))
		}
		for subtractedUserKey, subtractedFu := range subtractFoundUsersMap {
			if !tuple.IsTypedWildcard(subtractedUserKey) && subtractedFu.relationshipStatus == HasRelationship {
				wildcardExcludedUsers = append(wildcardExcludedUsers, tuple.StringToUserProto(subtractedUserKey))
			}
		}
	}

	// The subtract branch has a user directly or through a wildcard that does not exclude them.
	subtractGrants := func(key string) bool {
		if subtractedFu, ok := subtractFoundUsersMap[key]; ok && subtractedFu.relationshipStatus == HasRelationship {
			return true
		}
		_, excluded := subtractWildcardExcluded[key]
		return subtractWildcardExists && !excluded
	}

	if baseWildcardExists && subtractWildcardExists {
		// A subtracted wildcard does not take away the users it excludes, so those
		// keep the relationship wherever the base grants them.
		for excludedKey := range subtractWildcardExcluded {
			if subtractGrants(excludedKey) {
				continue
			}
			_, excludedFromBase := baseWildcardExcluded[excludedKey]
			if baseFu, ok := baseFoundUsersMap[excludedKey]; !excludedFromBase || (ok && baseFu.relationshipStatus == HasRelationship) {
				concurrency.TrySendThroughChannel(ctx, foundUser{
					user:               tuple.StringToUserProto(excludedKey),
					relationshipStatus: HasRelationship,
				}, foundUsersChan)
			}
		}
	}

	for userKey, fu := range baseFoundUsersMap {
		_, userIsSubtracted := subtractFoundUsersMap[userKey]
		_, wildcardSubtracted := subtractFoundUsersMap[wildcardKey]

		switch {
		case baseWildcardExists:
			if !userIsSubtracted && !wildcardSubtracted {
				// Preserve the relationship status and exclusions the base branch
				// already determined in case base on its own contains another exclusion.
				excludedUsers := fu.excludedUsers
				if userKey == wildcardKey {
					excludedUsers = wildcardExcludedUsers
				}
				concurrency.TrySendThroughChannel(ctx, foundUser{
					user:               tuple.StringToUserProto(userKey),
					relationshipStatus: fu.relationshipStatus,
					excludedUsers:      excludedUsers,
				}, foundUsersChan)
			}

			for subtractedUserKey, subtractedFu := range subtractFoundUsersMap {
				if tuple.IsTypedWildcard(subtractedUserKey) {
					if !userIsSubtracted {
						concurrency.TrySendThroughChannel(ctx, foundUser{
							user:               tuple.StringToUserProto(userKey),
							relationshipStatus: NoRelationship,
						}, foundUsersChan)
					}
					continue
				}

				// Another subtract source may still grant a user that one source reports
				// without the relationship.
				if subtractedFu.relationshipStatus == NoRelationship && !subtractGrants(subtractedUserKey) {
					// Only surface the subtracted user as having the relationship when the base
					// grants them, directly or through a wildcard that does not exclude them. When
					// the base is a wildcard carve-out (e.g. `user:* but not member`), re-inflating
					// an excluded user here would return a user that Check denies.
					_, excludedFromBase := baseWildcardExcluded[subtractedUserKey]
					if baseFu, ok := baseFoundUsersMap[subtractedUserKey]; !excludedFromBase || (ok && baseFu.relationshipStatus == HasRelationship) {
						concurrency.TrySendThroughChannel(ctx, foundUser{
							user:               tuple.StringToUserProto(subtractedUserKey),
							relationshipStatus: HasRelationship,
						}, foundUsersChan)
					}
				}

				// a found user under the subtracted branch causes the subtracted user to have a negated relationship with respect
				// to the base relation and is excluded since a wildcard is contained under the base branch.
				if subtractedFu.relationshipStatus == HasRelationship {
					concurrency.TrySendThroughChannel(ctx, foundUser{
						user:               tuple.StringToUserProto(subtractedUserKey),
						relationshipStatus: NoRelationship,
						excludedUsers: []*openfgav1.User{
							tuple.StringToUserProto(subtractedUserKey),
						},
					}, foundUsersChan)
				}
			}
		case subtractWildcardExists, userIsSubtracted:
			if subtractGrants(userKey) {
				concurrency.TrySendThroughChannel(ctx, foundUser{
					user:               tuple.StringToUserProto(userKey),
					relationshipStatus: NoRelationship,
				}, foundUsersChan)
			} else if fu.relationshipStatus == HasRelationship {
				concurrency.TrySendThroughChannel(ctx, foundUser{
					user:               tuple.StringToUserProto(userKey),
					relationshipStatus: HasRelationship,
				}, foundUsersChan)
			}

		default:
			concurrency.TrySendThroughChannel(ctx, foundUser{
				user:               tuple.StringToUserProto(userKey),
				relationshipStatus: fu.relationshipStatus,
			}, foundUsersChan)
		}
	}

	errs := errors.Join(baseError, subtractError)
	if errs != nil {
		telemetry.TraceError(span, errs)
	}
	return expandResponse{
		err: errs,
	}
}

// intersectUserKeys narrows common to the keys of users that it also holds. A nil common
// map means nothing has been intersected yet, so every user is kept. It is used to narrow
// the users excluded from a wildcard, which may arrive from several sources, to those
// that every copy excludes.
func intersectUserKeys(common map[string]struct{}, users []*openfgav1.User) map[string]struct{} {
	narrowed := make(map[string]struct{}, len(users))
	for _, user := range users {
		key := tuple.UserProtoToString(user)
		if _, ok := common[key]; ok || common == nil {
			narrowed[key] = struct{}{}
		}
	}
	return narrowed
}

func (l *listUsersQuery) expandTTU(
	ctx context.Context,
	req *internalListUsersRequest,
	rewrite *openfgav1.Userset_TupleToUserset,
	foundUsersChan chan<- foundUser,
) expandResponse {
	ctx, span := tracer.Start(ctx, "expandTTU")
	defer span.End()
	tuplesetRelation := rewrite.TupleToUserset.GetTupleset().GetRelation()
	computedRelation := rewrite.TupleToUserset.GetComputedUserset().GetRelation()

	typesys, _ := typesystem.TypesystemFromContext(ctx)

	opts := storage.ReadOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}
	iter, err := l.datastore.Read(ctx, req.GetStoreId(), storage.ReadFilter{
		Object:   tuple.ObjectKey(req.GetObject()),
		Relation: tuplesetRelation,
	}, opts)
	if err != nil {
		telemetry.TraceError(span, err)
		// Once the context is done, report its error instead: the cancellation may
		// have caused this one, and the request then returns partial results.
		return expandResponse{
			err: cmp.Or(ctx.Err(), err),
		}
	}
	defer iter.Stop()

	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(typesys),
	)
	defer filteredIter.Stop()

	pool := concurrency.NewPool(ctx, int(l.resolveNodeBreadthLimit))

	var errs error

LoopOnIterator:
	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if !errors.Is(err, storage.ErrIteratorDone) {
				errs = errors.Join(errs, cmp.Or(ctx.Err(), err))
			}

			break LoopOnIterator
		}

		var condMet bool
		if condition.IsInlineExpression(tupleKey.GetCondition().GetName()) {
			condMet, err = eval.EvaluateInlineExpression(ctx, tupleKey, req.Context)
		} else {
			cond, _ := typesys.GetCondition(tupleKey.GetCondition().GetName())
			condMet, err = eval.EvaluateTupleCondition(ctx, tupleKey, cond, req.Context)
		}
		if err != nil {
			errs = errors.Join(errs, cmp.Or(ctx.Err(), err))
			if !errors.Is(err, condition.ErrEvaluationFailed) {
				break LoopOnIterator
			}
			telemetry.TraceError(span, err)
		}

		if !condMet {
			continue
		}

		userObject := tupleKey.GetUser()
		userObjectType, userObjectID := tuple.SplitObject(userObject)

		pool.Go(func(ctx context.Context) error {
			rewrittenReq := req.clone()
			rewrittenReq.Object = &openfgav1.Object{Type: userObjectType, Id: userObjectID}
			rewrittenReq.Relation = computedRelation
			resp := l.dispatch(ctx, rewrittenReq, foundUsersChan)
			return resp.err
		})
	}

	errs = errors.Join(pool.Wait(), errs)
	if errs != nil {
		telemetry.TraceError(span, errs)
	}
	return expandResponse{
		err: errs,
	}
}

func enteredCycle(req *internalListUsersRequest) bool {
	key := fmt.Sprintf("%s#%s", tuple.ObjectKey(req.GetObject()), req.Relation)
	if _, loaded := req.visitedUsersetsMap[key]; loaded {
		return true
	}
	req.visitedUsersetsMap[key] = struct{}{}
	return false
}

func (l *listUsersQuery) buildResultsChannel() chan foundUser {
	foundUsersCh := make(chan foundUser, serverconfig.DefaultListUsersMaxResults)
	maxResults := l.maxResults
	if maxResults > 0 {
		foundUsersCh = make(chan foundUser, maxResults)
	}

	return foundUsersCh
}

func panicError(recovered *panics.Recovered) error {
	return fmt.Errorf("%w: %w", ErrPanic, recovered.AsError())
}

func panicExpanseResponse(recovered *panics.Recovered) expandResponse {
	return expandResponse{
		hasCycle: false,
		err:      panicError(recovered),
	}
}
