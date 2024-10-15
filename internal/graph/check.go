package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	openfgaErrors "github.com/openfga/openfga/internal/errors"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var tracer = otel.Tracer("internal/graph/check")

type setOperatorType int

const (
	unionSetOperator setOperatorType = iota
	intersectionSetOperator
	exclusionSetOperator
)

type checkOutcome struct {
	resp *ResolveCheckResponse
	err  error
}

type LocalChecker struct {
	delegate             CheckResolver
	concurrencyLimit     uint32
	maxConcurrentReads   uint32
	usersetBatchSize     int
	logger               logger.Logger
	optimizationsEnabled bool
}

type LocalCheckerOption func(d *LocalChecker)

// WithResolveNodeBreadthLimit see server.WithResolveNodeBreadthLimit.
func WithResolveNodeBreadthLimit(limit uint32) LocalCheckerOption {
	return func(d *LocalChecker) {
		d.concurrencyLimit = limit
	}
}

func WithOptimizations(enabled bool) LocalCheckerOption {
	return func(d *LocalChecker) {
		d.optimizationsEnabled = enabled
	}
}

// WithUsersetBatchSize see server.WithUsersetBatchSize.
func WithUsersetBatchSize(usersetBatchSize uint32) LocalCheckerOption {
	return func(d *LocalChecker) {
		d.usersetBatchSize = int(usersetBatchSize)
	}
}

// WithMaxConcurrentReads see server.WithMaxConcurrentReadsForCheck.
func WithMaxConcurrentReads(limit uint32) LocalCheckerOption {
	return func(d *LocalChecker) {
		d.maxConcurrentReads = limit
	}
}

func WithLocalCheckerLogger(logger logger.Logger) LocalCheckerOption {
	return func(d *LocalChecker) {
		d.logger = logger
	}
}

// NewLocalChecker constructs a LocalChecker that can be used to evaluate a Check
// request locally.
//
// Developers wanting a LocalChecker with other optional layers (e.g caching and others)
// are encouraged to use [[NewOrderedCheckResolvers]] instead.
func NewLocalChecker(opts ...LocalCheckerOption) *LocalChecker {
	checker := &LocalChecker{
		concurrencyLimit:   serverconfig.DefaultResolveNodeBreadthLimit,
		maxConcurrentReads: serverconfig.DefaultMaxConcurrentReadsForCheck,
		usersetBatchSize:   serverconfig.DefaultUsersetBatchSize,
		logger:             logger.NewNoopLogger(),
	}
	// by default, a LocalChecker delegates/dispatchs subproblems to itself (e.g. local dispatch) unless otherwise configured.
	checker.delegate = checker

	for _, opt := range opts {
		opt(checker)
	}

	return checker
}

// SetDelegate sets this LocalChecker's dispatch delegate.
func (c *LocalChecker) SetDelegate(delegate CheckResolver) {
	c.delegate = delegate
}

// GetDelegate sets this LocalChecker's dispatch delegate.
func (c *LocalChecker) GetDelegate() CheckResolver {
	return c.delegate
}

// CheckHandlerFunc defines a function that evaluates a CheckResponse or returns an error
// otherwise.
type CheckHandlerFunc func(ctx context.Context) (*ResolveCheckResponse, error)

// CheckFuncReducer defines a function that combines or reduces one or more CheckHandlerFunc into
// a single CheckResponse with a maximum limit on the number of concurrent evaluations that can be
// in flight at any given time.
type CheckFuncReducer func(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*ResolveCheckResponse, error)

// resolver concurrently resolves one or more CheckHandlerFunc and yields the results on the provided resultChan.
// Callers of the 'resolver' function should be sure to invoke the callback returned from this function to ensure
// every concurrent check is evaluated. The concurrencyLimit can be set to provide a maximum number of concurrent
// evaluations in flight at any point.
func resolver(ctx context.Context, concurrencyLimit uint32, resultChan chan<- checkOutcome, handlers ...CheckHandlerFunc) func() {
	limiter := make(chan struct{}, concurrencyLimit)

	var wg sync.WaitGroup

	checker := func(fn CheckHandlerFunc) {
		defer func() {
			wg.Done()
			<-limiter
		}()

		resolved := make(chan checkOutcome, 1)

		if ctx.Err() != nil {
			resultChan <- checkOutcome{nil, ctx.Err()}
			return
		}

		go func() {
			resp, err := fn(ctx)
			resolved <- checkOutcome{resp, err}
		}()

		select {
		case <-ctx.Done():
			return
		case res := <-resolved:
			resultChan <- res
		}
	}

	wg.Add(1)
	go func() {
	outer:
		for _, handler := range handlers {
			fn := handler // capture loop var

			select {
			case limiter <- struct{}{}:
				wg.Add(1)
				go checker(fn)
			case <-ctx.Done():
				break outer
			}
		}

		wg.Done()
	}()

	return func() {
		wg.Wait()
		close(limiter)
	}
}

// union implements a CheckFuncReducer that requires any of the provided CheckHandlerFunc to resolve
// to an allowed outcome. The first allowed outcome causes premature termination of the reducer.
func union(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*ResolveCheckResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	resultChan := make(chan checkOutcome, len(handlers))

	drain := resolver(ctx, concurrencyLimit, resultChan, handlers...)

	defer func() {
		cancel()
		drain()
		close(resultChan)
	}()

	var dbReads uint32
	var err error
	var cycleDetected bool
	for i := 0; i < len(handlers); i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				err = result.err
				continue
			}

			if result.resp.GetCycleDetected() {
				cycleDetected = true
			}

			dbReads += result.resp.GetResolutionMetadata().DatastoreQueryCount

			if result.resp.GetAllowed() {
				result.resp.GetResolutionMetadata().DatastoreQueryCount = dbReads
				return result.resp, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if err != nil {
		return nil, err
	}

	return &ResolveCheckResponse{
		Allowed: false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: dbReads,
			CycleDetected:       cycleDetected,
		},
	}, nil
}

// intersection implements a CheckFuncReducer that requires all of the provided CheckHandlerFunc to resolve
// to an allowed outcome. The first falsey or erroneous outcome causes premature termination of the reducer.
func intersection(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*ResolveCheckResponse, error) {
	if len(handlers) == 0 {
		return &ResolveCheckResponse{
			Allowed:            false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{},
		}, nil
	}

	span := trace.SpanFromContext(ctx)

	ctx, cancel := context.WithCancel(ctx)
	resultChan := make(chan checkOutcome, len(handlers))

	drain := resolver(ctx, concurrencyLimit, resultChan, handlers...)

	defer func() {
		cancel()
		drain()
		close(resultChan)
	}()

	var dbReads uint32
	var err error
	for i := 0; i < len(handlers); i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				span.RecordError(result.err)
				err = errors.Join(err, result.err)
				continue
			}

			dbReads += result.resp.GetResolutionMetadata().DatastoreQueryCount

			if result.resp.GetCycleDetected() || !result.resp.GetAllowed() {
				result.resp.GetResolutionMetadata().DatastoreQueryCount = dbReads
				return result.resp, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// all operands are either truthy or we've seen at least one error
	if err != nil {
		return nil, err
	}

	return &ResolveCheckResponse{
		Allowed: true,
		ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: dbReads,
		},
	}, nil
}

// exclusion implements a CheckFuncReducer that requires a 'base' CheckHandlerFunc to resolve to an allowed
// outcome and a 'sub' CheckHandlerFunc to resolve to a falsey outcome. The base and sub computations are
// handled concurrently relative to one another.
func exclusion(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*ResolveCheckResponse, error) {
	if len(handlers) != 2 {
		return nil, fmt.Errorf("%w, expected two rewrite operands for exclusion operator, but got '%d'", openfgaErrors.ErrUnknown, len(handlers))
	}

	span := trace.SpanFromContext(ctx)

	limiter := make(chan struct{}, concurrencyLimit)

	ctx, cancel := context.WithCancel(ctx)
	baseChan := make(chan checkOutcome, 1)
	subChan := make(chan checkOutcome, 1)

	var wg sync.WaitGroup

	defer func() {
		cancel()
		wg.Wait()
		close(baseChan)
		close(subChan)
	}()

	baseHandler := handlers[0]
	subHandler := handlers[1]

	limiter <- struct{}{}
	wg.Add(1)
	go func() {
		resp, err := baseHandler(ctx)
		baseChan <- checkOutcome{resp, err}
		<-limiter
		wg.Done()
	}()

	limiter <- struct{}{}
	wg.Add(1)
	go func() {
		resp, err := subHandler(ctx)
		subChan <- checkOutcome{resp, err}
		<-limiter
		wg.Done()
	}()

	response := &ResolveCheckResponse{
		Allowed: false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: 0,
		},
	}

	var baseErr error
	var subErr error

	var dbReads uint32
	for i := 0; i < len(handlers); i++ {
		select {
		case baseResult := <-baseChan:
			if baseResult.err != nil {
				span.RecordError(baseResult.err)
				baseErr = baseResult.err
				continue
			}

			dbReads += baseResult.resp.GetResolutionMetadata().DatastoreQueryCount

			if baseResult.resp.GetCycleDetected() {
				return &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: dbReads,
						CycleDetected:       true,
					},
				}, nil
			}

			if !baseResult.resp.GetAllowed() {
				response.GetResolutionMetadata().DatastoreQueryCount = dbReads
				return response, nil
			}

		case subResult := <-subChan:
			if subResult.err != nil {
				span.RecordError(subResult.err)
				subErr = subResult.err
				continue
			}

			dbReads += subResult.resp.GetResolutionMetadata().DatastoreQueryCount

			if subResult.resp.GetCycleDetected() {
				return &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: dbReads,
						CycleDetected:       true,
					},
				}, nil
			}

			if subResult.resp.GetAllowed() {
				response.GetResolutionMetadata().DatastoreQueryCount = dbReads
				return response, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// base is either (true) or error, sub is either (false) or error:
	// true, false - true
	// true, error - error
	// error, false - error
	// error, error - error
	if baseErr != nil || subErr != nil {
		return nil, errors.Join(baseErr, subErr)
	}

	return &ResolveCheckResponse{
		Allowed: true,
		ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: dbReads,
		},
	}, nil
}

// Close is a noop.
func (c *LocalChecker) Close() {
}

// dispatch clones the parent request, modifies its metadata and tupleKey, and dispatches the new request
// to the CheckResolver this LocalChecker was constructed with.
func (c *LocalChecker) dispatch(_ context.Context, parentReq *ResolveCheckRequest, tk *openfgav1.TupleKey) CheckHandlerFunc {
	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		parentReq.GetRequestMetadata().DispatchCounter.Add(1)
		childRequest := parentReq.clone()
		childRequest.TupleKey = tk
		childRequest.GetRequestMetadata().Depth--

		resp, err := c.delegate.ResolveCheck(ctx, childRequest)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
}

var _ CheckResolver = (*LocalChecker)(nil)

// ResolveCheck implements [[CheckResolver.ResolveCheck]].
func (c *LocalChecker) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	ctx, span := tracer.Start(ctx, "ResolveCheck", trace.WithAttributes(
		attribute.String("store_id", req.GetStoreID()),
		attribute.String("resolver_type", "LocalChecker"),
		attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(req.GetTupleKey())),
	))
	defer span.End()

	if req.GetRequestMetadata().Depth == 0 {
		return nil, ErrResolutionDepthExceeded
	}

	cycle := c.hasCycle(req)
	if cycle {
		span.SetAttributes(attribute.Bool("cycle_detected", true))
		return &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				CycleDetected: true,
			},
		}, nil
	}

	tupleKey := req.GetTupleKey()
	object := tupleKey.GetObject()
	relation := tupleKey.GetRelation()

	if tuple.IsSelfDefining(req.GetTupleKey()) {
		return &ResolveCheckResponse{
			Allowed: true,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount,
			},
		}, nil
	}

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
	}
	_, ok = storage.RelationshipTupleReaderFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: relationship tuple reader datastore missing in context", openfgaErrors.ErrUnknown)
	}

	objectType, _ := tuple.SplitObject(object)
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		return nil, fmt.Errorf("relation '%s' undefined for object type '%s'", relation, objectType)
	}

	resp, err := c.checkRewrite(ctx, req, rel.GetRewrite())(ctx)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	return resp, nil
}

// hasCycle returns true if a cycle has been found. It modifies the request object.
func (c *LocalChecker) hasCycle(req *ResolveCheckRequest) bool {
	key := tuple.TupleKeyToString(req.GetTupleKey())
	if req.VisitedPaths == nil {
		req.VisitedPaths = map[string]struct{}{}
	}

	_, cycleDetected := req.VisitedPaths[key]
	if cycleDetected {
		return true
	}

	req.VisitedPaths[key] = struct{}{}
	return false
}

// usersetsMapType is a map where the key is object#relation and the value is a sorted set (no duplicates allowed).
// For example, given [group:1#member, group:2#member, group:1#owner, group:3#owner] it will be stored as:
// [group#member][1, 2]
// [group#owner][1, 3].
type usersetsMapType map[string]storage.SortedSet

func checkAssociatedObjects(ctx context.Context, req *ResolveCheckRequest, objectRel string, objectIDs storage.SortedSet) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkAssociatedObjects")
	defer span.End()

	typesys, _ := typesystem.TypesystemFromContext(ctx)
	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)

	i, err := checkutil.IteratorReadStartingFromUser(ctx, typesys, ds, req, objectRel, objectIDs)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	reqContext := req.GetContext()
	// filter out invalid tuples yielded by the database iterator
	filteredIter := storage.NewConditionsFilteredTupleKeyIterator(
		storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(i),
			validation.FilterInvalidTuples(typesys),
		),
		checkutil.BuildTupleKeyConditionFilter(ctx, reqContext, typesys),
	)
	defer filteredIter.Stop()

	allowed, err := checkutil.ObjectIDInSortedSet(ctx, filteredIter, objectIDs)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}
	if allowed {
		span.SetAttributes(attribute.Bool("allowed", true))
	}

	return &ResolveCheckResponse{
		Allowed: allowed,
		ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: 1,
		},
	}, nil
}

type dispatchParams struct {
	parentReq *ResolveCheckRequest
	tk        *openfgav1.TupleKey
}

type dispatchMsg struct {
	err            error
	shortCircuit   bool
	dispatchParams *dispatchParams
}

func (c *LocalChecker) produceUsersetDispatches(ctx context.Context, req *ResolveCheckRequest, dispatches chan dispatchMsg, iter *storage.ConditionsFilteredTupleKeyIterator) {
	defer close(dispatches)
	reqTupleKey := req.GetTupleKey()
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	for {
		t, err := iter.Next(ctx)
		if err != nil {
			// cancelled doesn't need to flush nor send errors back to main routine
			if storage.IterIsDoneOrCancelled(err) {
				break
			}
			concurrency.TrySendThroughChannel(ctx, dispatchMsg{err: err}, dispatches)
			break
		}

		usersetObject, usersetRelation := tuple.SplitObjectRelation(t.GetUser())

		// if the user value is a typed wildcard and the type of the wildcard
		// matches the target user objectType, then we're done searching
		if tuple.IsTypedWildcard(usersetObject) && typesystem.IsSchemaVersionSupported(typesys.GetSchemaVersion()) {
			wildcardType := tuple.GetType(usersetObject)

			if tuple.GetType(reqTupleKey.GetUser()) == wildcardType {
				concurrency.TrySendThroughChannel(ctx, dispatchMsg{shortCircuit: true}, dispatches)
				break
			}
		}

		if usersetRelation != "" {
			tupleKey := tuple.NewTupleKey(usersetObject, usersetRelation, reqTupleKey.GetUser())
			concurrency.TrySendThroughChannel(ctx, dispatchMsg{dispatchParams: &dispatchParams{parentReq: req, tk: tupleKey}}, dispatches)
		}
	}
}

// processDispatches returns a channel where the outcomes of the dispatched checks are sent, and begins sending messages to this channel.
func (c *LocalChecker) processDispatches(ctx context.Context, limit uint32, dispatchChan chan dispatchMsg) chan checkOutcome {
	outcomes := make(chan checkOutcome, limit)
	dispatchPool := concurrency.NewPool(ctx, int(limit))

	go func() {
		defer func() {
			// We need to wait always to avoid a goroutine leak.
			_ = dispatchPool.Wait()
			close(outcomes)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-dispatchChan:
				if !ok {
					return
				}
				if msg.err != nil {
					concurrency.TrySendThroughChannel(ctx, checkOutcome{err: msg.err}, outcomes)
					break // continue
				}
				if msg.shortCircuit {
					resp := &ResolveCheckResponse{
						Allowed: true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							DatastoreQueryCount: 0,
						},
					}
					concurrency.TrySendThroughChannel(ctx, checkOutcome{resp: resp}, outcomes)
					return
				}

				if msg.dispatchParams != nil {
					dispatchPool.Go(func(ctx context.Context) error {
						resp, err := c.dispatch(ctx, msg.dispatchParams.parentReq, msg.dispatchParams.tk)(ctx)
						concurrency.TrySendThroughChannel(ctx, checkOutcome{resp: resp, err: err}, outcomes)
						return nil
					})
				}
			}
		}
	}()

	return outcomes
}

func (c *LocalChecker) consumeDispatches(ctx context.Context, req *ResolveCheckRequest, limit uint32, dispatchChan chan dispatchMsg) (*ResolveCheckResponse, error) {
	cancellableCtx, cancel := context.WithCancel(ctx)
	outcomeChannel := c.processDispatches(cancellableCtx, limit, dispatchChan)

	var finalErr error
	finalResult := &ResolveCheckResponse{
		Allowed: false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount,
		},
	}

ConsumerLoop:
	for {
		select {
		case <-ctx.Done():
			break ConsumerLoop
		case outcome, ok := <-outcomeChannel:
			if !ok {
				break ConsumerLoop
			}
			if outcome.err != nil {
				finalErr = outcome.err
				break // continue
			}

			if outcome.resp.GetResolutionMetadata().CycleDetected {
				finalResult.ResolutionMetadata.CycleDetected = true
			}

			finalResult.ResolutionMetadata.DatastoreQueryCount += outcome.resp.GetResolutionMetadata().DatastoreQueryCount

			if outcome.resp.Allowed {
				finalErr = nil
				dbReads := finalResult.GetResolutionMetadata().DatastoreQueryCount
				finalResult = outcome.resp
				finalResult.ResolutionMetadata.DatastoreQueryCount = dbReads
				break ConsumerLoop
			}
		}
	}
	cancel() // prevent further processing of other checks
	// context cancellation from upstream (e.g. client)
	if ctx.Err() != nil {
		finalErr = ctx.Err()
	}
	if finalErr != nil {
		return nil, finalErr
	}

	return finalResult, nil
}

// checkUsersetSlowPath will check userset or public wildcard path.
// This is the slow path as it requires dispatch on all its children.
func (c *LocalChecker) checkUsersetSlowPath(ctx context.Context, req *ResolveCheckRequest, iter *storage.ConditionsFilteredTupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkUsersetSlowPath")
	defer span.End()

	dispatchChan := make(chan dispatchMsg, c.concurrencyLimit)

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	// sending to channel in batches up to a pre-configured value to subsequently checkMembership for.
	pool := concurrency.NewPool(cancellableCtx, 1)
	defer func() {
		cancelFunc()
		// We need to wait always to avoid a goroutine leak.
		_ = pool.Wait()
	}()
	pool.Go(func(ctx context.Context) error {
		c.produceUsersetDispatches(ctx, req, dispatchChan, iter)
		return nil
	})

	resp, err := c.consumeDispatches(ctx, req, c.concurrencyLimit, dispatchChan)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	// for the read in checkDirectUsersetTuples
	resp.GetResolutionMetadata().DatastoreQueryCount++

	return resp, nil
}

// checkUsersetFastPath is the fast path to evaluate userset.
// The general idea of the algorithm is that it tries to find intersection on the objects as identified in the userset
// with the objects the user has the specified relation with.
// For example, for the following model, for check(user:bob, viewer, doc:1)
//
//	type group
//	  define member: [user]
//	type doc
//	  define viewer: [group#member]
//
// We will first look up the group(s) that are assigned to doc:1
// Next, we will look up all the group where user:bob is a member of.
// Finally, find the intersection between the two.
// To use the fast path, we will need to ensure that the userset and all the children associated with the userset are
// exclusively directly assignable. In our case, group member must be directly exclusively assignable.
func (c *LocalChecker) checkUsersetFastPath(ctx context.Context, req *ResolveCheckRequest, iter *storage.ConditionsFilteredTupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkUsersetFastPath")
	defer span.End()
	// Caller already verified typesys
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	usersetDetails := checkutil.BuildUsersetDetailsUserset(typesys)
	return c.checkMembership(ctx, req, iter, usersetDetails)
}

type usersetsChannelType struct {
	err            error
	objectRelation string            // e.g. group#member
	objectIDs      storage.SortedSet // eg. [1,2,3] (no duplicates allowed, sorted)
}

// checkMembership for this model
//
// type user
// type org
//
//	relations
//		define viewer: [user]
//
// type folder
//
//	relations
//		define viewer: [user]
//
// type doc
//
//	relations
//		define viewer: viewer from parent
//		define parent: [folder, org]
//
// works as follows.
// If the request is Check(user:maria, viewer, doc:1).
// 1. We build a map with folder#viewer:[1...N], org#viewer:[1...M] that are parents of doc:1. We send those through a channel.
// 2. The consumer of the channel finds all the folders (and orgs) by looking at tuples of the form folder:X#viewer@user:maria (and org:Y#viewer@user:maria).
// 3. If there is one folder or org found in step (2) that appears in the map found in step (1), it returns allowed=true immediately.
func (c *LocalChecker) checkMembership(ctx context.Context, req *ResolveCheckRequest, iter *storage.ConditionsFilteredTupleKeyIterator, usersetDetails checkutil.UsersetDetailsFunc) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkMembership")
	defer span.End()

	// all at least 1 message to queue up
	usersetsChan := make(chan usersetsChannelType, 2)

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	// sending to channel in batches up to a pre-configured value to subsequently checkMembership for.
	pool := concurrency.NewPool(cancellableCtx, 1)
	defer func() {
		cancelFunc()
		// We need to wait always to avoid a goroutine leak.
		_ = pool.Wait()
	}()
	pool.Go(func(ctx context.Context) error {
		c.produceUsersets(ctx, usersetsChan, iter, usersetDetails)
		return nil
	})

	resp, err := c.consumeUsersets(ctx, req, usersetsChan)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	// read from either checkTTU or checkDirectUsersetTuples
	resp.ResolutionMetadata.DatastoreQueryCount++

	return resp, err
}

// processUsersets returns a channel where the outcomes of the checkAssociatedObjects checks are sent, and begins sending messages to this channel.
func (c *LocalChecker) processUsersets(ctx context.Context, req *ResolveCheckRequest, usersetsChan chan usersetsChannelType, limit uint32) chan checkOutcome {
	outcomes := make(chan checkOutcome, limit)
	pool := concurrency.NewPool(ctx, int(limit))

	go func() {
		defer func() {
			// We need to wait always to avoid a goroutine leak.
			_ = pool.Wait()
			close(outcomes)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-usersetsChan:
				if !ok {
					return
				}
				if msg.err != nil {
					concurrency.TrySendThroughChannel(ctx, checkOutcome{err: msg.err}, outcomes)
					break // continue
				}

				pool.Go(func(ctx context.Context) error {
					resp, err := checkAssociatedObjects(ctx, req, msg.objectRelation, msg.objectIDs)
					concurrency.TrySendThroughChannel(ctx, checkOutcome{resp: resp, err: err}, outcomes)
					return nil
				})
			}
		}
	}()

	return outcomes
}

func (c *LocalChecker) consumeUsersets(ctx context.Context, req *ResolveCheckRequest, usersetsChan chan usersetsChannelType) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "consumeUsersets")
	defer span.End()

	cancellableCtx, cancel := context.WithCancel(ctx)
	outcomeChannel := c.processUsersets(cancellableCtx, req, usersetsChan, 2)

	var finalErr error
	finalResult := &ResolveCheckResponse{
		Allowed: false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount,
		},
	}

ConsumerLoop:
	for {
		select {
		case <-ctx.Done():
			break ConsumerLoop
		case outcome, channelOpen := <-outcomeChannel:
			if !channelOpen {
				break ConsumerLoop
			}
			if outcome.err != nil {
				finalErr = outcome.err
				break // continue
			}

			if outcome.resp.GetResolutionMetadata().CycleDetected {
				finalResult.ResolutionMetadata.CycleDetected = true
			}

			finalResult.ResolutionMetadata.DatastoreQueryCount += outcome.resp.GetResolutionMetadata().DatastoreQueryCount

			if outcome.resp.Allowed {
				finalErr = nil
				dbReads := finalResult.GetResolutionMetadata().DatastoreQueryCount
				finalResult = outcome.resp
				finalResult.ResolutionMetadata.DatastoreQueryCount = dbReads
				break ConsumerLoop
			}
		}
	}
	cancel() // prevent further processing of other checks
	// context cancellation from upstream (e.g. client)
	if ctx.Err() != nil {
		finalErr = ctx.Err()
	}

	if finalErr != nil {
		return nil, finalErr
	}

	return finalResult, nil
}

func (c *LocalChecker) produceUsersets(ctx context.Context, usersetsChan chan usersetsChannelType, iter *storage.ConditionsFilteredTupleKeyIterator, usersetDetails checkutil.UsersetDetailsFunc) {
	ctx, span := tracer.Start(ctx, "produceUsersets")
	defer span.End()

	usersetsMap := make(usersetsMapType)
	defer close(usersetsChan)
	for {
		t, err := iter.Next(ctx)
		if err != nil {
			// cancelled doesn't need to flush nor send errors back to main routine
			if !storage.IterIsDoneOrCancelled(err) {
				concurrency.TrySendThroughChannel(ctx, usersetsChannelType{err: err}, usersetsChan)
			}
			break
		}

		objectRel, objectID, err := usersetDetails(t)
		if err != nil {
			if errors.Is(err, typesystem.ErrRelationUndefined) {
				continue
			}
			concurrency.TrySendThroughChannel(ctx, usersetsChannelType{err: err}, usersetsChan)
			break
		}

		if _, ok := usersetsMap[objectRel]; !ok {
			if len(usersetsMap) > 0 {
				// Flush results from a previous objectRel it begin processing immediately.
				// The assumption (which may not be true) is that the datastore yields objectRel in order.
				trySendUsersetsAndDeleteFromMap(ctx, usersetsMap, usersetsChan)
			}
			usersetsMap[objectRel] = storage.NewSortedSet()
		}

		usersetsMap[objectRel].Add(objectID)

		if usersetsMap[objectRel].Size() >= c.usersetBatchSize {
			trySendUsersetsAndDeleteFromMap(ctx, usersetsMap, usersetsChan)
		}
	}

	trySendUsersetsAndDeleteFromMap(ctx, usersetsMap, usersetsChan)
}

func trySendUsersetsAndDeleteFromMap(ctx context.Context, usersetsMap usersetsMapType, usersetsChan chan usersetsChannelType) {
	for k, v := range usersetsMap {
		concurrency.TrySendThroughChannel(ctx, usersetsChannelType{objectRelation: k, objectIDs: v}, usersetsChan)
		delete(usersetsMap, k)
	}
}

// recursiveMatchUserUsersetCommonData groups common parameters needed
// for recursiveMatchUserUserset for convenience purpose.
type recursiveMatchUserUsersetCommonData struct {
	typesys                     *typesystem.TypeSystem
	ds                          storage.RelationshipTupleReader
	allowedUserTypeRestrictions []*openfgav1.RelationReference
	userToUsersetMapping        storage.SortedSet
	concurrencyLimit            int
	tupleMapperKind             TupleMapperKind
	// The following member are atomic/sync in anticipation
	// that the algorithm will parallelize the lookup.
	dsCount        *atomic.Uint32
	visitedUserset *sync.Map
}

// recursiveMatchUserUsersetFunc defines a function that recursively evaluates whether objects' matches userToUsersetMapping.
type recursiveMatchUserUsersetFunc func(ctx context.Context,
	req *ResolveCheckRequest,
	commonParameters *recursiveMatchUserUsersetCommonData,
	mapper TupleMapper) (*ResolveCheckResponse, error)

func parallelizeRecursiveMatchUserUserset(ctx context.Context, usersetItems []string, req *ResolveCheckRequest, commonParameters *recursiveMatchUserUsersetCommonData, recursiveFunc recursiveMatchUserUsersetFunc) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "parallelizeRecursiveMatchUserUserset")
	defer span.End()
	checkOutcomeChan := make(chan checkOutcome, commonParameters.concurrencyLimit)

	pool := concurrency.NewPool(ctx, commonParameters.concurrencyLimit)
	cancellableCtx, cancelFunc := context.WithCancel(ctx)

	defer func() {
		cancelFunc()
	}()

	go func() {
		defer func() {
			_ = pool.Wait()
			close(checkOutcomeChan)
		}()
		for _, usersetItem := range usersetItems {
			_, visited := commonParameters.visitedUserset.LoadOrStore(usersetItem, struct{}{})
			if !visited {
				newReq := req.clone()
				newTupleKey := tuple.NewTupleKey(usersetItem, req.GetTupleKey().GetRelation(), req.GetTupleKey().GetUser())
				newReq.TupleKey = newTupleKey
				newMapper, err := buildMapper(ctx, newReq, commonParameters)
				if err != nil {
					concurrency.TrySendThroughChannel(cancellableCtx, checkOutcome{
						err: err,
					}, checkOutcomeChan)
					return
				}
				pool.Go(func(ctx context.Context) error {
					result, err := recursiveFunc(cancellableCtx, newReq, commonParameters, newMapper)
					concurrency.TrySendThroughChannel(cancellableCtx, checkOutcome{
						resp: result,
						err:  err,
					}, checkOutcomeChan)

					return nil
				})
			}
			// Note that visited does not necessary means that there are cycles.  For the following model,
			// type user
			// type group
			//   relations
			//     define member: [user, group#member]
			// We have something like
			// group:1#member@group:2#member
			// group:1#member@group:3#member
			// group:2#member@group:a#member
			// group:3#member@group:a#member
			// Note that both group:2#member and group:3#member has group:a#member. However, they are not cycles.
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case msg, ok := <-checkOutcomeChan:
			if !ok {
				return &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + commonParameters.dsCount.Load(),
					},
				}, nil
			}
			if msg.err != nil {
				if errors.Is(msg.err, context.Canceled) || errors.Is(msg.err, context.DeadlineExceeded) {
					// ignore message
					break
				}
				return nil, msg.err
			}
			if msg.resp != nil && msg.resp.GetAllowed() {
				return msg.resp, nil
			}
		}
	}
}

func recursiveMatchUserUserset(ctx context.Context, req *ResolveCheckRequest, commonParameters *recursiveMatchUserUsersetCommonData,
	mapper TupleMapper) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "recursiveMatchUserUserset")
	defer span.End()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	req.GetRequestMetadata().Depth--
	if req.GetRequestMetadata().Depth == 0 {
		return nil, ErrResolutionDepthExceeded
	}

	commonParameters.dsCount.Add(1)
	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	objectToUsersetMessageChan := streamedLookupUsersetForObject(cancellableCtx, commonParameters, mapper)

	var usersetItems []string
	for usersetMsg := range objectToUsersetMessageChan {
		if usersetMsg.err != nil {
			return nil, usersetMsg.err
		}
		usersetName := usersetMsg.userset
		if commonParameters.userToUsersetMapping.Exists(usersetName) {
			return &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + commonParameters.dsCount.Load(),
				},
			}, nil
		}
		usersetItems = append(usersetItems, usersetName)
	}

	if len(usersetItems) == 0 {
		return &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + commonParameters.dsCount.Load(),
			},
		}, nil
	}

	return parallelizeRecursiveMatchUserUserset(ctx, usersetItems, req, commonParameters, recursiveMatchUserUserset)
}

type usersetMessage struct {
	userset string
	err     error
}

// streamedLookupUsersetForUser streams the userset (req's object#relation) that are assigned to
// the user to the usersetMessageChan channel.
func streamedLookupUsersetForUser(ctx context.Context,
	commonParameters *recursiveMatchUserUsersetCommonData,
	req *ResolveCheckRequest,
) chan usersetMessage {
	ctx, span := tracer.Start(ctx, "streamedLookupUsersetForUser")
	defer span.End()

	usersetMessageChan := make(chan usersetMessage, commonParameters.concurrencyLimit)

	go func() {
		defer func() {
			close(usersetMessageChan)
		}()

		// Note that if the type of the request's user is publicly assignable, this will fetch that tuple as well.
		iter, err := checkutil.IteratorReadStartingFromUser(ctx,
			commonParameters.typesys, commonParameters.ds, req,
			tuple.ToObjectRelationString(tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation()),
			nil)
		if err != nil {
			span.RecordError(err)
			concurrency.TrySendThroughChannel(ctx, usersetMessage{
				userset: "",
				err:     err,
			}, usersetMessageChan)
			return
		}

		filteredIter := storage.NewConditionsFilteredTupleKeyIterator(
			storage.NewFilteredTupleKeyIterator(
				storage.NewTupleKeyIteratorFromTupleIterator(iter),
				validation.FilterInvalidTuples(commonParameters.typesys),
			),
			checkutil.BuildTupleKeyConditionFilter(ctx, req.GetContext(), commonParameters.typesys),
		)
		defer filteredIter.Stop()

		for {
			t, err := filteredIter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}

				// error encountered.  No need to process further
				span.RecordError(err)
				concurrency.TrySendThroughChannel(ctx, usersetMessage{
					userset: "",
					err:     err,
				}, usersetMessageChan)
				return
			}
			concurrency.TrySendThroughChannel(ctx, usersetMessage{
				userset: t.GetObject(),
				err:     nil,
			}, usersetMessageChan)
		}
	}()
	return usersetMessageChan
}

// streamedLookupUsersetForObject streams the userset that are assigned to
// the object to the usersetMessageChan channel.
func streamedLookupUsersetForObject(ctx context.Context, commonParameters *recursiveMatchUserUsersetCommonData, tupleMapper TupleMapper) chan usersetMessage {
	ctx, span := tracer.Start(ctx, "streamedLookupUsersetForObject")
	defer span.End()

	usersetMessageChan := make(chan usersetMessage, commonParameters.concurrencyLimit)

	go func() {
		defer func() {
			tupleMapper.Stop()
			close(usersetMessageChan)
		}()

		for {
			res, err := tupleMapper.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					break
				}

				span.RecordError(err)
				concurrency.TrySendThroughChannel(ctx, usersetMessage{
					userset: "",
					err:     err,
				}, usersetMessageChan)
				return
			}
			concurrency.TrySendThroughChannel(ctx, usersetMessage{
				userset: res,
				err:     nil,
			}, usersetMessageChan)
		}
	}()
	return usersetMessageChan
}

// processUsersetMessage will add the message's userset in the primarySortedSet.
// In addition, it returns whether the message's userset exists in secondarySortedSet.
// This is used to find the intersection between userset from user and userset from object.
func processUsersetMessage(message usersetMessage,
	primarySortedSet storage.SortedSet,
	secondarySortedSet storage.SortedSet) (bool, error) {
	if message.err != nil {
		return false, message.err
	}
	primarySortedSet.Add(message.userset)
	return secondarySortedSet.Exists(message.userset), nil
}

func matchUsersetFromUserAndUsersetFromObject(ctx context.Context,
	req *ResolveCheckRequest,
	userToUsersetMessageChan,
	objectToUsersetMessageChan chan usersetMessage) (*ResolveCheckResponse, storage.SortedSet, storage.SortedSet, error) {
	usersetFromUser := storage.NewSortedSet()
	usersetFromObject := storage.NewSortedSet()

	userToUsersetDone := false
	objectToUsersetDone := false

	for !userToUsersetDone || !objectToUsersetDone {
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		case userToUsersetMessage, ok := <-userToUsersetMessageChan:
			if !ok {
				userToUsersetDone = true
				if usersetFromUser.Size() == 0 {
					return &ResolveCheckResponse{
						Allowed: false,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							// It probably is +1.  However, we have no control on whether
							// the userset member lookup for the first level has started DS query.
							DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + 2,
						},
					}, nil, nil, nil
				}
			} else {
				found, err := processUsersetMessage(userToUsersetMessage, usersetFromUser, usersetFromObject)
				if err != nil {
					return nil, nil, nil, err
				}
				if found {
					return &ResolveCheckResponse{
						Allowed: true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + 2,
						},
					}, nil, nil, nil
				}
				//  check to see if there is a direct assignment
				if req.GetTupleKey().GetObject() == userToUsersetMessage.userset {
					return &ResolveCheckResponse{
						Allowed: true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + 2,
						},
					}, nil, nil, nil
				}
			}
		case objectToUsersetMessage, ok := <-objectToUsersetMessageChan:
			if !ok {
				objectToUsersetDone = true
				// note that if channel is closed and usersetFromObject is empty, it is still possible
				// for allowed:true IF there is a direct assignment. Therefore, we will need to wait
				// until userset has finished processing.
			} else {
				found, err := processUsersetMessage(objectToUsersetMessage, usersetFromObject, usersetFromUser)
				if err != nil {
					return nil, nil, nil, err
				}
				if found {
					return &ResolveCheckResponse{
						Allowed: true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							// It probably is +1.  However, we have no control on whether
							// the userset member lookup for the first level has started DS query.
							DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + 2,
						},
					}, nil, nil, nil
				}
			}
		}
	}
	if usersetFromObject.Size() == 0 {
		return &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				// It probably is +1.  However, we have no control on whether
				// the userset member lookup for the first level has started DS query.
				DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + 2,
			},
		}, nil, nil, nil
	}
	return nil, usersetFromUser, usersetFromObject, nil
}

func nestedUsersetFastpath(ctx context.Context,
	typesys *typesystem.TypeSystem,
	ds storage.RelationshipTupleReader,
	req *ResolveCheckRequest,
	mapperKind TupleMapperKind,
	allowedUserTypeRestrictions []*openfgav1.RelationReference,
	concurrencyLimit int) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "nestedUsersetFastpath")
	defer span.End()

	cancellable, cancel := context.WithCancel(ctx)
	defer cancel()

	dsCount := &atomic.Uint32{}
	dsCount.Store(2)

	recursiveCommonData := &recursiveMatchUserUsersetCommonData{
		typesys:                     typesys,
		ds:                          ds,
		dsCount:                     dsCount,
		userToUsersetMapping:        nil, // we don't know the userToUsersetMapping yet
		concurrencyLimit:            concurrencyLimit,
		tupleMapperKind:             mapperKind,
		allowedUserTypeRestrictions: allowedUserTypeRestrictions,
		visitedUserset:              &sync.Map{},
	}

	userToUsersetMessageChan := streamedLookupUsersetForUser(cancellable, recursiveCommonData, req)

	mapper, err := buildMapper(ctx, req, recursiveCommonData)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	objectToUsersetMessageChan := streamedLookupUsersetForObject(cancellable, recursiveCommonData, mapper)

	resp, usersetFromUser, usersetFromObject, err := matchUsersetFromUserAndUsersetFromObject(cancellable, req, userToUsersetMessageChan, objectToUsersetMessageChan)

	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if resp != nil {
		return resp, nil
	}

	recursiveCommonData.userToUsersetMapping = usersetFromUser

	return parallelizeRecursiveMatchUserUserset(ctx, usersetFromObject.Values(), req, recursiveCommonData, recursiveMatchUserUserset)
}

func buildMapper(ctx context.Context, req *ResolveCheckRequest, common *recursiveMatchUserUsersetCommonData) (TupleMapper, error) {
	switch common.tupleMapperKind {
	case NestedUsersetKind:
		iter, err := common.ds.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
			Object:                      req.GetTupleKey().GetObject(),
			Relation:                    req.GetTupleKey().GetRelation(),
			AllowedUserTypeRestrictions: common.allowedUserTypeRestrictions,
		}, storage.ReadUsersetTuplesOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: req.GetConsistency(),
			},
		})
		if err != nil {
			return nil, err
		}
		filteredIter := storage.NewConditionsFilteredTupleKeyIterator(
			storage.NewFilteredTupleKeyIterator(
				storage.NewTupleKeyIteratorFromTupleIterator(iter),
				validation.FilterInvalidTuples(common.typesys),
			),
			checkutil.BuildTupleKeyConditionFilter(ctx, req.GetContext(), common.typesys),
		)
		return &NestedUsersetMapper{Iter: filteredIter}, nil
	case NestedTTUKind:
		panic("TODO not implemented")
	}

	return nil, fmt.Errorf("unsupported mapper kind %v", common.tupleMapperKind)
}

// checkDirect composes two CheckHandlerFunc which evaluate direct relationships with the provided
// 'object#relation'. The first handler looks up direct matches on the provided 'object#relation@user',
// while the second handler looks up relationships between the target 'object#relation' and any usersets
// related to it.
func (c *LocalChecker) checkDirect(parentctx context.Context, req *ResolveCheckRequest) CheckHandlerFunc {
	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		ctx, span := tracer.Start(ctx, "checkDirect")
		defer span.End()

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		typesys, _ := typesystem.TypesystemFromContext(parentctx) // note: use of 'parentctx' not 'ctx' - this is important

		ds, _ := storage.RelationshipTupleReaderFromContext(parentctx)

		storeID := req.GetStoreID()
		reqTupleKey := req.GetTupleKey()
		objectType := tuple.GetType(reqTupleKey.GetObject())
		relation := reqTupleKey.GetRelation()

		// directlyRelatedUsersetTypes could be "user:*" or "group#member"
		directlyRelatedUsersetTypes, _ := typesys.DirectlyRelatedUsersets(objectType, relation)

		// TODO(jpadilla): can we lift this function up?
		checkDirectUserTuple := func(ctx context.Context) (*ResolveCheckResponse, error) {
			ctx, span := tracer.Start(ctx, "checkDirectUserTuple",
				trace.WithAttributes(attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(reqTupleKey))))
			defer span.End()

			response := &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + 1,
				},
			}

			opts := storage.ReadUserTupleOptions{
				Consistency: storage.ConsistencyOptions{
					Preference: req.GetConsistency(),
				},
			}
			t, err := ds.ReadUserTuple(ctx, storeID, reqTupleKey, opts)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return response, nil
				}

				return nil, err
			}

			// filter out invalid tuples yielded by the database query
			tupleKey := t.GetKey()
			err = validation.ValidateTupleForRead(typesys, tupleKey)
			if err != nil {
				return response, nil
			}
			tupleKeyConditionFilter := checkutil.BuildTupleKeyConditionFilter(ctx, req.Context, typesys)
			conditionMet, err := tupleKeyConditionFilter(tupleKey)
			if err != nil {
				telemetry.TraceError(span, err)
				return nil, err
			}
			if conditionMet {
				span.SetAttributes(attribute.Bool("allowed", true))
				response.Allowed = true
				return response, nil
			}
			return response, nil
		}

		// TODO(jpadilla): can we lift this function up?
		checkDirectUsersetTuples := func(ctx context.Context) (*ResolveCheckResponse, error) {
			ctx, span := tracer.Start(ctx, "checkDirectUsersetTuples", trace.WithAttributes(attribute.String("userset", tuple.ToObjectRelationString(reqTupleKey.GetObject(), reqTupleKey.GetRelation()))))
			defer span.End()

			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			opts := storage.ReadUsersetTuplesOptions{
				Consistency: storage.ConsistencyOptions{
					Preference: req.GetConsistency(),
				},
			}
			iter, err := ds.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
				Object:                      reqTupleKey.GetObject(),
				Relation:                    reqTupleKey.GetRelation(),
				AllowedUserTypeRestrictions: directlyRelatedUsersetTypes,
			}, opts)
			if err != nil {
				return nil, err
			}

			filteredIter := storage.NewConditionsFilteredTupleKeyIterator(
				storage.NewFilteredTupleKeyIterator(
					storage.NewTupleKeyIteratorFromTupleIterator(iter),
					validation.FilterInvalidTuples(typesys),
				),
				checkutil.BuildTupleKeyConditionFilter(ctx, req.GetContext(), typesys),
			)
			defer filteredIter.Stop()
			resolver := c.checkUsersetSlowPath

			if !tuple.IsObjectRelation(reqTupleKey.GetUser()) {
				if typesys.UsersetCanFastPath(directlyRelatedUsersetTypes) {
					resolver = c.checkUsersetFastPath
				} else if c.optimizationsEnabled && typesys.RecursiveUsersetCanFastPath(
					tuple.ToObjectRelationString(tuple.GetType(reqTupleKey.GetObject()), reqTupleKey.GetRelation()),
					tuple.GetType(reqTupleKey.GetUser())) {
					return nestedUsersetFastpath(ctx, typesys, ds, req, NestedUsersetKind, directlyRelatedUsersetTypes, int(c.concurrencyLimit))
				}
			}

			return resolver(ctx, req, filteredIter)
		}

		var checkFuncs []CheckHandlerFunc

		shouldCheckDirectTuple, _ := typesys.IsDirectlyRelated(
			typesystem.DirectRelationReference(objectType, relation),                                                           // target
			typesystem.DirectRelationReference(tuple.GetType(reqTupleKey.GetUser()), tuple.GetRelation(reqTupleKey.GetUser())), // source
		)

		if shouldCheckDirectTuple {
			checkFuncs = []CheckHandlerFunc{checkDirectUserTuple}
		}

		if len(directlyRelatedUsersetTypes) > 0 {
			checkFuncs = append(checkFuncs, checkDirectUsersetTuples)
		}

		resp, err := union(ctx, c.concurrencyLimit, checkFuncs...)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}

		return resp, nil
	}
}

// checkComputedUserset evaluates the Check request with the rewritten relation (e.g. the computed userset relation).
func (c *LocalChecker) checkComputedUserset(_ context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset) CheckHandlerFunc {
	rewrittenTupleKey := tuple.NewTupleKey(
		req.GetTupleKey().GetObject(),
		rewrite.GetComputedUserset().GetRelation(),
		req.GetTupleKey().GetUser(),
	)

	childRequest := req.clone()
	childRequest.TupleKey = rewrittenTupleKey

	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		ctx, span := tracer.Start(ctx, "checkComputedUserset")
		defer span.End()
		// No dispatch here, as we don't want to increase resolution depth.
		return c.ResolveCheck(ctx, childRequest)
	}
}

func (c *LocalChecker) produceTTUDispatches(ctx context.Context, computedRelation string, req *ResolveCheckRequest, dispatches chan dispatchMsg, iter *storage.ConditionsFilteredTupleKeyIterator) {
	defer close(dispatches)
	reqTupleKey := req.GetTupleKey()
	typesys, _ := typesystem.TypesystemFromContext(ctx)

	for {
		t, err := iter.Next(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				break
			}
			concurrency.TrySendThroughChannel(ctx, dispatchMsg{err: err}, dispatches)
			break
		}

		userObj, _ := tuple.SplitObjectRelation(t.GetUser())
		if _, err := typesys.GetRelation(tuple.GetType(userObj), computedRelation); err != nil {
			if errors.Is(err, typesystem.ErrRelationUndefined) {
				continue // skip computed relations on tupleset relationships if they are undefined
			}
		}

		tupleKey := &openfgav1.TupleKey{
			Object:   userObj,
			Relation: computedRelation,
			User:     reqTupleKey.GetUser(),
		}

		concurrency.TrySendThroughChannel(ctx, dispatchMsg{dispatchParams: &dispatchParams{parentReq: req, tk: tupleKey}}, dispatches)
	}
}

// checkTTUSlowPath is the slow path for checkTTU where we cannot short-circuit TTU evaluation and
// resort to dispatch check on its children.
func (c *LocalChecker) checkTTUSlowPath(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter *storage.ConditionsFilteredTupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkTTUSlowPath")
	defer span.End()

	computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()

	dispatchChan := make(chan dispatchMsg, c.concurrencyLimit)

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	// sending to channel in batches up to a pre-configured value to subsequently checkMembership for.
	pool := concurrency.NewPool(cancellableCtx, 1)
	defer func() {
		cancelFunc()
		// We need to wait always to avoid a goroutine leak.
		_ = pool.Wait()
	}()
	pool.Go(func(ctx context.Context) error {
		c.produceTTUDispatches(ctx, computedRelation, req, dispatchChan, iter)
		return nil
	})

	resp, err := c.consumeDispatches(ctx, req, c.concurrencyLimit, dispatchChan)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	// read from checkTTU
	resp.GetResolutionMetadata().DatastoreQueryCount++

	return resp, nil
}

// checkTTUFastPath is the fast path for checkTTU where we can short-circuit TTU evaluation.
// This requires both the TTU's tuplesetRelation and computedRelation be exclusively directly assignable.
// The general idea is to check whether user has relation with the specified TTU by finding object
// intersection between tuplesetRelation's object AND objectType's computedRelation for user.  For example,
//
//	type group
//	  define member: [user]
//	type doc
//	  define parent: [group]
//	  define viewer: member from parent
//
// check(user, viewer, doc) will find the intersection of all group assigned to the doc's parent AND
// all group where the user is a member of.
func (c *LocalChecker) checkTTUFastPath(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter *storage.ConditionsFilteredTupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkTTUFastPath")
	defer span.End()
	// Caller already verified typesys
	typesys, _ := typesystem.TypesystemFromContext(ctx)

	computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()

	usersetDetails := checkutil.BuildUsersetDetailsTTU(typesys, computedRelation)
	return c.checkMembership(ctx, req, iter, usersetDetails)
}

// checkTTU looks up all tuples of the target tupleset relation on the provided object and for each one
// of them evaluates the computed userset of the TTU rewrite rule for them.
func (c *LocalChecker) checkTTU(parentctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset) CheckHandlerFunc {
	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		ctx, span := tracer.Start(ctx, "checkTTU")
		defer span.End()

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		typesys, _ := typesystem.TypesystemFromContext(parentctx) // note: use of 'parentctx' not 'ctx' - this is important

		ds, _ := storage.RelationshipTupleReaderFromContext(parentctx)

		ctx = typesystem.ContextWithTypesystem(ctx, typesys)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		tuplesetRelation := rewrite.GetTupleToUserset().GetTupleset().GetRelation()
		computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()

		tk := req.GetTupleKey()
		object := tk.GetObject()

		span.SetAttributes(
			attribute.String("tupleset_relation", fmt.Sprintf("%s#%s", tuple.GetType(object), tuplesetRelation)),
			attribute.String("computed_relation", computedRelation),
		)

		opts := storage.ReadOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: req.GetConsistency(),
			},
		}

		storeID := req.GetStoreID()
		iter, err := ds.Read(
			ctx,
			storeID,
			tuple.NewTupleKey(object, tuplesetRelation, ""),
			opts,
		)
		if err != nil {
			return nil, err
		}

		// filter out invalid tuples yielded by the database iterator
		filteredIter := storage.NewConditionsFilteredTupleKeyIterator(
			storage.NewFilteredTupleKeyIterator(
				storage.NewTupleKeyIteratorFromTupleIterator(iter),
				validation.FilterInvalidTuples(typesys),
			),
			checkutil.BuildTupleKeyConditionFilter(ctx, req.GetContext(), typesys),
		)
		defer filteredIter.Stop()

		resolver := c.checkTTUSlowPath

		// TODO: optimize the case where user is an userset.
		// If the user is a userset, we will not be able to use the shortcut because the algo
		// will look up the objects associated with user.
		if !tuple.IsObjectRelation(tk.GetUser()) {
			if canFastPath := typesys.TTUCanFastPath(
				tuple.GetType(object), tuplesetRelation, computedRelation); canFastPath {
				resolver = c.checkTTUFastPath
			}
		}
		return resolver(ctx, req, rewrite, filteredIter)
	}
}

func (c *LocalChecker) checkSetOperation(
	ctx context.Context,
	req *ResolveCheckRequest,
	setOpType setOperatorType,
	reducer CheckFuncReducer,
	children ...*openfgav1.Userset,
) CheckHandlerFunc {
	var handlers []CheckHandlerFunc

	var reducerKey string
	switch setOpType {
	case unionSetOperator, intersectionSetOperator, exclusionSetOperator:
		if setOpType == unionSetOperator {
			reducerKey = "union"
		}

		if setOpType == intersectionSetOperator {
			reducerKey = "intersection"
		}

		if setOpType == exclusionSetOperator {
			reducerKey = "exclusion"
		}

		for _, child := range children {
			handlers = append(handlers, c.checkRewrite(ctx, req, child))
		}
	default:
		return func(ctx context.Context) (*ResolveCheckResponse, error) {
			return nil, fmt.Errorf("%w: unexpected set operator type encountered", openfgaErrors.ErrUnknown)
		}
	}

	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		var err error
		var resp *ResolveCheckResponse
		ctx, span := tracer.Start(ctx, reducerKey)
		defer func() {
			if err != nil {
				telemetry.TraceError(span, err)
			}
			span.End()
		}()

		resp, err = reducer(ctx, c.concurrencyLimit, handlers...)
		return resp, err
	}
}

func (c *LocalChecker) checkRewrite(
	ctx context.Context,
	req *ResolveCheckRequest,
	rewrite *openfgav1.Userset,
) CheckHandlerFunc {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return c.checkDirect(ctx, req)
	case *openfgav1.Userset_ComputedUserset:
		return c.checkComputedUserset(ctx, req, rewrite)
	case *openfgav1.Userset_TupleToUserset:
		return c.checkTTU(ctx, req, rewrite)
	case *openfgav1.Userset_Union:
		return c.checkSetOperation(ctx, req, unionSetOperator, union, rw.Union.GetChild()...)
	case *openfgav1.Userset_Intersection:
		return c.checkSetOperation(ctx, req, intersectionSetOperator, intersection, rw.Intersection.GetChild()...)
	case *openfgav1.Userset_Difference:
		return c.checkSetOperation(ctx, req, exclusionSetOperator, exclusion, rw.Difference.GetBase(), rw.Difference.GetSubtract())
	default:
		return func(ctx context.Context) (*ResolveCheckResponse, error) {
			return nil, fmt.Errorf("%w: unexpected set operator type encountered", openfgaErrors.ErrUnknown)
		}
	}
}
