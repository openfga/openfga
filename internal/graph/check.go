package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/emirpasic/gods/sets/hashset"
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

var (
	ErrShortCircuit = errors.New("short circuit")
)

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

			if result.resp.GetAllowed() {
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
			CycleDetected: cycleDetected,
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

	var err error
	for i := 0; i < len(handlers); i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				telemetry.TraceError(span, result.err)
				err = errors.Join(err, result.err)
				continue
			}

			if result.resp.GetCycleDetected() || !result.resp.GetAllowed() {
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
		Allowed:            true,
		ResolutionMetadata: &ResolveCheckResponseMetadata{},
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
		Allowed:            false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{},
	}

	var baseErr error
	var subErr error

	for i := 0; i < len(handlers); i++ {
		select {
		case baseResult := <-baseChan:
			if baseResult.err != nil {
				telemetry.TraceError(span, baseResult.err)
				baseErr = baseResult.err
				continue
			}

			if baseResult.resp.GetCycleDetected() {
				return &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						CycleDetected: true,
					},
				}, nil
			}

			if !baseResult.resp.GetAllowed() {
				return response, nil
			}

		case subResult := <-subChan:
			if subResult.err != nil {
				telemetry.TraceError(span, subResult.err)
				subErr = subResult.err
				continue
			}

			if subResult.resp.GetCycleDetected() {
				return &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						CycleDetected: true,
					},
				}, nil
			}

			if subResult.resp.GetAllowed() {
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
		Allowed:            true,
		ResolutionMetadata: &ResolveCheckResponseMetadata{},
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
			Allowed:            true,
			ResolutionMetadata: &ResolveCheckResponseMetadata{},
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

	iter, err := checkutil.IteratorReadStartingFromUser(ctx, typesys, ds, req, objectRel, objectIDs)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	defer iter.Stop()

	allowed, err := checkutil.ObjectIDInSortedSet(ctx, iter, objectIDs)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}
	if allowed {
		span.SetAttributes(attribute.Bool("allowed", true))
	}

	return &ResolveCheckResponse{
		Allowed:            allowed,
		ResolutionMetadata: &ResolveCheckResponseMetadata{},
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

func (c *LocalChecker) produceUsersetDispatches(ctx context.Context, req *ResolveCheckRequest, dispatches chan dispatchMsg, iter storage.TupleKeyIterator) {
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
						Allowed:            true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{},
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

func (c *LocalChecker) consumeDispatches(ctx context.Context, limit uint32, dispatchChan chan dispatchMsg) (*ResolveCheckResponse, error) {
	cancellableCtx, cancel := context.WithCancel(ctx)
	outcomeChannel := c.processDispatches(cancellableCtx, limit, dispatchChan)

	var finalErr error
	finalResult := &ResolveCheckResponse{
		Allowed:            false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{},
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

			if outcome.resp.Allowed {
				finalErr = nil
				finalResult = outcome.resp
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

// checkUsersetSlowPath will check userset path.
// This is the slow path as it requires dispatch on all its children.
func (c *LocalChecker) checkUsersetSlowPath(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkUsersetSlowPath")
	defer span.End()

	dispatchChan := make(chan dispatchMsg, c.concurrencyLimit)

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
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

	resp, err := c.consumeDispatches(ctx, c.concurrencyLimit, dispatchChan)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

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
func (c *LocalChecker) checkUsersetFastPath(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
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
func (c *LocalChecker) checkMembership(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator, usersetDetails checkutil.UsersetDetailsFunc) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkMembership")
	defer span.End()

	// all at least 1 userset to queue up
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
		Allowed:            false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{},
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

			if outcome.resp.Allowed {
				finalErr = nil
				finalResult = outcome.resp
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

func (c *LocalChecker) produceUsersets(ctx context.Context, usersetsChan chan usersetsChannelType, iter storage.TupleKeyIterator, usersetDetails checkutil.UsersetDetailsFunc) {
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

// Note that visited does not necessary means that there are cycles.  For the following model,
// type user
// type group
//
//	relations
//	  define member: [user, group#member]
//
// We have something like
// group:1#member@group:2#member
// group:1#member@group:3#member
// group:2#member@group:a#member
// group:3#member@group:a#member
// Note that both group:2#member and group:3#member has group:a#member. However, they are not cycles.

func (c *LocalChecker) breadthFirstNestedMatch(ctx context.Context, req *ResolveCheckRequest, mapping *nestedMapping, visitedUserset *sync.Map, currentUsersetLevel *hashset.Set, usersetFromUser *hashset.Set, checkOutcomeChan chan checkOutcome) {
	req.GetRequestMetadata().Depth--
	if req.GetRequestMetadata().Depth == 0 {
		concurrency.TrySendThroughChannel(ctx, checkOutcome{err: ErrResolutionDepthExceeded}, checkOutcomeChan)
		close(checkOutcomeChan)
		return
	}
	if currentUsersetLevel.Size() == 0 || ctx.Err() != nil {
		// nothing else to search for or upstream cancellation
		close(checkOutcomeChan)
		return
	}

	pool := concurrency.NewPool(ctx, int(c.concurrencyLimit))

	mu := &sync.Mutex{}
	nextUsersetLevel := hashset.New()

	relation := req.GetTupleKey().GetRelation()
	user := req.GetTupleKey().GetUser()

	for _, usersetInterface := range currentUsersetLevel.Values() {
		userset := usersetInterface.(string)
		_, visited := visitedUserset.LoadOrStore(userset, struct{}{})
		if visited {
			continue
		}
		newReq := req.clone()
		newReq.TupleKey = tuple.NewTupleKey(userset, relation, user)
		mapper, err := c.buildNestedMapper(ctx, newReq, mapping)

		if err != nil {
			concurrency.TrySendThroughChannel(ctx, checkOutcome{err: err}, checkOutcomeChan)
			// TODO: TBD if we hard exit here, if yes, the channel needs to be closed
			continue
		}
		// if the pool is short-circuited, the iterator should be stopped
		defer mapper.Stop()
		pool.Go(func(ctx context.Context) error {
			objectToUsersetMessageChan := streamedLookupUsersetFromIterator(ctx, mapper)
			for usersetMsg := range objectToUsersetMessageChan {
				if usersetMsg.err != nil {
					concurrency.TrySendThroughChannel(ctx, checkOutcome{err: usersetMsg.err}, checkOutcomeChan)
					return nil
				}
				userset := usersetMsg.userset
				if usersetFromUser.Contains(userset) {
					concurrency.TrySendThroughChannel(ctx, checkOutcome{resp: &ResolveCheckResponse{
						Allowed:            true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{},
					}}, checkOutcomeChan)
					return ErrShortCircuit // cancel will be propagated to the remaining goroutines
				}
				mu.Lock()
				nextUsersetLevel.Add(userset)
				mu.Unlock()
			}
			return nil
		})
	}
	// wait for all checks to wrap up
	// if a match was found, clean up
	if err := pool.Wait(); err != nil && errors.Is(err, ErrShortCircuit) {
		close(checkOutcomeChan)
		return
	}

	concurrency.TrySendThroughChannel(ctx, checkOutcome{resp: &ResolveCheckResponse{
		Allowed:            false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{},
	}}, checkOutcomeChan)
	c.breadthFirstNestedMatch(ctx, req, mapping, visitedUserset, nextUsersetLevel, usersetFromUser, checkOutcomeChan)
}

func (c *LocalChecker) recursiveMatchUserUserset(ctx context.Context, req *ResolveCheckRequest, mapping *nestedMapping, currentLevelFromObject *hashset.Set, usersetFromUser *hashset.Set) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "recursiveMatchUserUserset", trace.WithAttributes(
		attribute.Int("first_level_size", currentLevelFromObject.Size()),
		attribute.Int("terminal_type_size", usersetFromUser.Size()),
	))
	defer span.End()
	checkOutcomeChan := make(chan checkOutcome, c.concurrencyLimit)

	cancellableCtx, cancel := context.WithCancel(ctx)
	pool := concurrency.NewPool(cancellableCtx, 1)
	defer func() {
		cancel()
		// We need to wait always to avoid a goroutine leak.
		_ = pool.Wait()
	}()
	pool.Go(func(ctx context.Context) error {
		c.breadthFirstNestedMatch(ctx, req, mapping, &sync.Map{}, currentLevelFromObject, usersetFromUser, checkOutcomeChan)
		return nil
	})

	var finalErr error
	finalResult := &ResolveCheckResponse{
		Allowed:            false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{},
	}

ConsumerLoop:
	for {
		select {
		case <-ctx.Done():
			break ConsumerLoop
		case outcome, ok := <-checkOutcomeChan:
			if !ok {
				break ConsumerLoop
			}
			if outcome.err != nil {
				finalErr = outcome.err
				break // continue
			}

			if outcome.resp.Allowed {
				finalErr = nil
				finalResult = outcome.resp
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

type usersetMessage struct {
	userset string
	err     error
}

// streamedLookupUsersetFromIterator streams the userset that are assigned to
// the object to the usersetMessageChan channel.
func streamedLookupUsersetFromIterator(ctx context.Context, tupleMapper TupleMapper) chan usersetMessage {
	ctx, span := tracer.Start(ctx, "streamedLookupUsersetFromIterator")
	usersetMessageChan := make(chan usersetMessage, 100)

	go func() {
		defer func() {
			close(usersetMessageChan)
			span.End()
		}()

		for {
			res, err := tupleMapper.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					return
				}
				telemetry.TraceError(span, err)
				concurrency.TrySendThroughChannel(ctx, usersetMessage{err: err}, usersetMessageChan)
				return
			}
			concurrency.TrySendThroughChannel(ctx, usersetMessage{userset: res}, usersetMessageChan)
		}
	}()
	return usersetMessageChan
}

// processUsersetMessage will add the userset in the primarySet.
// In addition, it returns whether the userset exists in secondarySet.
// This is used to find the intersection between userset from user and userset from object.
func processUsersetMessage(userset string,
	primarySet *hashset.Set,
	secondarySet *hashset.Set) bool {
	primarySet.Add(userset)
	return secondarySet.Contains(userset)
}

type nestedMapping struct {
	kind                        TupleMapperKind
	tuplesetRelation            string
	allowedUserTypeRestrictions []*openfgav1.RelationReference
}

func (c *LocalChecker) nestedFastPath(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator, mapping *nestedMapping) (*ResolveCheckResponse, error) {
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)

	usersetFromUser := hashset.New()
	usersetFromObject := hashset.New()

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	objectToUsersetIter := wrapIterator(mapping.kind, iter)
	defer objectToUsersetIter.Stop()
	objectToUsersetMessageChan := streamedLookupUsersetFromIterator(cancellableCtx, objectToUsersetIter)

	res := &ResolveCheckResponse{
		Allowed:            false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{},
	}

	// check to see if there are any nested userset assigned. If not,
	// we don't even need to check the terminal type side.
	objectToUsersetMessage, ok := <-objectToUsersetMessageChan
	if !ok {
		return res, nil
	}
	if objectToUsersetMessage.err != nil {
		return nil, objectToUsersetMessage.err
	}
	usersetFromObject.Add(objectToUsersetMessage.userset)

	userIter, err := checkutil.IteratorReadStartingFromUser(ctx, typesys, ds, req,
		tuple.ToObjectRelationString(tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation()),
		nil)
	if err != nil {
		return nil, err
	}
	usersetFromUserIter := wrapIterator(ObjectIDKind, userIter)
	defer usersetFromUserIter.Stop()
	userToUsersetMessageChan := streamedLookupUsersetFromIterator(cancellableCtx, usersetFromUserIter)

	userToUsersetDone := false
	objectToUsersetDone := false

	// NOTE: This loop initializes the terminal type and the first level of depth as this is a breadth first traversal.
	// To maintain simplicity the terminal type will be fully loaded, but it could arguably be loaded async.
	for !userToUsersetDone || !objectToUsersetDone {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case userToUsersetMessage, ok := <-userToUsersetMessageChan:
			if !ok {
				userToUsersetDone = true
				if usersetFromUser.Size() == 0 {
					return res, nil
				}
				break
			}
			if userToUsersetMessage.err != nil {
				return nil, userToUsersetMessage.err
			}
			if processUsersetMessage(userToUsersetMessage.userset, usersetFromUser, usersetFromObject) {
				res.Allowed = true
				return res, nil
			}
		case objectToUsersetMessage, ok := <-objectToUsersetMessageChan:
			if !ok {
				// usersetFromObject must not be empty because we would have caught it earlier.
				objectToUsersetDone = true
				break
			}
			if objectToUsersetMessage.err != nil {
				return nil, objectToUsersetMessage.err
			}
			if processUsersetMessage(objectToUsersetMessage.userset, usersetFromObject, usersetFromUser) {
				res.Allowed = true
				return res, nil
			}
		}
	}

	newReq := req.clone()
	return c.recursiveMatchUserUserset(ctx, newReq, mapping, usersetFromObject, usersetFromUser)
}

func (c *LocalChecker) nestedTTUFastPath(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "nestedTTUFastPath")
	defer span.End()

	return c.nestedFastPath(ctx, req, iter, &nestedMapping{
		kind:             NestedTTUKind,
		tuplesetRelation: rewrite.GetTupleToUserset().GetTupleset().GetRelation(),
	})
}

func (c *LocalChecker) nestedUsersetFastPath(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "nestedUsersetFastPath")
	defer span.End()

	typesys, _ := typesystem.TypesystemFromContext(ctx)

	directlyRelatedUsersetTypes, _ := typesys.DirectlyRelatedUsersets(tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation())
	return c.nestedFastPath(ctx, req, iter, &nestedMapping{
		kind:                        NestedUsersetKind,
		allowedUserTypeRestrictions: directlyRelatedUsersetTypes,
	})
}

func (c *LocalChecker) buildNestedMapper(ctx context.Context, req *ResolveCheckRequest, mapping *nestedMapping) (TupleMapper, error) {
	var iter storage.TupleIterator
	var err error
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)
	consistencyOpts := storage.ConsistencyOptions{
		Preference: req.GetConsistency(),
	}
	switch mapping.kind {
	case NestedUsersetKind:
		iter, err = ds.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
			Object:                      req.GetTupleKey().GetObject(),
			Relation:                    req.GetTupleKey().GetRelation(),
			AllowedUserTypeRestrictions: mapping.allowedUserTypeRestrictions,
		}, storage.ReadUsersetTuplesOptions{Consistency: consistencyOpts})
	case NestedTTUKind:
		iter, err = ds.Read(ctx, req.GetStoreID(), tuple.NewTupleKey(req.GetTupleKey().GetObject(), mapping.tuplesetRelation, ""),
			storage.ReadOptions{Consistency: consistencyOpts})
	default:
		return nil, errors.New("unsupported mapper kind")
	}
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
	return wrapIterator(mapping.kind, filteredIter), nil
}

func (c *LocalChecker) checkPublicAssignable(ctx context.Context, req *ResolveCheckRequest) CheckHandlerFunc {
	ctx, span := tracer.Start(ctx, "checkPublicAssignable")
	defer span.End()

	typesys, _ := typesystem.TypesystemFromContext(ctx)

	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)
	storeID := req.GetStoreID()
	reqTupleKey := req.GetTupleKey()
	userType := tuple.GetType(reqTupleKey.GetUser())
	wildcardRelationReference := typesystem.WildcardRelationReference(userType)
	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		response := &ResolveCheckResponse{
			Allowed:            false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{},
		}

		opts := storage.ReadUsersetTuplesOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: req.GetConsistency(),
			},
		}

		iter, err := ds.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:                      reqTupleKey.GetObject(),
			Relation:                    reqTupleKey.GetRelation(),
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{wildcardRelationReference},
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

		_, err = filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				return response, nil
			}
			return nil, err
		}
		// when we get to here, it means there is public wild card assigned
		span.SetAttributes(attribute.Bool("allowed", true))
		response.Allowed = true
		return response, nil
	}
}

func (c *LocalChecker) checkDirectUserTuple(ctx context.Context, req *ResolveCheckRequest) CheckHandlerFunc {
	typesys, _ := typesystem.TypesystemFromContext(ctx)

	reqTupleKey := req.GetTupleKey()

	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		ctx, span := tracer.Start(ctx, "checkDirectUserTupleWrapper",
			trace.WithAttributes(attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(reqTupleKey))))
		defer span.End()

		response := &ResolveCheckResponse{
			Allowed:            false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{},
		}

		ds, _ := storage.RelationshipTupleReaderFromContext(ctx)
		storeID := req.GetStoreID()

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
		}
		return response, nil
	}
}

// helper function to return whether checkDirectUserTuple should run.
func shouldCheckDirectTuple(ctx context.Context, reqTupleKey *openfgav1.TupleKey) bool {
	typesys, _ := typesystem.TypesystemFromContext(ctx)

	objectType := tuple.GetType(reqTupleKey.GetObject())
	relation := reqTupleKey.GetRelation()

	isDirectlyRelated, _ := typesys.IsDirectlyRelated(
		typesystem.DirectRelationReference(objectType, relation),                                                           // target
		typesystem.DirectRelationReference(tuple.GetType(reqTupleKey.GetUser()), tuple.GetRelation(reqTupleKey.GetUser())), // source
	)

	return isDirectlyRelated
}

// helper function to return whether checkPublicAssignable should run.
func shouldCheckPublicAssignable(ctx context.Context, reqTupleKey *openfgav1.TupleKey) bool {
	typesys, _ := typesystem.TypesystemFromContext(ctx)

	objectType := tuple.GetType(reqTupleKey.GetObject())
	relation := reqTupleKey.GetRelation()

	isPubliclyAssignable, _ := typesys.IsPubliclyAssignable(
		typesystem.DirectRelationReference(objectType, relation), // target
		tuple.GetType(reqTupleKey.GetUser()),
	)
	return isPubliclyAssignable
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

			resolver := c.checkUsersetSlowPath

			if !tuple.IsObjectRelation(reqTupleKey.GetUser()) {
				if typesys.UsersetCanFastPath(directlyRelatedUsersetTypes) {
					resolver = c.checkUsersetFastPath
				} else if c.optimizationsEnabled && typesys.RecursiveUsersetCanFastPath(
					tuple.ToObjectRelationString(tuple.GetType(reqTupleKey.GetObject()), reqTupleKey.GetRelation()),
					tuple.GetType(reqTupleKey.GetUser())) {
					resolver = c.nestedUsersetFastPath
				}
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

			return resolver(ctx, req, filteredIter)
		}

		var checkFuncs []CheckHandlerFunc

		if shouldCheckDirectTuple(ctx, req.GetTupleKey()) {
			checkFuncs = []CheckHandlerFunc{c.checkDirectUserTuple(parentctx, req)}
		}

		if shouldCheckPublicAssignable(ctx, reqTupleKey) {
			checkFuncs = append(checkFuncs, c.checkPublicAssignable(parentctx, req))
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

func (c *LocalChecker) produceTTUDispatches(ctx context.Context, computedRelation string, req *ResolveCheckRequest, dispatches chan dispatchMsg, iter storage.TupleKeyIterator) {
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
func (c *LocalChecker) checkTTUSlowPath(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
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

	resp, err := c.consumeDispatches(ctx, c.concurrencyLimit, dispatchChan)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

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
func (c *LocalChecker) checkTTUFastPath(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
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

		objectType, relation := tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation()
		objectTypeRelation := tuple.ToObjectRelationString(objectType, relation)

		userType := tuple.GetType(req.GetTupleKey().GetUser())

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
		if c.optimizationsEnabled && typesys.RecursiveTTUCanFastPath(objectTypeRelation, userType) {
			resolver = c.nestedTTUFastPath
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
