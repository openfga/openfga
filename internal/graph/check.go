package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/structpb"

	openfgaErrors "github.com/openfga/openfga/internal/errors"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var tracer = otel.Tracer("internal/graph/check")

type ResolveCheckRequest struct {
	StoreID              string
	AuthorizationModelID string
	TupleKey             *openfgav1.TupleKey
	ContextualTuples     []*openfgav1.TupleKey
	Context              *structpb.Struct
	RequestMetadata      *ResolveCheckRequestMetadata
	VisitedPaths         map[string]struct{}
	Consistency          openfgav1.ConsistencyPreference
}

func clone(r *ResolveCheckRequest) *ResolveCheckRequest {
	return &ResolveCheckRequest{
		StoreID:              r.StoreID,
		AuthorizationModelID: r.AuthorizationModelID,
		TupleKey:             r.TupleKey,
		ContextualTuples:     r.ContextualTuples,
		Context:              r.Context,
		RequestMetadata: &ResolveCheckRequestMetadata{
			DispatchCounter:     r.GetRequestMetadata().DispatchCounter,
			Depth:               r.GetRequestMetadata().Depth,
			DatastoreQueryCount: r.GetRequestMetadata().DatastoreQueryCount,
			WasThrottled:        r.GetRequestMetadata().WasThrottled,
		},
		VisitedPaths: maps.Clone(r.VisitedPaths),
		Consistency:  r.Consistency,
	}
}

// CloneResolveCheckResponse clones the provided ResolveCheckResponse.
//
// If 'r' defines a nil ResolutionMetadata then this function returns
// an empty value struct for the resolution metadata instead of nil.
func CloneResolveCheckResponse(r *ResolveCheckResponse) *ResolveCheckResponse {
	resolutionMetadata := &ResolveCheckResponseMetadata{
		DatastoreQueryCount: 0,
		CycleDetected:       false,
	}

	if r.GetResolutionMetadata() != nil {
		resolutionMetadata.DatastoreQueryCount = r.GetResolutionMetadata().DatastoreQueryCount
		resolutionMetadata.CycleDetected = r.GetResolutionMetadata().CycleDetected
	}

	return &ResolveCheckResponse{
		Allowed:            r.GetAllowed(),
		ResolutionMetadata: resolutionMetadata,
	}
}

type ResolveCheckResponse struct {
	Allowed            bool
	ResolutionMetadata *ResolveCheckResponseMetadata
}

func (r *ResolveCheckResponse) GetCycleDetected() bool {
	if r != nil {
		return r.GetResolutionMetadata().CycleDetected
	}

	return false
}

func (r *ResolveCheckResponse) GetAllowed() bool {
	if r != nil {
		return r.Allowed
	}

	return false
}

func (r *ResolveCheckResponse) GetResolutionMetadata() *ResolveCheckResponseMetadata {
	if r != nil {
		return r.ResolutionMetadata
	}

	return nil
}

func (r *ResolveCheckRequest) GetStoreID() string {
	if r != nil {
		return r.StoreID
	}

	return ""
}

func (r *ResolveCheckRequest) GetAuthorizationModelID() string {
	if r != nil {
		return r.AuthorizationModelID
	}

	return ""
}

func (r *ResolveCheckRequest) GetTupleKey() *openfgav1.TupleKey {
	if r != nil {
		return r.TupleKey
	}

	return nil
}

func (r *ResolveCheckRequest) GetContextualTuples() []*openfgav1.TupleKey {
	if r != nil {
		return r.ContextualTuples
	}

	return nil
}

func (r *ResolveCheckRequest) GetRequestMetadata() *ResolveCheckRequestMetadata {
	if r != nil {
		return r.RequestMetadata
	}

	return nil
}

func (r *ResolveCheckRequest) GetContext() *structpb.Struct {
	if r != nil {
		return r.Context
	}
	return nil
}

func (r *ResolveCheckRequest) GetConsistency() openfgav1.ConsistencyPreference {
	if r != nil {
		return r.Consistency
	}
	return openfgav1.ConsistencyPreference_UNSPECIFIED
}

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
	delegate           CheckResolver
	concurrencyLimit   uint32
	maxConcurrentReads uint32
}

type LocalCheckerOption func(d *LocalChecker)

// WithResolveNodeBreadthLimit see server.WithResolveNodeBreadthLimit.
func WithResolveNodeBreadthLimit(limit uint32) LocalCheckerOption {
	return func(d *LocalChecker) {
		d.concurrencyLimit = limit
	}
}

// WithMaxConcurrentReads see server.WithMaxConcurrentReadsForCheck.
func WithMaxConcurrentReads(limit uint32) LocalCheckerOption {
	return func(d *LocalChecker) {
		d.maxConcurrentReads = limit
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
		childRequest := clone(parentReq)
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
		attribute.String("tuple_key", req.GetTupleKey().String()),
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

	userObject, userRelation := tuple.SplitObjectRelation(req.GetTupleKey().GetUser())

	// Check(document:1#viewer@document:1#viewer) will always return true
	if relation == userRelation && object == userObject {
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

func (c *LocalChecker) buildCheckAssociatedObjects(req *ResolveCheckRequest, objectRel string, objectIDs storage.SortedSet) CheckHandlerFunc {
	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		ctx, span := tracer.Start(ctx, "checkAssociatedObjects")
		defer span.End()

		typesys, ok := typesystem.TypesystemFromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
		}

		ds, ok := storage.RelationshipTupleReaderFromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("relationship tuple reader datastore missing in context")
		}

		storeID := req.GetStoreID()
		reqTupleKey := req.GetTupleKey()
		reqContext := req.GetContext()

		response := &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + 1,
			},
		}
		objectType, relation := tuple.SplitObjectRelation(objectRel)

		user := reqTupleKey.GetUser()
		userType := tuple.GetType(user)

		relationReference := typesystem.DirectRelationReference(objectType, relation)
		hasPubliclyAssignedType, _ := typesys.IsPubliclyAssignable(relationReference, userType)

		userFilter := []*openfgav1.ObjectRelation{{
			Object: user,
		}}
		if hasPubliclyAssignedType {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: tuple.TypedPublicWildcard(userType),
			})
		}

		opts := storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: req.GetConsistency(),
			},
		}

		i, err := ds.ReadStartingWithUser(ctx, storeID, storage.ReadStartingWithUserFilter{
			ObjectType: objectType,
			Relation:   relation,
			UserFilter: userFilter,
			ObjectIDs:  objectIDs,
		}, opts)

		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}
		// filter out invalid tuples yielded by the database iterator
		filteredIter := storage.NewConditionsFilteredTupleKeyIterator(
			storage.NewFilteredTupleKeyIterator(
				storage.NewTupleKeyIteratorFromTupleIterator(i),
				validation.FilterInvalidTuples(typesys),
			),
			buildTupleKeyConditionFilter(ctx, reqContext, typesys),
		)
		defer filteredIter.Stop()

		for {
			t, err := filteredIter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				telemetry.TraceError(span, err)
				return nil, err
			}

			_, objectID := tuple.SplitObject(t.GetObject())
			if objectIDs.Exists(objectID) {
				span.SetAttributes(attribute.Bool("allowed", true))
				response.Allowed = true
				return response, nil
			}
		}
		return response, nil
	}
}

// checkUsersetSlowPath will check userset or public wildcard path.
// This is the slow path as it requires dispatch on all its children.
func (c *LocalChecker) checkUsersetSlowPath(ctx context.Context, iter *storage.ConditionsFilteredTupleKeyIterator, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkUsersetSlowPath")
	defer span.End()
	var handlers []CheckHandlerFunc

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
	}

	response := &ResolveCheckResponse{
		Allowed: false,
		ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: req.GetRequestMetadata().DatastoreQueryCount + 1,
		},
	}

	for {
		t, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			telemetry.TraceError(span, err)
			return nil, err
		}

		usersetObject, usersetRelation := tuple.SplitObjectRelation(t.GetUser())
		reqTupleKey := req.GetTupleKey()

		// if the user value is a typed wildcard and the type of the wildcard
		// matches the target user objectType, then we're done searching
		if tuple.IsTypedWildcard(usersetObject) && typesystem.IsSchemaVersionSupported(typesys.GetSchemaVersion()) {
			wildcardType := tuple.GetType(usersetObject)

			if tuple.GetType(reqTupleKey.GetUser()) == wildcardType {
				span.SetAttributes(attribute.Bool("allowed", true))
				response.Allowed = true
				return response, nil
			}

			continue
		}

		if usersetRelation != "" {
			tupleKey := tuple.NewTupleKey(usersetObject, usersetRelation, reqTupleKey.GetUser())
			handlers = append(handlers, c.dispatch(ctx, req, tupleKey))
		}
	}

	resp, err := union(ctx, c.concurrencyLimit, handlers...)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	resp.GetResolutionMetadata().DatastoreQueryCount += response.GetResolutionMetadata().DatastoreQueryCount

	return resp, nil
}

func buildTupleKeyConditionFilter(ctx context.Context, reqCtx *structpb.Struct, typesys *typesystem.TypeSystem) storage.TupleKeyConditionFilterFunc {
	return func(t *openfgav1.TupleKey) (bool, error) {
		condEvalResult, err := eval.EvaluateTupleCondition(ctx, t, typesys, reqCtx)
		if err != nil {
			return false, err
		}

		if len(condEvalResult.MissingParameters) > 0 {
			return false, condition.NewEvaluationError(
				t.GetCondition().GetName(),
				fmt.Errorf("tuple '%s' is missing context parameters '%v'",
					tuple.TupleKeyToString(t),
					condEvalResult.MissingParameters),
			)
		}

		return condEvalResult.ConditionMet, nil
	}
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
func (c *LocalChecker) checkUsersetFastPath(ctx context.Context, iter *storage.ConditionsFilteredTupleKeyIterator, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkUsersetFastPath")
	defer span.End()

	typesys, _ := typesystem.TypesystemFromContext(ctx)
	// We had just checked for the existence of the typesys in the caller.
	// So, we are guaranteed for the presence of the context.

	reqUserType := tuple.GetType(req.GetTupleKey().GetUser())

	usersetsMap := make(usersetsMapType)

	for {
		// NOTE: For the future, once we observe a new ObjectRelation in the usersetsMap that means that all objectIDs
		// needed for the intersection lookup for that ObjectRelation are in memory. There is no need to build the full
		// usersetMap by draining the full iterator, it can start processing in batches of ObjectRelation.
		// Consider doing it when we have better concurrency tooling for "streaming" operations.
		t, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			telemetry.TraceError(span, err)
			return nil, err
		}

		object, relation := tuple.SplitObjectRelation(t.GetUser())
		objectType, objectID := tuple.SplitObject(object)
		terminalRelations := typesys.GetTerminalRelations(objectType, relation, reqUserType)
		// the terminalRelations is expected to be 1 (as we checked earlier in typesys.UsersetCanFastPath)
		if len(terminalRelations) != 1 {
			return nil, fmt.Errorf("expected exactly one terminal relation for fast path, received %d", len(terminalRelations))
		}
		computedRelation := terminalRelations[0]
		objectRel := tuple.ToObjectRelationString(objectType, computedRelation)
		if _, ok := usersetsMap[objectRel]; !ok {
			usersetsMap[objectRel] = storage.NewSortedSet()
		}
		usersetsMap[objectRel].Add(objectID)
	}

	// Next, for all the ObjectRelation, compare the associated objectIDs
	// to the users associated objects
	// all of this can likely bee its own function
	handlers := make([]CheckHandlerFunc, 0, len(usersetsMap))
	for objectRel, objectIDs := range usersetsMap {
		handler := c.buildCheckAssociatedObjects(req, objectRel, objectIDs)
		handlers = append(handlers, handler)
	}
	resp, err := union(ctx, c.concurrencyLimit, handlers...)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	resp.GetResolutionMetadata().DatastoreQueryCount++

	return resp, nil
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

		typesys, ok := typesystem.TypesystemFromContext(parentctx) // note: use of 'parentctx' not 'ctx' - this is important
		if !ok {
			return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
		}

		ds, ok := storage.RelationshipTupleReaderFromContext(parentctx)
		if !ok {
			return nil, fmt.Errorf("relationship tuple reader datastore missing in context")
		}

		storeID := req.GetStoreID()
		reqTupleKey := req.GetTupleKey()
		objectType := tuple.GetType(reqTupleKey.GetObject())
		relation := reqTupleKey.GetRelation()

		// directlyRelatedUsersetTypes could be "user:*" or "group#member"
		directlyRelatedUsersetTypes, _ := typesys.DirectlyRelatedUsersets(objectType, relation)

		// TODO(jpadilla): can we lift this function up?
		checkDirectUserTuple := func(ctx context.Context) (*ResolveCheckResponse, error) {
			ctx, span := tracer.Start(ctx, "checkDirectUserTuple", trace.WithAttributes(attribute.String("tuple_key", reqTupleKey.String())))
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
			err = validation.ValidateTuple(typesys, tupleKey)
			if err != nil {
				return response, nil
			}
			tupleKeyConditionFilter := buildTupleKeyConditionFilter(ctx, req.Context, typesys)
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
				buildTupleKeyConditionFilter(ctx, req.GetContext(), typesys),
			)
			defer filteredIter.Stop()

			resolver := c.checkUsersetSlowPath

			if typesys.UsersetCanFastPath(directlyRelatedUsersetTypes, tuple.GetType(reqTupleKey.GetUser())) {
				resolver = c.checkUsersetFastPath
			}

			return resolver(ctx, filteredIter, req)
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
func (c *LocalChecker) checkComputedUserset(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset) CheckHandlerFunc {
	_, span := tracer.Start(ctx, "checkComputedUserset")
	defer span.End()

	rewrittenTupleKey := tuple.NewTupleKey(
		req.GetTupleKey().GetObject(),
		rewrite.GetComputedUserset().GetRelation(),
		req.GetTupleKey().GetUser(),
	)

	childRequest := clone(req)
	childRequest.TupleKey = rewrittenTupleKey

	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		// No dispatch here, as we don't want to increase resolution depth.
		return c.ResolveCheck(ctx, childRequest)
	}
}

// checkTTUSlowPath is the slow path for checkTTU where we cannot short-circuit TTU evaluation and
// resort to dispatch check on its children.
func (c *LocalChecker) checkTTUSlowPath(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter *storage.ConditionsFilteredTupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkTTUSlowPath")
	defer span.End()

	var handlers []CheckHandlerFunc

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
	}

	computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()
	tk := req.GetTupleKey()

	for {
		t, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			telemetry.TraceError(span, err)
			return nil, err
		}

		userObj, _ := tuple.SplitObjectRelation(t.GetUser())
		tupleKey := &openfgav1.TupleKey{
			Object:   userObj,
			Relation: computedRelation,
			User:     tk.GetUser(),
		}

		if _, err := typesys.GetRelation(tuple.GetType(userObj), computedRelation); err != nil {
			if errors.Is(err, typesystem.ErrRelationUndefined) {
				continue // skip computed relations on tupleset relationships if they are undefined
			}
		}

		// Note: we add TTU read below
		handlers = append(handlers, c.dispatch(ctx, req, tupleKey))
	}

	resp, err := union(ctx, c.concurrencyLimit, handlers...)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	// if we had 3 dispatched requests, and the final result is "allowed = false",
	// we want final reads to be (N1 + N2 + N3 + 1) and not (N1 + 1) + (N2 + 1) + (N3 + 1)
	// if final result is "allowed = true", we want final reads to be N1 + 1
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

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
	}

	terminalRelations := typesys.GetTerminalRelations(
		tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation(), tuple.GetType(req.GetTupleKey().GetUser()),
	)
	if len(terminalRelations) != 1 {
		return nil, fmt.Errorf("expected exactly one terminal relation for fast path, received %d", len(terminalRelations))
	}

	computedRelation := terminalRelations[0]
	// usersetsMap is a map of all ObjectRelations and its Ids. For example,
	// [group:1#member, group:2#member, group:1#owner, group:3#owner] will be stored as
	// [group#member][1, 2]
	// [group#owner][1, 3]
	usersetsMap := make(usersetsMapType)

	for {
		t, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			telemetry.TraceError(span, err)
			return nil, err
		}

		object, _ := tuple.SplitObjectRelation(t.GetUser())
		objectType, objectID := tuple.SplitObject(object)
		objectRel := tuple.ToObjectRelationString(objectType, computedRelation)
		if _, ok := usersetsMap[objectRel]; !ok {
			usersetsMap[objectRel] = storage.NewSortedSet()
		}
		usersetsMap[objectRel].Add(objectID)
	}

	// Next, for each of the type in tuplesetRelationUserMap, look up what object is in computedRelation for the specified user.
	// We will then try to see if there are any intersection.
	// Return true if user is in any of the computedRelation.  For example,
	// type group
	//   define member: [user]
	// type doc
	//   define parent: [group]
	//   define viewer: member from parent
	// we want to find out which group user:bob is a member of.
	// After that, we will find the intersection.
	handlers := make([]CheckHandlerFunc, 0, len(usersetsMap))
	for objectRel, objectIDs := range usersetsMap {
		handler := c.buildCheckAssociatedObjects(req, objectRel, objectIDs)
		handlers = append(handlers, handler)
	}

	resp, err := union(ctx, c.concurrencyLimit, handlers...)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	resp.GetResolutionMetadata().DatastoreQueryCount++

	return resp, nil
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

		typesys, ok := typesystem.TypesystemFromContext(parentctx) // note: use of 'parentctx' not 'ctx' - this is important
		if !ok {
			return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
		}

		ds, ok := storage.RelationshipTupleReaderFromContext(parentctx)
		if !ok {
			return nil, fmt.Errorf("relationship tuple reader datastore missing in context")
		}

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
		defer iter.Stop()

		// filter out invalid tuples yielded by the database iterator
		filteredIter := storage.NewConditionsFilteredTupleKeyIterator(
			storage.NewFilteredTupleKeyIterator(
				storage.NewTupleKeyIteratorFromTupleIterator(iter),
				validation.FilterInvalidTuples(typesys),
			),
			buildTupleKeyConditionFilter(ctx, req.GetContext(), typesys),
		)
		defer filteredIter.Stop()

		resolver := c.checkTTUSlowPath

		// TODO: optimize the case where user is an userset.
		// If the user is a userset, we will not be able to use the shortcut because the algo
		// will look up the objects associated with user.
		if !tuple.IsObjectRelation(tk.GetUser()) {
			if canFastPath := typesys.TTUCanFastPath(
				tuple.GetType(object), req.GetTupleKey().GetRelation(), tuple.GetType(req.GetTupleKey().GetUser())); canFastPath {
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
