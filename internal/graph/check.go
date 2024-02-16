package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/structpb"

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

var (
	ErrCycleDetected = errors.New("a cycle has been detected")
)

type ResolveCheckRequest struct {
	StoreID              string
	AuthorizationModelID string
	TupleKey             *openfgav1.TupleKey
	ContextualTuples     []*openfgav1.TupleKey
	Context              *structpb.Struct
	ResolutionMetadata   *ResolutionMetadata
	VisitedPaths         map[string]struct{}
}

type ResolveCheckResponse struct {
	Allowed            bool
	ResolutionMetadata *ResolutionMetadata
}

func (r *ResolveCheckResponse) GetAllowed() bool {
	if r != nil {
		return r.Allowed
	}

	return false
}

func (r *ResolveCheckResponse) GetResolutionMetadata() *ResolutionMetadata {
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

func (r *ResolveCheckRequest) GetResolutionMetadata() *ResolutionMetadata {
	if r != nil {
		return r.ResolutionMetadata
	}

	return nil
}

func (r *ResolveCheckRequest) GetContext() *structpb.Struct {
	if r != nil {
		return r.Context
	}
	return nil
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

// WithResolveNodeBreadthLimit see server.WithResolveNodeBreadthLimit
func WithResolveNodeBreadthLimit(limit uint32) LocalCheckerOption {
	return func(d *LocalChecker) {
		d.concurrencyLimit = limit
	}
}

// WithMaxConcurrentReads see server.WithMaxConcurrentReadsForCheck
func WithMaxConcurrentReads(limit uint32) LocalCheckerOption {
	return func(d *LocalChecker) {
		d.maxConcurrentReads = limit
	}
}

// NewLocalChecker constructs a LocalChecker that can be used to evaluate a Check.
// The constructed LocalChecker is not wrapped with cycle detection. Developers
// wanting a LocalChecker without other wrapped layers (e.g caching and others)
// are encouraged to use [[NewLocalCheckerWithCycleDetection]] instead.
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

// NewLocalCheckerWithCycleDetection constructs a LocalChecker wrapped with a [[CycleDetectionCheckResolver]]
// which can be used to evaluate a Check request locally with cycle detection enabled.
func NewLocalCheckerWithCycleDetection(opts ...LocalCheckerOption) CheckResolver {
	cycleDetectionCheckResolver := NewCycleDetectionCheckResolver()
	localCheckResolver := NewLocalChecker(opts...)

	cycleDetectionCheckResolver.SetDelegate(localCheckResolver)
	localCheckResolver.SetDelegate(cycleDetectionCheckResolver)

	return cycleDetectionCheckResolver
}

// SetDelegate sets this LocalChecker's dispatch delegate.
func (c *LocalChecker) SetDelegate(delegate CheckResolver) {
	c.delegate = delegate
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
	cancelCtx, cancel := context.WithCancel(ctx)
	resultChan := make(chan checkOutcome, len(handlers))

	drain := resolver(cancelCtx, concurrencyLimit, resultChan, handlers...)

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
				if errors.Is(result.err, ErrCycleDetected) {
					continue
				}
				err = result.err
				continue
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

	return &ResolveCheckResponse{
		Allowed: false,
		ResolutionMetadata: &ResolutionMetadata{
			DatastoreQueryCount: dbReads,
		},
	}, err
}

// intersection implements a CheckFuncReducer that requires all of the provided CheckHandlerFunc to resolve
// to an allowed outcome. The first falsey or erroneous outcome causes premature termination of the reducer.
func intersection(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*ResolveCheckResponse, error) {
	cancelCtx, cancel := context.WithCancel(ctx)
	resultChan := make(chan checkOutcome, len(handlers))

	drain := resolver(cancelCtx, concurrencyLimit, resultChan, handlers...)

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
				err = result.err
				continue
			}

			dbReads += result.resp.GetResolutionMetadata().DatastoreQueryCount
			if !result.resp.GetAllowed() {
				result.resp.GetResolutionMetadata().DatastoreQueryCount = dbReads
				return result.resp, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	allowed := true
	if err != nil {
		allowed = false
	}
	return &ResolveCheckResponse{
		Allowed: allowed,
		ResolutionMetadata: &ResolutionMetadata{
			DatastoreQueryCount: dbReads,
		},
	}, err
}

// exclusion implements a CheckFuncReducer that requires a 'base' CheckHandlerFunc to resolve to an allowed
// outcome and a 'sub' CheckHandlerFunc to resolve to a falsey outcome. The base and sub computations are
// handled concurrently relative to one another.
func exclusion(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*ResolveCheckResponse, error) {
	if len(handlers) != 2 {
		panic(fmt.Sprintf("expected two rewrite operands for exclusion operator, but got '%d'", len(handlers)))
	}

	limiter := make(chan struct{}, concurrencyLimit)

	cancelCtx, cancel := context.WithCancel(ctx)
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
		resp, err := baseHandler(cancelCtx)
		baseChan <- checkOutcome{resp, err}
		<-limiter
		wg.Done()
	}()

	limiter <- struct{}{}
	wg.Add(1)
	go func() {
		resp, err := subHandler(cancelCtx)
		subChan <- checkOutcome{resp, err}
		<-limiter
		wg.Done()
	}()

	response := &ResolveCheckResponse{
		Allowed: false,
		ResolutionMetadata: &ResolutionMetadata{
			DatastoreQueryCount: 0,
		},
	}

	var errs *multierror.Error
	var dbReads uint32
	for i := 0; i < len(handlers); i++ {
		select {
		case baseResult := <-baseChan:
			if baseResult.err != nil {
				errs = multierror.Append(errs, baseResult.err)
				continue
			}

			dbReads += baseResult.resp.GetResolutionMetadata().DatastoreQueryCount

			if !baseResult.resp.GetAllowed() {
				response.GetResolutionMetadata().DatastoreQueryCount = dbReads
				return response, nil
			}

		case subResult := <-subChan:
			if subResult.err != nil {
				errs = multierror.Append(errs, subResult.err)
				continue
			}

			dbReads += subResult.resp.GetResolutionMetadata().DatastoreQueryCount

			if subResult.resp.GetAllowed() {
				response.GetResolutionMetadata().DatastoreQueryCount = dbReads
				return response, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if errs.ErrorOrNil() != nil {
		return response, errs
	}

	return &ResolveCheckResponse{
		Allowed: true,
		ResolutionMetadata: &ResolutionMetadata{
			DatastoreQueryCount: dbReads,
		},
	}, nil
}

// Close is a noop
func (c *LocalChecker) Close() {
}

// dispatch dispatches the provided Check request to the CheckResolver this LocalChecker
// was constructed with.
func (c *LocalChecker) dispatch(_ context.Context, req *ResolveCheckRequest) CheckHandlerFunc {
	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		return c.delegate.ResolveCheck(ctx, req)
	}
}

// ResolveCheck resolves a node out of a tree of evaluations. If the depth of the tree has gotten too large,
// evaluation is aborted and an error is returned. The depth is NOT increased on computed usersets.
//
// It is expected that callers pass in, contextually, a [[storage.RelationshipTupleReader]] using
// [[storage.ContextWithRelationshipTupleReader]]. This is by design because this method is called by
// [[server.Check]], but each time it is called there are invariants that must be met that relate
// to the concurrency of the underlying RelationshipTupleReader as well as contextual tuples per
// parent request. Since [[server.Check]] shares a single instance of a [[LocalChecker]], and to
// meet the invariants mentioned above, we contextually define the datastore used to serve the
// ResolveCheck request.
func (c *LocalChecker) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	ctx, span := tracer.Start(ctx, "ResolveCheck")
	defer span.End()
	span.SetAttributes(attribute.String("resolver_type", "LocalChecker"))
	span.SetAttributes(attribute.String("tuple_key", req.GetTupleKey().String()))

	if req.GetResolutionMetadata().Depth == 0 {
		return nil, ErrResolutionDepthExceeded
	}

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		panic("typesystem missing in context")
	}

	tupleKey := req.GetTupleKey()
	object := tupleKey.GetObject()
	relation := tupleKey.GetRelation()

	objectType, _ := tuple.SplitObject(object)
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		return nil, fmt.Errorf("relation '%s' undefined for object type '%s'", relation, objectType)
	}

	resp, err := union(ctx, c.concurrencyLimit, c.checkRewrite(ctx, req, rel.GetRewrite()))
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

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
			return nil, fmt.Errorf("typesystem missing in context")
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

		fn1 := func(ctx context.Context) (*ResolveCheckResponse, error) {
			ctx, span := tracer.Start(ctx, "checkDirectUserTuple", trace.WithAttributes(attribute.String("tuple_key", reqTupleKey.String())))
			defer span.End()

			response := &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolutionMetadata{
					DatastoreQueryCount: req.GetResolutionMetadata().DatastoreQueryCount + 1,
				},
			}

			t, err := ds.ReadUserTuple(ctx, storeID, reqTupleKey)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return response, nil
				}

				return response, err
			}

			// filter out invalid tuples yielded by the database query
			tupleKey := t.GetKey()
			err = validation.ValidateTuple(typesys, tupleKey)

			if t != nil && err == nil {
				condEvalResult, err := eval.EvaluateTupleCondition(ctx, tupleKey, typesys, req.GetContext())
				if err != nil {
					telemetry.TraceError(span, err)
					return nil, err
				}

				if len(condEvalResult.MissingParameters) > 0 {
					evalErr := condition.NewEvaluationError(
						tupleKey.GetCondition().GetName(),
						fmt.Errorf("context is missing parameters '%v'", condEvalResult.MissingParameters),
					)
					telemetry.TraceError(span, evalErr)
					return nil, evalErr
				}

				if !condEvalResult.ConditionMet {
					return response, nil
				}

				span.SetAttributes(attribute.Bool("allowed", true))
				response.Allowed = true
				return response, nil
			}
			return response, nil
		}

		fn2 := func(ctx context.Context) (*ResolveCheckResponse, error) {
			ctx, span := tracer.Start(ctx, "checkDirectUsersetTuples", trace.WithAttributes(attribute.String("userset", tuple.ToObjectRelationString(reqTupleKey.Object, reqTupleKey.Relation))))
			defer span.End()

			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			response := &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolutionMetadata{
					DatastoreQueryCount: req.GetResolutionMetadata().DatastoreQueryCount + 1,
				},
			}

			iter, err := ds.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
				Object:                      reqTupleKey.Object,
				Relation:                    reqTupleKey.Relation,
				AllowedUserTypeRestrictions: directlyRelatedUsersetTypes,
			})
			if err != nil {
				return response, err
			}
			defer iter.Stop()

			// filter out invalid tuples yielded by the database iterator
			filteredIter := storage.NewFilteredTupleKeyIterator(
				storage.NewTupleKeyIteratorFromTupleIterator(iter),
				validation.FilterInvalidTuples(typesys),
			)
			defer filteredIter.Stop()

			var errs *multierror.Error
			var handlers []CheckHandlerFunc
			for {
				t, err := filteredIter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}

					return response, err
				}

				condEvalResult, err := eval.EvaluateTupleCondition(ctx, t, typesys, req.GetContext())
				if err != nil {
					errs = multierror.Append(errs, err)
					continue
				}

				if len(condEvalResult.MissingParameters) > 0 {
					errs = multierror.Append(errs, condition.NewEvaluationError(
						t.GetCondition().GetName(),
						fmt.Errorf("tuple '%s' is missing context parameters '%v'",
							tuple.TupleKeyToString(t),
							condEvalResult.MissingParameters),
					))

					continue
				}

				if !condEvalResult.ConditionMet {
					continue
				}

				usersetObject, usersetRelation := tuple.SplitObjectRelation(t.GetUser())

				// if the user value is a typed wildcard and the type of the wildcard
				// matches the target user objectType, then we're done searching
				if tuple.IsTypedWildcard(usersetObject) && typesys.GetSchemaVersion() == typesystem.SchemaVersion1_1 {
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

					handlers = append(handlers, c.dispatch(
						ctx,
						&ResolveCheckRequest{
							StoreID:              storeID,
							AuthorizationModelID: req.GetAuthorizationModelID(),
							TupleKey:             tupleKey,
							ContextualTuples:     req.GetContextualTuples(),
							ResolutionMetadata: &ResolutionMetadata{
								Depth:               req.GetResolutionMetadata().Depth - 1,
								DatastoreQueryCount: response.GetResolutionMetadata().DatastoreQueryCount,
							},
							VisitedPaths: maps.Clone(req.VisitedPaths),
							Context:      req.GetContext(),
						}))
				}
			}

			if len(handlers) == 0 && errs.ErrorOrNil() != nil {
				telemetry.TraceError(span, errs)
				return nil, errs
			}

			resp, err := union(ctx, c.concurrencyLimit, handlers...)
			if err != nil {
				telemetry.TraceError(span, err)
				return nil, multierror.Append(errs, err)
			}

			return resp, nil
		}

		var checkFuncs []CheckHandlerFunc

		shouldCheckDirectTuple, _ := typesys.IsDirectlyRelated(
			typesystem.DirectRelationReference(objectType, relation),                                                           // target
			typesystem.DirectRelationReference(tuple.GetType(reqTupleKey.GetUser()), tuple.GetRelation(reqTupleKey.GetUser())), // source
		)

		if shouldCheckDirectTuple {
			checkFuncs = []CheckHandlerFunc{fn1}
		}

		if len(directlyRelatedUsersetTypes) > 0 {
			checkFuncs = append(checkFuncs, fn2)
		}

		resp, err := union(ctx, c.concurrencyLimit, checkFuncs...)
		if err != nil {
			telemetry.TraceError(span, err)
		}
		return resp, err
	}
}

// checkComputedUserset evaluates the Check request with the rewritten relation (e.g. the computed userset relation).
func (c *LocalChecker) checkComputedUserset(_ context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset_ComputedUserset) CheckHandlerFunc {
	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		ctx, span := tracer.Start(ctx, "checkComputedUserset")
		defer span.End()

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		rewrittenTupleKey := tuple.NewTupleKey(
			req.TupleKey.GetObject(),
			rewrite.ComputedUserset.GetRelation(),
			req.TupleKey.GetUser(),
		)

		return c.dispatch(
			ctx,
			&ResolveCheckRequest{
				StoreID:              req.GetStoreID(),
				AuthorizationModelID: req.GetAuthorizationModelID(),
				TupleKey:             rewrittenTupleKey,
				ContextualTuples:     req.GetContextualTuples(),
				ResolutionMetadata: &ResolutionMetadata{
					Depth:               req.GetResolutionMetadata().Depth - 1,
					DatastoreQueryCount: req.GetResolutionMetadata().DatastoreQueryCount,
				},
				VisitedPaths: maps.Clone(req.VisitedPaths),
				Context:      req.GetContext(),
			})(ctx)
	}
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
			return nil, fmt.Errorf("typesystem missing in context")
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

		span.SetAttributes(attribute.String("tupleset_relation", fmt.Sprintf("%s#%s", tuple.GetType(object), tuplesetRelation)))
		span.SetAttributes(attribute.String("computed_relation", computedRelation))

		response := &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolutionMetadata{
				DatastoreQueryCount: req.GetResolutionMetadata().DatastoreQueryCount + 1,
			},
		}
		iter, err := ds.Read(
			ctx,
			req.GetStoreID(),
			tuple.NewTupleKey(object, tuplesetRelation, ""),
		)
		if err != nil {
			return response, err
		}
		defer iter.Stop()

		// filter out invalid tuples yielded by the database iterator
		filteredIter := storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(iter),
			validation.FilterInvalidTuples(typesys),
		)
		defer filteredIter.Stop()

		var errs *multierror.Error
		var handlers []CheckHandlerFunc
		for {
			t, err := filteredIter.Next(ctx)
			if err != nil {
				if err == storage.ErrIteratorDone {
					break
				}

				return response, err
			}

			condEvalResult, err := eval.EvaluateTupleCondition(ctx, t, typesys, req.GetContext())
			if err != nil {
				errs = multierror.Append(errs, err)

				continue
			}

			if len(condEvalResult.MissingParameters) > 0 {
				errs = multierror.Append(errs, condition.NewEvaluationError(
					t.GetCondition().GetName(),
					fmt.Errorf("tuple '%s' is missing context parameters '%v'",
						tuple.TupleKeyToString(t),
						condEvalResult.MissingParameters),
				))

				continue
			}

			if !condEvalResult.ConditionMet {
				continue
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

			handlers = append(handlers, c.dispatch(
				ctx,
				&ResolveCheckRequest{
					StoreID:              req.GetStoreID(),
					AuthorizationModelID: req.GetAuthorizationModelID(),
					TupleKey:             tupleKey,
					ContextualTuples:     req.GetContextualTuples(),
					ResolutionMetadata: &ResolutionMetadata{
						Depth:               req.GetResolutionMetadata().Depth - 1,
						DatastoreQueryCount: req.GetResolutionMetadata().DatastoreQueryCount, // add TTU read below
					},
					VisitedPaths: maps.Clone(req.VisitedPaths),
					Context:      req.GetContext(),
				}))
		}

		if len(handlers) == 0 && errs.ErrorOrNil() != nil {
			telemetry.TraceError(span, errs)
			return nil, errs
		}

		unionResponse, err := union(ctx, c.concurrencyLimit, handlers...)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, multierror.Append(errs, err)
		}

		// if we had 3 dispatched requests, and the final result is "allowed = false",
		// we want final reads to be (N1 + N2 + N3 + 1) and not (N1 + 1) + (N2 + 1) + (N3 + 1)
		// if final result is "allowed = true", we want final reads to be N1 + 1
		unionResponse.GetResolutionMetadata().DatastoreQueryCount++

		return unionResponse, nil
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
		panic("unexpected set operator type encountered")
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
	switch rw := rewrite.Userset.(type) {
	case *openfgav1.Userset_This:
		return c.checkDirect(ctx, req)
	case *openfgav1.Userset_ComputedUserset:
		return c.checkComputedUserset(ctx, req, rw)
	case *openfgav1.Userset_TupleToUserset:
		return c.checkTTU(ctx, req, rewrite)
	case *openfgav1.Userset_Union:
		return c.checkSetOperation(ctx, req, unionSetOperator, union, rw.Union.GetChild()...)
	case *openfgav1.Userset_Intersection:
		return c.checkSetOperation(ctx, req, intersectionSetOperator, intersection, rw.Intersection.GetChild()...)
	case *openfgav1.Userset_Difference:
		return c.checkSetOperation(ctx, req, exclusionSetOperator, exclusion, rw.Difference.GetBase(), rw.Difference.GetSubtract())
	default:
		panic("unexpected userset rewrite encountered")
	}
}
