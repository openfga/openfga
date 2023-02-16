package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("internal/graph/check")

// CheckResolver represents an interface that can be implemented to provide recursive resolution
// of a Check.
type CheckResolver interface {
	ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error)
}

type ResolveCheckRequest struct {
	StoreID              string
	AuthorizationModelID string
	TupleKey             *openfgapb.TupleKey
	ContextualTuples     []*openfgapb.TupleKey
	ResolutionMetadata   *ResolutionMetadata
}

type ResolveCheckResponse struct {
	Allowed bool
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

func (r *ResolveCheckRequest) GetTupleKey() *openfgapb.TupleKey {
	if r != nil {
		return r.TupleKey
	}

	return nil
}

func (r *ResolveCheckRequest) GetContextualTuples() []*openfgapb.TupleKey {
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

type setOperatorType int

const (
	unionSetOperator setOperatorType = iota
	intersectionSetOperator
	exclusionSetOperator
)

type checkOutcome struct {
	resp *openfgapb.CheckResponse
	err  error
}

// LocalChecker implements Check in a highly concurrent and localized manner. The
// Check resolution is limited per branch of evaluation by the concurrencyLimit.
type LocalChecker struct {
	ds               storage.RelationshipTupleReader
	concurrencyLimit uint32
}

// NewLocalChecker constructs a LocalChecker that can be used to evaluate a Check
// request locally and with a high degree of concurrency.
func NewLocalChecker(
	ds storage.RelationshipTupleReader,
	concurrencyLimit uint32,
) *LocalChecker {
	checker := &LocalChecker{ds: ds, concurrencyLimit: concurrencyLimit}
	return checker
}

// CheckHandlerFunc defines a function that evaluates a CheckResponse or returns an error
// otherwise.
type CheckHandlerFunc func(ctx context.Context) (*openfgapb.CheckResponse, error)

// CheckFuncReducer defines a function that combines or reduces one or more CheckHandlerFunc into
// a single CheckResponse with a maximum limit on the number of concurrent evaluations that can be
// in flight at any given time.
type CheckFuncReducer func(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*openfgapb.CheckResponse, error)

// resolver concurrently resolves one or more CheckHandlerFunc and yields the results on the provided resultChan.
// Callers of the 'resolver' function should be sure to invoke the callback returned from this function to ensure
// every concurrent check is evaluated. The concurrencyLimit can be set to provide a maximum number of concurrent
// evaluations in flight at any point.
func resolver(ctx context.Context, concurrencyLimit uint32, resultChan chan<- checkOutcome, handlers ...CheckHandlerFunc) func() {
	limiter := make(chan struct{}, concurrencyLimit)

	var wg sync.WaitGroup

	checker := func(fn CheckHandlerFunc) {
		defer wg.Done()

		resolved := make(chan checkOutcome, 1)

		go func() {
			resp, err := fn(ctx)
			resolved <- checkOutcome{resp, err}
			<-limiter
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
func union(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*openfgapb.CheckResponse, error) {

	ctx, cancel := context.WithCancel(ctx)
	resultChan := make(chan checkOutcome, len(handlers))

	drain := resolver(ctx, concurrencyLimit, resultChan, handlers...)

	defer func() {
		cancel()
		drain()
		close(resultChan)
	}()

	for i := 0; i < len(handlers); i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, result.err
			}

			if result.resp.GetAllowed() {
				return result.resp, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &openfgapb.CheckResponse{Allowed: false}, nil
}

// intersection implements a CheckFuncReducer that requires all of the provided CheckHandlerFunc to resolve
// to an allowed outcome. The first falsey or erroneous outcome causes premature termination of the reducer.
func intersection(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*openfgapb.CheckResponse, error) {

	ctx, cancel := context.WithCancel(ctx)
	resultChan := make(chan checkOutcome, len(handlers))

	drain := resolver(ctx, concurrencyLimit, resultChan, handlers...)

	defer func() {
		cancel()
		drain()
		close(resultChan)
	}()

	for i := 0; i < len(handlers); i++ {
		select {
		case result := <-resultChan:

			if result.err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, result.err
			}

			if !result.resp.GetAllowed() {
				return result.resp, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &openfgapb.CheckResponse{Allowed: true}, nil
}

// exclusion implements a CheckFuncReducer that requires a 'base' CheckHandlerFunc to resolve to an allowed
// outcome and a 'sub' CheckHandlerFunc to resolve to a falsey outcome. The base and sub computations are
// handled concurrently relative to one another.
func exclusion(ctx context.Context, concurrencyLimit uint32, handlers ...CheckHandlerFunc) (*openfgapb.CheckResponse, error) {

	if len(handlers) != 2 {
		panic(fmt.Sprintf("expected two rewrite operands for exclusion operator, but got '%d'", len(handlers)))
	}

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

	for i := 0; i < len(handlers); i++ {
		select {
		case baseResult := <-baseChan:
			if baseResult.err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, baseResult.err
			}

			if !baseResult.resp.Allowed {
				return &openfgapb.CheckResponse{Allowed: false}, nil
			}
		case subResult := <-subChan:
			if subResult.err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, subResult.err
			}

			if subResult.resp.Allowed {
				return &openfgapb.CheckResponse{Allowed: false}, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &openfgapb.CheckResponse{Allowed: true}, nil
}

// dispatch dispatches the provided Check request to the CheckResolver this LocalChecker
// was constructed with.
func (c *LocalChecker) dispatch(ctx context.Context, req *ResolveCheckRequest) CheckHandlerFunc {
	return func(ctx context.Context) (*openfgapb.CheckResponse, error) {
		resp, err := c.ResolveCheck(ctx, req)
		if err != nil {
			return nil, err
		}

		return &openfgapb.CheckResponse{
			Allowed: resp.Allowed,
		}, nil
	}
}

func (c *LocalChecker) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "ResolveCheck")
	defer span.End()

	if req.GetResolutionMetadata().Depth == 0 {
		return nil, ErrResolutionDepthExceeded
	}

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		panic("typesystem missing in context")
	}

	object := req.GetTupleKey().GetObject()
	relation := req.GetTupleKey().GetRelation()

	objectType, _ := tuple.SplitObject(object)
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		return nil, fmt.Errorf("relation '%s' undefined for object type '%s'", relation, objectType)
	}

	resp, err := union(ctx, c.concurrencyLimit, c.checkRewrite(ctx, req, rel.GetRewrite()))
	if err != nil {
		return nil, err
	}

	return &ResolveCheckResponse{
		Allowed: resp.Allowed,
	}, nil
}

// checkDirect composes two CheckHandlerFunc which evaluate direct relationships with the provided
// 'object#relation'. The first handler looks up direct matches on the provided 'object#relation@user',
// while the second handler looks up relationships between the target 'object#relation' and any usersets
// related to it.
func (c *LocalChecker) checkDirect(parentctx context.Context, req *ResolveCheckRequest) CheckHandlerFunc {

	return func(ctx context.Context) (*openfgapb.CheckResponse, error) {
		typesys, ok := typesystem.TypesystemFromContext(parentctx) // note: use of 'parentctx' not 'ctx' - this is important
		if !ok {
			return nil, fmt.Errorf("typesystem missing in context")
		}

		ctx, span := tracer.Start(ctx, "checkDirect")
		defer span.End()

		storeID := req.GetStoreID()
		tk := req.GetTupleKey()

		fn1 := func(ctx context.Context) (*openfgapb.CheckResponse, error) {
			t, err := c.ds.ReadUserTuple(ctx, storeID, tk)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return &openfgapb.CheckResponse{Allowed: false}, nil
				}

				return &openfgapb.CheckResponse{Allowed: false}, err
			}

			// filter out invalid tuples yielded by the database query
			err = validation.ValidateTuple(typesys, tk)

			if t != nil && err == nil {
				return &openfgapb.CheckResponse{Allowed: true}, nil
			}
			return &openfgapb.CheckResponse{Allowed: false}, nil
		}

		var checkFuncs []CheckHandlerFunc

		if typesys.GetSchemaVersion() == typesystem.SchemaVersion1_0 {
			checkFuncs = append(checkFuncs, fn1)
		} else {
			shouldCheckDirectTuple, _ := typesys.IsDirectlyRelated(
				typesystem.DirectRelationReference(tuple.GetType(tk.GetObject()), tk.GetRelation()),              //target
				typesystem.DirectRelationReference(tuple.GetType(tk.GetUser()), tuple.GetRelation(tk.GetUser())), //source
			)
			if shouldCheckDirectTuple {
				checkFuncs = append(checkFuncs, fn1)
			}
		}

		fn2 := func(ctx context.Context) (*openfgapb.CheckResponse, error) {
			var allowedTypesForUser []*openfgapb.RelationReference
			if typesys.GetSchemaVersion() == typesystem.SchemaVersion1_1 {
				// allowedTypesForUser could be "user" or "user:*" or "group#member"
				allowedTypesForUser, _ = typesys.GetDirectlyRelatedUserTypes(tuple.GetType(tk.GetObject()), tk.Relation)
			}
			iter, err := c.ds.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
				ObjectID:            tk.Object,
				Relation:            tk.Relation,
				AllowedTypesForUser: allowedTypesForUser,
			})
			if err != nil {
				return &openfgapb.CheckResponse{Allowed: false}, err
			}
			defer iter.Stop()

			// filter out invalid tuples yielded by the database iterator
			filteredIter := storage.NewFilteredTupleKeyIterator(
				storage.NewTupleKeyIteratorFromTupleIterator(iter),
				validation.FilterInvalidTuples(typesys),
			)
			defer filteredIter.Stop()

			var handlers []CheckHandlerFunc
			for {
				t, err := filteredIter.Next()
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}

					return &openfgapb.CheckResponse{Allowed: false}, err
				}

				usersetObject, usersetRelation := tuple.SplitObjectRelation(t.GetUser())

				// for 1.0 models, if the user is '*' then we're done searching
				if usersetObject == tuple.Wildcard && typesys.GetSchemaVersion() == typesystem.SchemaVersion1_0 {
					return &openfgapb.CheckResponse{Allowed: true}, nil
				}

				// for 1.1 models, if the user value is a typed wildcard and the type of the wildcard
				// matches the target user objectType, then we're done searching
				if tuple.IsTypedWildcard(usersetObject) && typesys.GetSchemaVersion() == typesystem.SchemaVersion1_1 {

					wildcardType := tuple.GetType(usersetObject)

					if tuple.GetType(tk.GetUser()) == wildcardType {
						return &openfgapb.CheckResponse{Allowed: true}, nil
					}

					continue
				}

				if usersetRelation != "" {
					handlers = append(handlers, c.dispatch(
						ctx,
						&ResolveCheckRequest{
							StoreID:              storeID,
							AuthorizationModelID: req.GetAuthorizationModelID(),
							TupleKey:             tuple.NewTupleKey(usersetObject, usersetRelation, tk.GetUser()),
							ResolutionMetadata: &ResolutionMetadata{
								Depth: req.GetResolutionMetadata().Depth - 1,
							},
						}))
				}
			}

			if len(handlers) == 0 {
				return &openfgapb.CheckResponse{Allowed: false}, nil

			}

			return union(ctx, c.concurrencyLimit, handlers...)
		}

		checkFuncs = append(checkFuncs, fn2)

		return union(ctx, c.concurrencyLimit, checkFuncs...)
	}
}

// checkTTU looks up all tuples of the target tupleset relation on the provided object and for each one
// of them evaluates the computed userset of the TTU rewrite rule for them.
func (c *LocalChecker) checkTTU(parentctx context.Context, req *ResolveCheckRequest, rewrite *openfgapb.Userset) CheckHandlerFunc {

	return func(ctx context.Context) (*openfgapb.CheckResponse, error) {
		typesys, ok := typesystem.TypesystemFromContext(parentctx) // note: use of 'parentctx' not 'ctx' - this is important
		if !ok {
			return nil, fmt.Errorf("typesystem missing in context")
		}

		ctx, span := tracer.Start(ctx, "checkTTU")
		defer span.End()

		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		tuplesetRelation := rewrite.GetTupleToUserset().GetTupleset().GetRelation()
		computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()

		tk := req.GetTupleKey()

		iter, err := c.ds.Read(
			ctx,
			req.GetStoreID(),
			tuple.NewTupleKey(tk.GetObject(), tuplesetRelation, ""),
		)
		if err != nil {
			return &openfgapb.CheckResponse{Allowed: false}, err
		}
		defer iter.Stop()

		// filter out invalid tuples yielded by the database iterator
		filteredIter := storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(iter),
			validation.FilterInvalidTuples(typesys),
		)
		defer filteredIter.Stop()

		var handlers []CheckHandlerFunc
		for {
			t, err := filteredIter.Next()
			if err != nil {
				if err == storage.ErrIteratorDone {
					break
				}

				return &openfgapb.CheckResponse{Allowed: false}, err
			}

			userObj, _ := tuple.SplitObjectRelation(t.GetUser())

			tupleKey := &openfgapb.TupleKey{
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
					ResolutionMetadata: &ResolutionMetadata{
						Depth: req.GetResolutionMetadata().Depth - 1,
					},
				}))
		}

		if len(handlers) == 0 {
			return &openfgapb.CheckResponse{Allowed: false}, nil
		}

		return union(ctx, c.concurrencyLimit, handlers...)
	}
}

func (c *LocalChecker) checkSetOperation(
	ctx context.Context,
	req *ResolveCheckRequest,
	setOpType setOperatorType,
	reducer CheckFuncReducer,
	children ...*openfgapb.Userset,
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

	return func(ctx context.Context) (*openfgapb.CheckResponse, error) {
		ctx, span := tracer.Start(ctx, "checkSetOperation")
		defer span.End()

		span.SetAttributes(attribute.String("reducer", reducerKey))

		return reducer(ctx, c.concurrencyLimit, handlers...)
	}
}

func (c *LocalChecker) checkRewrite(
	ctx context.Context,
	req *ResolveCheckRequest,
	rewrite *openfgapb.Userset,
) CheckHandlerFunc {

	switch rw := rewrite.Userset.(type) {
	case *openfgapb.Userset_This:
		return c.checkDirect(ctx, req)
	case *openfgapb.Userset_ComputedUserset:
		ctx, span := tracer.Start(ctx, "checkComputedUserset")
		defer span.End()

		return c.dispatch(
			ctx,
			&ResolveCheckRequest{
				StoreID:              req.GetStoreID(),
				AuthorizationModelID: req.GetAuthorizationModelID(),
				TupleKey: tuple.NewTupleKey(
					req.TupleKey.GetObject(),
					rw.ComputedUserset.GetRelation(),
					req.TupleKey.GetUser(),
				),
				ResolutionMetadata: &ResolutionMetadata{
					Depth: req.ResolutionMetadata.Depth - 1,
				},
			})

	case *openfgapb.Userset_TupleToUserset:
		return c.checkTTU(ctx, req, rewrite)
	case *openfgapb.Userset_Union:
		return c.checkSetOperation(ctx, req, unionSetOperator, union, rw.Union.GetChild()...)
	case *openfgapb.Userset_Intersection:
		return c.checkSetOperation(ctx, req, intersectionSetOperator, intersection, rw.Intersection.GetChild()...)
	case *openfgapb.Userset_Difference:
		return c.checkSetOperation(ctx, req, exclusionSetOperator, exclusion, rw.Difference.GetBase(), rw.Difference.GetSubtract())
	default:
		panic("unexpected userset rewrite encountered")
	}
}
