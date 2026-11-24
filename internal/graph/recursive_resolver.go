package graph

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/hashset"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const recursiveResolver = "recursive"

// In general these values tell the query planner that the recursive strategy usually performs around 150 ms but occasionally spikes.
// However, even when it spikes we want to keep it using it or exploring it despite variance, rather than over-penalizing single slow runs.
var recursivePlan = &planner.PlanConfig{
	Name:         recursiveResolver,
	InitialGuess: 150 * time.Millisecond,
	// Medium Lambda: Represents medium confidence in the initial guess. It's like
	// starting with the belief of having already seen 3 good runs.
	Lambda: 3.0,
	// UNCERTAINTY ABOUT CONSISTENCY: The gap between p50 and p99 is large.
	// Moderate Alpha/Beta values create a balanced belief curve, telling the planner
	// to expect variations but with higher confidence than before.
	// Higher expected precision: E[τ]= α/β = 3.0/2.0 = 1.5.
	// Moderate expected variance: E[σ²]= β/(α−1) = 2.0/(3.0−1) = 1.0. This allows for variance but is less jittery than previous settings.
	// Tighter tolerance for spread: α = 3 indicates a narrower uncertainty than α = 2, meaning we are more certain about the variance range.
	// When α > β, we expect higher precision and more controlled variance.
	Alpha: 3.0,
	Beta:  2.0,
}

type recursiveMapping struct {
	kind                        storage.TupleMapperKind
	tuplesetRelation            string
	computedRelation            string
	allowedUserTypeRestrictions []*openfgav1.RelationReference
}

func (c *LocalChecker) recursiveUserset(_ context.Context, req *ResolveCheckRequest, recursiveUsersets []*openfgav1.RelationReference, rightIter storage.TupleKeyIterator, _ string) CheckHandlerFunc {
	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		typesys, _ := typesystem.TypesystemFromContext(ctx)
		objectProvider := newRecursiveUsersetObjectProvider(typesys)

		return c.recursiveFastPath(ctx, req, rightIter, &recursiveMapping{
			kind:                        storage.UsersetKind,
			allowedUserTypeRestrictions: recursiveUsersets,
		}, objectProvider)
	}
}

// recursiveTTU solves a union relation of the form "{operand1} OR ... {operandN} OR {recursive TTU}"
// rightIter gives the iterator for the recursive TTU.
func (c *LocalChecker) recursiveTTU(_ context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, rightIter storage.TupleKeyIterator, _ string) CheckHandlerFunc {
	return func(ctx context.Context) (*ResolveCheckResponse, error) {
		typesys, _ := typesystem.TypesystemFromContext(ctx)

		ttu := rewrite.GetTupleToUserset()

		objectProvider := newRecursiveTTUObjectProvider(typesys, ttu)

		return c.recursiveFastPath(ctx, req, rightIter, &recursiveMapping{
			kind:             storage.TTUKind,
			tuplesetRelation: ttu.GetTupleset().GetRelation(),
			computedRelation: ttu.GetComputedUserset().GetRelation(),
		}, objectProvider)
	}
}

func (c *LocalChecker) recursiveFastPath(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator, mapping *recursiveMapping, objectProvider objectProvider) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "recursiveFastPath")
	defer span.End()
	usersetFromUser := hashset.New()
	usersetFromObject := hashset.New()

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	objectToUsersetIter := storage.WrapIterator(mapping.kind, iter)
	defer objectToUsersetIter.Stop()
	objectToUsersetMessageChan := streamedLookupUsersetFromIterator(cancellableCtx, objectToUsersetIter)

	res := &ResolveCheckResponse{
		Allowed: false,
	}

	// check to see if there are any recursive userset assigned. If not,
	// we don't even need to check the terminal type side.

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case objectToUsersetMessage, ok := <-objectToUsersetMessageChan:
		if !ok {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return res, nil
		}
		if objectToUsersetMessage.err != nil {
			return nil, objectToUsersetMessage.err
		}
		usersetFromObject.Add(objectToUsersetMessage.userset)
	}

	// condErrs holds the objects whose condition could not be evaluated on the user side.
	condErrs := newObjectSet()
	userToUsersetMessageChan, err := objectProvider.Begin(cancellableCtx, req, condErrs)
	if err != nil {
		return nil, err
	}
	defer objectProvider.End()

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
				userToUsersetMessageChan = nil
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if usersetFromUser.Size() == 0 && condErrs.empty() {
					return res, nil
				}
				break
			}
			if userToUsersetMessage.err != nil {
				return nil, userToUsersetMessage.err
			}
			if processUsersetMessage(userToUsersetMessage.userset, usersetFromUser, usersetFromObject) && !condErrs.has(userToUsersetMessage.userset) {
				res.Allowed = true
				return res, nil
			}
		case objectToUsersetMessage, ok := <-objectToUsersetMessageChan:
			if !ok {
				// usersetFromObject must not be empty because we would have caught it earlier.
				objectToUsersetDone = true
				objectToUsersetMessageChan = nil
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				break
			}
			if objectToUsersetMessage.err != nil {
				return nil, objectToUsersetMessage.err
			}
			if processUsersetMessage(objectToUsersetMessage.userset, usersetFromObject, usersetFromUser) && !condErrs.has(objectToUsersetMessage.userset) {
				res.Allowed = true
				return res, nil
			}
		}
	}

	return c.recursiveMatchUserUserset(ctx, req, mapping, usersetFromObject, usersetFromUser, condErrs)
}

func buildRecursiveMapper(ctx context.Context, req *ResolveCheckRequest, mapping *recursiveMapping) (storage.TupleMapper, error) {
	var iter storage.TupleIterator
	var err error
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)
	consistencyOpts := storage.ConsistencyOptions{
		Preference: req.GetConsistency(),
	}

	switch mapping.kind {
	case storage.UsersetKind:
		objectType := req.GetTupleKey().GetObject()
		relation := req.GetTupleKey().GetRelation()
		iter, err = ds.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
			Object:                      objectType,
			Relation:                    relation,
			AllowedUserTypeRestrictions: mapping.allowedUserTypeRestrictions,
		}, storage.ReadUsersetTuplesOptions{Consistency: consistencyOpts})
	case storage.TTUKind:
		objectType := req.GetTupleKey().GetObject()
		iter, err = ds.Read(ctx, req.GetStoreID(),
			storage.ReadFilter{Object: objectType, Relation: mapping.tuplesetRelation, User: ""},
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
	return storage.WrapIterator(mapping.kind, filteredIter), nil
}

func (c *LocalChecker) recursiveMatchUserUserset(ctx context.Context, req *ResolveCheckRequest, mapping *recursiveMapping, currentLevelFromObject *hashset.Set, usersetFromUser *hashset.Set, condErrs *objectSet) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "recursiveMatchUserUserset", trace.WithAttributes(
		attribute.Int("first_level_size", currentLevelFromObject.Size()),
		attribute.Int("terminal_type_size", usersetFromUser.Size()),
	))
	defer span.End()
	checkOutcomeChan := make(chan checkOutcome, c.concurrencyLimit)

	cancellableCtx, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	defer func() {
		cancel()
		// We need to wait always to avoid a goroutine leak.
		wg.Wait()
	}()
	visited := &sync.Map{}
	bfsReq := req.clone()
	wg.Add(1)
	go func() {
		c.breadthFirstRecursiveMatch(cancellableCtx, bfsReq, mapping, visited, currentLevelFromObject, usersetFromUser, condErrs, checkOutcomeChan)
		wg.Done()
	}()

	var finalErr error
	finalResult := &ResolveCheckResponse{
		Allowed: false,
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
	// context cancellation from upstream (e.g. client)
	if ctx.Err() != nil {
		finalErr = ctx.Err()
	}

	if !finalResult.GetAllowed() && ctx.Err() == nil {
		// Objects with a condition error only matter if the search reached them.
		unresolved := condErrs.filter(func(object string) bool {
			_, ok := visited.Load(object)
			return ok
		})
		if len(unresolved) > 0 {
			relation := req.GetTupleKey().GetRelation()
			if mapping.kind == storage.TTUKind {
				relation = mapping.computedRelation
			}
			resp, err := c.checkObjectsWithDefault(ctx, unresolved, func(object string) (*ResolveCheckRequest, string) {
				// Check the object from the depth the search reached it at, as the default strategy would.
				depth, _ := visited.Load(object)
				parentReq := req.clone()
				parentReq.GetRequestMetadata().Depth = depth.(uint32) - 1
				return parentReq, relation
			})
			// Without another error, resp is the answer, including any cycle found while checking.
			if err == nil && (resp.GetAllowed() || finalErr == nil) {
				return resp, nil
			}
			if finalErr == nil {
				finalErr = err
			}
		}
	}

	if finalErr != nil {
		return nil, finalErr
	}

	return finalResult, nil
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
// The visitedUserset map holds the depth each object searched was reached at.
// Objects in condErrs are not counted as a match, but are still searched through.
func (c *LocalChecker) breadthFirstRecursiveMatch(ctx context.Context, req *ResolveCheckRequest, mapping *recursiveMapping, visitedUserset *sync.Map, currentUsersetLevel *hashset.Set, usersetFromUser *hashset.Set, condErrs *objectSet, checkOutcomeChan chan checkOutcome) {
	if currentUsersetLevel.Size() == 0 || ctx.Err() != nil {
		// nothing else to search for or upstream cancellation
		close(checkOutcomeChan)
		return
	}
	req.GetRequestMetadata().Depth++
	if req.GetRequestMetadata().Depth == c.maxResolutionDepth {
		concurrency.TrySendThroughChannel(ctx, checkOutcome{err: ErrResolutionDepthExceeded}, checkOutcomeChan)
		close(checkOutcomeChan)
		return
	}
	relation := req.GetTupleKey().GetRelation()
	user := req.GetTupleKey().GetUser()

	pool := concurrency.NewPool(ctx, c.concurrencyLimit)
	mu := &sync.Mutex{}
	nextUsersetLevel := hashset.New()

	for _, usersetInterface := range currentUsersetLevel.Values() {
		userset := usersetInterface.(string)
		_, visited := visitedUserset.LoadOrStore(userset, req.GetRequestMetadata().Depth)
		if visited {
			continue
		}
		newReq := req.clone()
		newReq.TupleKey = tuple.NewTupleKey(userset, relation, user)
		mapper, err := buildRecursiveMapper(ctx, newReq, mapping)

		if err != nil {
			concurrency.TrySendThroughChannel(ctx, checkOutcome{err: err}, checkOutcomeChan)
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
				if usersetFromUser.Contains(userset) && !condErrs.has(userset) {
					concurrency.TrySendThroughChannel(ctx, checkOutcome{resp: &ResolveCheckResponse{
						Allowed: true,
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
	if err := pool.Wait(); errors.Is(err, ErrShortCircuit) {
		close(checkOutcomeChan)
		return
	}
	c.breadthFirstRecursiveMatch(ctx, req, mapping, visitedUserset, nextUsersetLevel, usersetFromUser, condErrs, checkOutcomeChan)
}
