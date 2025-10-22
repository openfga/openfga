package strategies

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/hashset"
	authzGraph "github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/iterator"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type Recursive struct {
	model     *check.AuthorizationModelGraph
	datastore storage.RelationshipTupleReader
}

func NewRecursive(model *check.AuthorizationModelGraph, ds storage.RelationshipTupleReader) *Recursive {
	return &Recursive{
		model:     model,
		datastore: ds,
	}
}

func (s *Recursive) Userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, rightIter storage.TupleKeyIterator) (*check.Response, error) {
	w2s := NewWeight2(s.model, s.datastore)
	objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	childReq, err := check.NewRequest(check.RequestParams{
		StoreID:                   req.GetStoreID(),
		TupleKey:                  tuple.NewTupleKey(tuple.BuildObject(objectType, "ignore"), relation, req.GetTupleKey().GetUser()),
		ContextualTuples:          req.GetContextualTuples(),
		Context:                   req.GetContext(),
		Consistency:               req.GetConsistency(),
		LastCacheInvalidationTime: req.GetLastCacheInvalidationTime(),
		AuthorizationModelID:      req.GetAuthorizationModelID(),
	})
	if err != nil {
		return nil, err
	}
	leftChan, err := w2s.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}

	stream := iterator.ToChannel[string](ctx, iterator.NewStream(0, leftChan), 100)
	return s.execute(ctx, req, edge, stream, iterator.ToChannel[string](ctx, storage.WrapIterator(storage.UsersetKind, rightIter), 100)

}

// recursiveTTU solves a union relation of the form "{operand1} OR ... {operandN} OR {recursive TTU}"
// rightIter gives the iterator for the recursive TTU.
func (s *Recursive) TTU(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, rightIter storage.TupleKeyIterator) (*check.Response, error) {
	w2s := NewWeight2(s.model, s.datastore)
	objectType, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	childReq, err := check.NewRequest(check.RequestParams{
		StoreID:                   req.GetStoreID(),
		TupleKey:                  tuple.NewTupleKey(objectType, computedRelation, req.GetTupleKey().GetUser()),
		ContextualTuples:          req.GetContextualTuples(),
		Context:                   req.GetContext(),
		Consistency:               req.GetConsistency(),
		LastCacheInvalidationTime: req.GetLastCacheInvalidationTime(),
		AuthorizationModelID:      req.GetAuthorizationModelID(),
	})
	if err != nil {
		return nil, err
	}
	leftChan, err := w2s.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}

	return s.execute(ctx, req, edge, leftChan, storage.WrapIterator(storage.TTUKind, rightIter))
}

func (s *Recursive) execute(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, leftChan chan iterator.Msg[string], rightChan chan iterator.Msg) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "recursiveFastPath")
	defer span.End()
	usersetFromUser := hashset.New()
	usersetFromObject := hashset.New()

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer rightIter.Stop()
	rightChan := iterator.ToChannel[string](ctx, rightIter, 100)

	res := &ResolveCheckResponse{
		Allowed: false,
	}

	// check to see if there are any recursive userset assigned. If not,
	// we don't even need to check the terminal type side.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case objectToUsersetMessage, ok := <-rightChan:
		if !ok {
			return res, ctx.Err()
		}
		if objectToUsersetMessage.err != nil {
			return nil, objectToUsersetMessage.err
		}
		usersetFromObject.Add(objectToUsersetMessage.userset)
	}

	userToUsersetMessageChan, err := objectProvider.Begin(cancellableCtx, req)
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
				if usersetFromUser.Size() == 0 {
					return res, ctx.Err()
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
		case objectToUsersetMessage, ok := <-rightChan:
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
		iter, err = ds.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
			Object:                      req.GetTupleKey().GetObject(),
			Relation:                    req.GetTupleKey().GetRelation(),
			AllowedUserTypeRestrictions: mapping.allowedUserTypeRestrictions,
		}, storage.ReadUsersetTuplesOptions{Consistency: consistencyOpts})
	case storage.TTUKind:
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
	return storage.WrapIterator(mapping.kind, filteredIter), nil
}

func (c *LocalChecker) recursiveMatchUserUserset(ctx context.Context, req *ResolveCheckRequest, mapping *recursiveMapping, currentLevelFromObject *hashset.Set, usersetFromUser *hashset.Set) (*ResolveCheckResponse, error) {
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
	wg.Add(1)
	go func() {
		c.breadthFirstRecursiveMatch(cancellableCtx, req, mapping, &sync.Map{}, currentLevelFromObject, usersetFromUser, checkOutcomeChan)
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
func (c *LocalChecker) breadthFirstRecursiveMatch(ctx context.Context, req *ResolveCheckRequest, mapping *recursiveMapping, visitedUserset *sync.Map, currentUsersetLevel *hashset.Set, usersetFromUser *hashset.Set, checkOutcomeChan chan checkOutcome) {
	req.GetRequestMetadata().Depth++
	if req.GetRequestMetadata().Depth == c.maxResolutionDepth {
		concurrency.TrySendThroughChannel(ctx, checkOutcome{err: ErrResolutionDepthExceeded}, checkOutcomeChan)
		close(checkOutcomeChan)
		return
	}
	if currentUsersetLevel.Size() == 0 || ctx.Err() != nil {
		// nothing else to search for or upstream cancellation
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
		_, visited := visitedUserset.LoadOrStore(userset, struct{}{})
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
				if usersetFromUser.Contains(userset) {
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
	c.breadthFirstRecursiveMatch(ctx, req, mapping, visitedUserset, nextUsersetLevel, usersetFromUser, checkOutcomeChan)
}
