package strategies

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/errgroup"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type Recursive struct {
	concurrencyLimit int
	bottomUp         *bottomUp
	model            *check.AuthorizationModelGraph
	datastore        storage.RelationshipTupleReader
}

func NewRecursive(model *check.AuthorizationModelGraph, ds storage.RelationshipTupleReader, limit int) *Recursive {
	return &Recursive{
		bottomUp:         newBottomUpRecursive(model, ds),
		model:            model,
		datastore:        ds,
		concurrencyLimit: limit,
	}
}

func (s *Recursive) Userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, rightIter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "recursive.Userset")
	defer span.End()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
	leftChan, err := s.bottomUp.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}

	return s.execute(ctx, req, edge, leftChan, storage.WrapIterator(storage.UsersetKind, rightIter))
}

// recursiveTTU solves a union relation of the form "{operand1} OR ... {operandN} OR {recursive TTU}"
// rightIter gives the iterator for the recursive TTU.
func (s *Recursive) TTU(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, rightIter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "recursive.TTU")
	defer span.End()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	objectType, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	childReq, err := check.NewRequest(check.RequestParams{
		StoreID:                   req.GetStoreID(),
		TupleKey:                  tuple.NewTupleKey(tuple.BuildObject(objectType, "ignore"), computedRelation, req.GetTupleKey().GetUser()),
		ContextualTuples:          req.GetContextualTuples(),
		Context:                   req.GetContext(),
		Consistency:               req.GetConsistency(),
		LastCacheInvalidationTime: req.GetLastCacheInvalidationTime(),
		AuthorizationModelID:      req.GetAuthorizationModelID(),
	})
	if err != nil {
		return nil, err
	}
	leftChan, err := s.bottomUp.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}

	return s.execute(ctx, req, edge, leftChan, storage.WrapIterator(storage.TTUKind, rightIter))
}

func (s *Recursive) execute(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, leftChan chan *iterator.Msg, rightIter storage.TupleMapper) (*check.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// right hand side bootstrap
	idsFromObject := make(map[string]struct{})
	defer rightIter.Stop() // the caller calls stop when creating the iterator, this is just being defensive
	rightChan := iterator.ToChannel[string](ctx, rightIter, s.concurrencyLimit)

	// right hand side bootstrap
	idsFromUser := make(map[string]struct{})
	defer iterator.Drain(leftChan)

	// NOTE: This loop initializes the terminal type and the first level of depth as this is a breadth first traversal.
	// To maintain simplicity the terminal type will be fully loaded, but it could arguably be loaded async.
	for leftChan != nil || rightChan != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-leftChan:
			if !ok {
				leftChan = nil
				if len(idsFromUser) == 0 {
					return &check.Response{Allowed: false}, nil
				}
				break
			}
			if msg.Err != nil {
				return nil, msg.Err
			}
			for {
				t, err := msg.Iter.Next(ctx)
				if err != nil {
					msg.Iter.Stop()
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					// TODO: should we continue?
					return nil, err
				}

				if _, exists := idsFromObject[t]; exists {
					return &check.Response{Allowed: true}, nil
				}
				idsFromUser[t] = struct{}{}
			}

		case msg, ok := <-rightChan:
			if !ok {
				// idsFromObject must not be empty because we would have caught it earlier.
				rightChan = nil
				if len(idsFromObject) == 0 {
					return &check.Response{Allowed: false}, nil
				}
				break
			}
			if msg.Err != nil {
				return nil, msg.Err
			}
			if _, exists := idsFromUser[msg.Value]; exists {
				return &check.Response{Allowed: true}, nil
			}
			idsFromObject[msg.Value] = struct{}{}
		}
	}

	return s.recursiveMatch(ctx, req, edge, idsFromUser, idsFromObject)
}

func (s *Recursive) recursiveMatch(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, idsFromUser map[string]struct{}, idsFromObject map[string]struct{}) (*check.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	responsesChan := make(chan check.ResponseMsg, s.concurrencyLimit) // needs to be buffered to prevent out of order closed events

	var err error
	conditionEdge := edge
	if edge.GetTuplesetRelation() != "" {
		subjectType, _ := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
		conditionEdge, err = s.model.GetDirectEdgeFromNodeForUserType(edge.GetTuplesetRelation(), subjectType)
		if err != nil {
			return nil, err
		}
	}

	go s.breadthFirstRecursiveMatch(ctx, req, edge, conditionEdge, &sync.Map{}, idsFromUser, idsFromObject, responsesChan)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-responsesChan:
			if !ok {
				return &check.Response{Allowed: false}, err
			}
			if msg.Err != nil {
				err = msg.Err
				continue
			}

			if msg.Res.Allowed {
				return msg.Res, nil
			}
		}
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
func (s *Recursive) breadthFirstRecursiveMatch(ctx context.Context, req *check.Request, edge, conditionEdge *authzGraph.WeightedAuthorizationModelEdge, visitedIds *sync.Map, idsFromUser, idsFromObjectToVisit map[string]struct{}, out chan check.ResponseMsg) {
	// TODO: How do we want to exit due to depth
	if len(idsFromObjectToVisit) == 0 || ctx.Err() != nil {
		// nothing else to search for or upstream cancellation
		close(out)
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pool := errgroup.Group{}
	pool.SetLimit(s.concurrencyLimit)
	mu := &sync.Mutex{}
	nextIdsFromObjectToVisit := make(map[string]struct{})

	for id := range idsFromObjectToVisit {
		_, visited := visitedIds.LoadOrStore(id, struct{}{})
		if visited {
			continue
		}

		pool.Go(func() error {
			iter, err := s.buildTupleMapperForID(ctx, req, edge, conditionEdge, id, visitedIds)
			if err != nil {
				return err
			}
			defer iter.Stop()
			for {
				t, err := iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						return nil
					}
					concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Err: err}, out)
					return nil
				}
				if _, exists := idsFromUser[t]; exists {
					concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Res: &check.Response{
						Allowed: true,
					}}, out)
					return concurrency.ErrShortCircuit // cancel will be propagated to the remaining goroutines
				}
				mu.Lock()
				nextIdsFromObjectToVisit[t] = struct{}{}
				mu.Unlock()
			}
		})
	}

	// wait for all checks to wrap up
	// if a match was found, clean up
	if err := pool.Wait(); errors.Is(err, concurrency.ErrShortCircuit) {
		close(out)
		return
	}
	s.breadthFirstRecursiveMatch(ctx, req, edge, conditionEdge, visitedIds, idsFromUser, nextIdsFromObjectToVisit, out)
}

func (s *Recursive) buildTupleMapperForID(ctx context.Context, req *check.Request, edge, conditionEdge *authzGraph.WeightedAuthorizationModelEdge, id string, visited *sync.Map) (storage.TupleMapper, error) {
	if ctx.Err() != nil { // short circuit whenever context is done
		return nil, ctx.Err()
	}
	consistencyOpts := storage.ConsistencyOptions{
		Preference: req.GetConsistency(),
	}
	var iter storage.TupleIterator
	var err error
	if edge.GetTuplesetRelation() != "" {
		subjectType, _ := tuple.SplitObjectRelation(conditionEdge.GetTo().GetUniqueLabel())
		iter, err = s.datastore.Read(ctx, req.GetStoreID(), storage.ReadFilter{
			Object:     id,
			Relation:   edge.GetTuplesetRelation(),
			User:       subjectType + ":",
			Conditions: conditionEdge.GetConditions(),
		}, storage.ReadOptions{Consistency: consistencyOpts})
	} else {
		objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
		iter, err = s.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
			Object:   id,
			Relation: relation,
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
				Type:               objectType,
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: relation},
			}},
			Conditions: conditionEdge.GetConditions(),
		}, storage.ReadUsersetTuplesOptions{Consistency: consistencyOpts})
	}
	if err != nil {
		return nil, err
	}

	var kind storage.TupleMapperKind
	var uniqueKeyFunc func(key *openfgav1.TupleKey) string
	if edge.GetTuplesetRelation() != "" {
		kind = storage.TTUKind
		uniqueKeyFunc = func(key *openfgav1.TupleKey) string {
			t, _ := storage.MapTTU(key)
			return t
		}
	} else {
		kind = storage.UsersetKind
		uniqueKeyFunc = func(key *openfgav1.TupleKey) string {
			t, _ := storage.MapUserset(key)
			return t
		}
	}

	iterFilters := make([]iterator.FilterFunc[*openfgav1.TupleKey], 0, 2)
	iterFilters = append(iterFilters, check.BuildUniqueTupleKeyFilter(visited, uniqueKeyFunc))
	if len(edge.GetConditions()) > 1 || edge.GetConditions()[0] != authzGraph.NoCond {
		iterFilters = append(iterFilters, check.BuildConditionTupleKeyFilter(ctx, s.model, edge, req.GetContext()))
	}
	i := iterator.NewFilteredIterator(storage.NewTupleKeyIteratorFromTupleIterator(iter), iterFilters...)
	return storage.WrapIterator(kind, i), nil
}
