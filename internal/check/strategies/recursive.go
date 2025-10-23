package strategies

import (
	"context"
	"errors"
	"sync"

	"github.com/emirpasic/gods/sets/hashset"
	"golang.org/x/sync/errgroup"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

var ErrShortCircuit = errors.New("short circuit")

type Recursive struct {
	concurrencyLimit int
	model            *check.AuthorizationModelGraph
	datastore        storage.RelationshipTupleReader
}

func NewRecursive(model *check.AuthorizationModelGraph, ds storage.RelationshipTupleReader, limit int) *Recursive {
	return &Recursive{
		model:            model,
		datastore:        ds,
		concurrencyLimit: limit,
	}
}

func (s *Recursive) Userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, rightIter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "recursive.Userset")
	defer span.End()

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

	return s.execute(ctx, req, edge, leftChan, storage.WrapIterator(storage.UsersetKind, rightIter))
}

// recursiveTTU solves a union relation of the form "{operand1} OR ... {operandN} OR {recursive TTU}"
// rightIter gives the iterator for the recursive TTU.
func (s *Recursive) TTU(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, rightIter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "recursive.TTU")
	defer span.End()

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

func (s *Recursive) execute(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, leftChanSrc chan *iterator.Msg, rightIter storage.TupleMapper) (*check.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// right hand side bootstrap
	idsFromObject := hashset.New()
	defer rightIter.Stop() // the caller calls stop when creating the iterator, this is just being defensive
	rightChan := iterator.ToChannel[string](ctx, rightIter, s.concurrencyLimit)

	// check to see if there are any recursive userset assigned. If not,
	// we don't even need to check the terminal type side.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-rightChan:
		if !ok {
			return &check.Response{Allowed: false}, nil
		}
		if msg.Err != nil {
			return nil, msg.Err
		}
		idsFromObject.Add(msg.Value)
	}

	// right hand side bootstrap
	idsFromUser := hashset.New()
	stream := iterator.NewStream(0, leftChanSrc)
	defer stream.Stop()
	leftChan := iterator.ToChannel[string](ctx, stream, s.concurrencyLimit)

	leftDone := false
	rightDone := false

	// NOTE: This loop initializes the terminal type and the first level of depth as this is a breadth first traversal.
	// To maintain simplicity the terminal type will be fully loaded, but it could arguably be loaded async.
	for !leftDone || !rightDone {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-leftChan:
			if !ok {
				leftDone = true
				if idsFromUser.Size() == 0 {
					return &check.Response{Allowed: false}, nil
				}
				break
			}
			if msg.Err != nil {
				return nil, msg.Err
			}
			if processMessage(msg.Value, idsFromUser, idsFromObject) {
				return &check.Response{Allowed: true}, nil
			}
		case msg, ok := <-rightChan:
			if !ok {
				// idsFromObject must not be empty because we would have caught it earlier.
				rightDone = true
				break
			}
			if msg.Err != nil {
				return nil, msg.Err
			}
			if processMessage(msg.Value, idsFromObject, idsFromUser) {
				return &check.Response{Allowed: true}, nil
			}
		}
	}

	return s.recursiveMatch(ctx, req, edge, idsFromUser, idsFromObject)
}

func (s *Recursive) recursiveMatch(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, idsFromUser *hashset.Set, idsFromObject *hashset.Set) (*check.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	responsesChan := make(chan check.ResponseMsg, s.concurrencyLimit) // needs to be buffered to prevent out of order closed events

	go s.breadthFirstRecursiveMatch(ctx, req, edge, &sync.Map{}, idsFromUser, idsFromObject, responsesChan)

	var err error

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
func (s *Recursive) breadthFirstRecursiveMatch(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, visitedIds *sync.Map, idsFromUser *hashset.Set, idsFromObjectToVisit *hashset.Set, out chan check.ResponseMsg) {
	// TODO: How do we want to exit due to depth

	if idsFromObjectToVisit.Size() == 0 || ctx.Err() != nil {
		// nothing else to search for or upstream cancellation
		close(out)
		return
	}

	pool := errgroup.Group{}
	pool.SetLimit(s.concurrencyLimit)
	mu := &sync.Mutex{}
	nextIdsFromObjectToVisit := hashset.New()

	for _, idIface := range idsFromObjectToVisit.Values() {
		id := idIface.(string)
		_, visited := visitedIds.LoadOrStore(id, struct{}{})
		if visited {
			continue
		}

		consistencyOpts := storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		}
		var iter storage.TupleIterator
		var err error
		if edge.GetTuplesetRelation() != "" {
			iter, err = s.datastore.Read(ctx, req.GetStoreID(), tuple.NewTupleKey(id, edge.GetTuplesetRelation(), ""), storage.ReadOptions{Consistency: consistencyOpts})
		} else {
			objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
			iter, err = s.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
				Object:   id,
				Relation: req.GetTupleKey().GetRelation(),
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
					Type:               objectType,
					RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: relation},
				}},
			}, storage.ReadUsersetTuplesOptions{Consistency: consistencyOpts})
		}
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Err: err}, out)
			continue
		}

		conditionEdge, err := s.model.GetConditionsEdgeForUserType(tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation()), edge.GetTo().GetUniqueLabel())
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Err: err}, out)
			continue
		}

		i := storage.NewTupleKeyIteratorFromTupleIterator(iter)
		if len(conditionEdge.GetConditions()) > 1 || conditionEdge.GetConditions()[0] != "" {
			i = storage.NewConditionsFilteredTupleKeyIterator(i,
				check.BuildTupleKeyConditionFilter(ctx, s.model, conditionEdge, req.GetContext()),
			)
		}

		defer i.Stop()

		var mappedIter storage.TupleMapper
		if edge.GetTuplesetRelation() != "" {
			mappedIter = storage.WrapIterator(storage.TTUKind, i)
		} else {
			mappedIter = storage.WrapIterator(storage.UsersetKind, i)
		}
		pool.Go(func() error {
			for msg := range iterator.ToChannel(ctx, mappedIter, s.concurrencyLimit) {
				if msg.Err != nil {
					concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Err: msg.Err}, out)
					return nil
				}
				id := msg.Value
				if idsFromUser.Contains(id) {
					concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Res: &check.Response{
						Allowed: true,
					}}, out)
					return ErrShortCircuit // cancel will be propagated to the remaining goroutines
				}
				mu.Lock()
				nextIdsFromObjectToVisit.Add(id)
				mu.Unlock()
			}
			return nil
		})
	}

	// wait for all checks to wrap up
	// if a match was found, clean up
	if err := pool.Wait(); errors.Is(err, ErrShortCircuit) {
		close(out)
		return
	}
	s.breadthFirstRecursiveMatch(ctx, req, edge, visitedIds, idsFromUser, nextIdsFromObjectToVisit, out)
}
