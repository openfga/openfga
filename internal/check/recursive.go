package check

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type RecursiveType int8

const (
	RecursiveTypeUserset RecursiveType = 0
	RecursiveTypeTTU     RecursiveType = 1
)

type Recursive struct {
	concurrencyLimit int
	bottomUp         *bottomUp
	model            *modelgraph.AuthorizationModelGraph
	datastore        storage.RelationshipTupleReader
}

func NewRecursive(model *modelgraph.AuthorizationModelGraph, ds storage.RelationshipTupleReader, limit int) *Recursive {
	return &Recursive{
		bottomUp:         newBottomUpRecursive(model, ds),
		model:            model,
		datastore:        ds,
		concurrencyLimit: limit,
	}
}

func (s *Recursive) Userset(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, rightIter storage.TupleKeyIterator, _ *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "recursive.Userset")
	defer span.End()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	childReq := req.cloneWithTupleKey(tuple.NewTupleKey(tuple.BuildObject(objectType, "ignore"), relation, req.GetTupleKey().GetUser()))

	leftChan, err := s.bottomUp.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}

	return s.execute(ctx, req, edge, RecursiveTypeUserset, leftChan, storage.WrapIterator(storage.UsersetKind, rightIter))
}

// recursiveTTU solves a union relation of the form "{operand1} OR ... {operandN} OR {recursive TTU}"
// rightIter gives the iterator for the recursive TTU.
func (s *Recursive) TTU(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, rightIter storage.TupleKeyIterator, _ *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "recursive.TTU")
	defer span.End()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	objectType, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	childReq := req.cloneWithTupleKey(tuple.NewTupleKey(tuple.BuildObject(objectType, "ignore"), computedRelation, req.GetTupleKey().GetUser()))

	leftChan, err := s.bottomUp.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}

	return s.execute(ctx, req, edge, RecursiveTypeTTU, leftChan, storage.WrapIterator(storage.TTUKind, rightIter))
}

func (s *Recursive) execute(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, recursiveType RecursiveType, leftChan chan *iterator.Msg, rightIter storage.TupleMapper) (*Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// right hand side bootstrap
	idsFromObject := make(map[string]struct{})
	defer rightIter.Stop() // the caller calls stop when creating the iterator, this is just being defensive
	rightChan := iterator.ToChannel[string](ctx, rightIter, s.concurrencyLimit)

	// left hand side bootstrap
	idsFromUser := make(map[string]struct{})
	defer iterator.Drain(leftChan)

	// NOTE: This loop initializes the terminal type and the first level of depth as this is a breadth first traversal.
	// To maintain simplicity the terminal type will be fully loaded, but it could arguably be loaded async.
	var err error
	for leftChan != nil || rightChan != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-leftChan:
			if !ok {
				leftChan = nil
				// if no ids from the left side were returned then return false without error
				if len(idsFromUser) == 0 {
					return &Response{Allowed: false}, err
				}
				break
			}
			if msg.Err != nil {
				err = msg.Err
				// if no ids from the left side were returned then return false with error, there are no values to compare against the right side
				if len(idsFromUser) == 0 {
					return &Response{Allowed: false}, err
				}
				continue
			}
			for {
				t, errIter := msg.Iter.Next(ctx)
				if errIter != nil {
					msg.Iter.Stop()
					if !storage.IterIsDoneOrCancelled(errIter) {
						err = errIter
					}
					break
				}

				if _, exists := idsFromObject[t]; exists {
					return &Response{Allowed: true}, nil
				}
				idsFromUser[t] = struct{}{}
			}

		case msg, ok := <-rightChan:
			if !ok {
				rightChan = nil
				if len(idsFromObject) == 0 {
					return &Response{Allowed: false}, err
				}
				break
			}
			if msg.Err != nil {
				err = msg.Err
				continue
			}
			if _, exists := idsFromUser[msg.Value]; exists {
				return &Response{Allowed: true}, nil
			}
			idsFromObject[msg.Value] = struct{}{}
		}
	}

	res, errMatch := s.recursiveMatch(ctx, req, edge, recursiveType, idsFromUser, idsFromObject)
	if errMatch != nil {
		return res, errMatch
	}
	if res.Allowed {
		return res, nil
	}
	return res, err
}

func (s *Recursive) recursiveMatch(ctx context.Context, req *Request, recursiveEdge *authzGraph.WeightedAuthorizationModelEdge, recursiveType RecursiveType, idsFromUser, idsFromObject map[string]struct{}) (*Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	responsesChan := make(chan ResponseMsg, s.concurrencyLimit) // needs to be buffered to prevent out of order closed events

	var err error
	edge := recursiveEdge
	if recursiveType == RecursiveTypeTTU {
		subjectType, _ := tuple.SplitObjectRelation(recursiveEdge.GetTo().GetUniqueLabel())
		edge, err = s.model.GetDirectEdgeFromNodeForUserType(recursiveEdge.GetTuplesetRelation(), subjectType)
		if err != nil {
			return nil, err
		}
	}

	visited := &sync.Map{}

	var pool errgroup.Group
	pool.SetLimit(s.concurrencyLimit)

	for id := range idsFromObject {
		visited.Store(id, true)
		pool.Go(func() error {
			s.recursiveMatchResolver(ctx, req, edge, recursiveType, &pool, idsFromUser, visited, id, responsesChan)
			return nil
		})
	}

	go func() {
		_ = pool.Wait()
		close(responsesChan)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-responsesChan:
			if !ok {
				return &Response{Allowed: false}, err
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
func (s *Recursive) recursiveMatchResolver(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, recursiveType RecursiveType, pool *errgroup.Group, idsFromUser map[string]struct{}, visitedIds *sync.Map, id string, out chan ResponseMsg) {
	iter, err := s.buildTupleMapperForID(ctx, req, edge, recursiveType, id, visitedIds)
	if err != nil {
		concurrency.TrySendThroughChannel(ctx, ResponseMsg{Err: err}, out)
		return
	}
	defer iter.Stop()
	for {
		t, err := iter.Next(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				return
			}
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{Err: err}, out)
			return
		}
		if _, exists := idsFromUser[t]; exists {
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: &Response{Allowed: true}}, out)
			return // cancel will be propagated to the remaining goroutines
		}
		fn := func() error {
			s.recursiveMatchResolver(ctx, req, edge, recursiveType, pool, idsFromUser, visitedIds, t, out)
			return nil
		}
		if !pool.TryGo(fn) {
			_ = fn()
		}
	}
}

func (s *Recursive) buildTupleMapperForID(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, recursiveType RecursiveType, id string, visited *sync.Map) (storage.TupleMapper, error) {
	if ctx.Err() != nil { // short circuit whenever context is done
		return nil, ctx.Err()
	}
	consistencyOpts := storage.ConsistencyOptions{
		Preference: req.GetConsistency(),
	}
	var tIter storage.TupleIterator
	var ctxIter storage.TupleKeyIterator
	var kind storage.TupleMapperKind
	var uniqueKeyFunc func(key *openfgav1.TupleKey) string
	var err error

	if recursiveType == RecursiveTypeTTU {
		subjectType, _ := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
		_, relation := tuple.SplitObjectRelation(edge.GetFrom().GetUniqueLabel())
		tIter, err = s.datastore.Read(ctx, req.GetStoreID(), storage.ReadFilter{
			Object:     id,
			Relation:   relation,
			User:       subjectType + ":",
			Conditions: edge.GetConditions(),
		}, storage.ReadOptions{Consistency: consistencyOpts})

		if ctxTuples, ok := req.GetContextualTuplesByObjectID(id, relation, subjectType); ok {
			ctxIter = storage.NewStaticTupleKeyIterator(ctxTuples)
		}

		kind = storage.TTUKind
		uniqueKeyFunc = func(key *openfgav1.TupleKey) string {
			t, _ := storage.MapTTU(key)
			return t
		}
	} else {
		userObjectType, userRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
		tIter, err = s.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
			Object:   id,
			Relation: userRelation, // a recursive relation userset, the relation to where it belongs is the same where is going to
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
				Type:               userObjectType,
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: userRelation},
			}},
			Conditions: edge.GetConditions(),
		}, storage.ReadUsersetTuplesOptions{Consistency: consistencyOpts})
		if ctxTuples, ok := req.GetContextualTuplesByObjectID(id, userRelation, edge.GetTo().GetUniqueLabel()); ok {
			ctxIter = storage.NewStaticTupleKeyIterator(ctxTuples)
		}

		kind = storage.UsersetKind
		uniqueKeyFunc = func(key *openfgav1.TupleKey) string {
			t, _ := storage.MapUserset(key)
			return t
		}
	}
	if err != nil {
		return nil, err
	}
	iter := storage.NewTupleKeyIteratorFromTupleIterator(tIter)
	if ctxIter != nil {
		iter = iterator.Concat(ctxIter, iter)
	}
	iterFilters := make([]iterator.FilterFunc[*openfgav1.TupleKey], 0, 2)
	iterFilters = append(iterFilters, BuildUniqueTupleKeyFilter(visited, uniqueKeyFunc))
	conditions := edge.GetConditions()
	if len(conditions) > 0 && (len(conditions) > 1 || conditions[0] != authzGraph.NoCond) {
		iterFilters = append(iterFilters, BuildConditionTupleKeyFilter(ctx, s.model, conditions, req.GetContext()))
	}
	i := iterator.NewFilteredIterator(iter, iterFilters...)
	return storage.WrapIterator(kind, i), nil
}
