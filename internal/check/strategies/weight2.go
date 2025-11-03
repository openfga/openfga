package strategies

import (
	"context"
	"fmt"

	"github.com/sourcegraph/conc/panics"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

const IteratorMinBatchThreshold = 100
const BaseIndex = 0
const DifferenceIndex = 1

type Weight2 struct {
	model     *check.AuthorizationModelGraph
	datastore storage.RelationshipTupleReader
}

func NewWeight2(model *check.AuthorizationModelGraph, ds storage.RelationshipTupleReader) *Weight2 {
	return &Weight2{
		model:     model,
		datastore: ds,
	}
}

func (s *Weight2) Userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "weight2.Userset")
	defer span.End()

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
	leftChan, err := s.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}
	return s.execute(ctx, leftChan, storage.WrapIterator(storage.UsersetKind, iter))
}

func (s *Weight2) TTU(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "weight2.Userset")
	defer span.End()

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

	leftChan, err := s.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}

	return s.execute(ctx, leftChan, storage.WrapIterator(storage.TTUKind, iter))
}

// Weight2 attempts to find the intersection across 2 producers (channels) of ObjectIDs.
// In the case of a TTU:
// Right channel is the result set of the Read of ObjectID/Relation that yields the User's ObjectID.
// Left channel is the result set of ReadStartingWithUser of User/Relation that yields Object's ObjectID.
// From the perspective of the model, the left hand side of a TTU is the computed relationship being expanded.
func (s *Weight2) execute(ctx context.Context, leftChan chan *iterator.Msg, rightIter storage.TupleMapper) (*check.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer rightIter.Stop()
	defer iterator.Drain(leftChan)

	// Set to store already seen values from each side
	// We use maps for O(1) lookup complexity, consistent with hashset implementation
	leftSeen := make(map[string]struct{})
	rightSeen := make(map[string]struct{})

	// Convert right iterator to channel for uniform processing
	rightChan := iterator.ToChannel[string](ctx, rightIter, IteratorMinBatchThreshold)

	var lastErr error

	// Process both channels concurrently
	for leftChan != nil || rightChan != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		// Process an item from the left channel
		case leftMsg, ok := <-leftChan:
			if !ok {
				leftChan = nil
				if len(leftSeen) == 0 {
					// If we've processed nothing from the left side, we can't have any intersection
					return &check.Response{Allowed: false}, lastErr
				}
				continue
			}

			if leftMsg.Err != nil {
				lastErr = leftMsg.Err
				continue
			}

			// Process all items from this iterator message
			if leftMsg.Iter != nil {
				for {
					t, err := leftMsg.Iter.Next(ctx)
					if err != nil {
						leftMsg.Iter.Stop()
						if storage.IterIsDoneOrCancelled(err) {
							break
						}
						lastErr = err
						continue
					}

					// Check if this value exists in the right set first (early match)
					if _, exists := rightSeen[t]; exists {
						leftMsg.Iter.Stop() // Stop the iterator early
						return &check.Response{Allowed: true}, nil
					}

					// Otherwise, store for future comparison
					leftSeen[t] = struct{}{}
				}
			}

		// Process an item from the right channel
		case rightMsg, ok := <-rightChan:
			if !ok {
				rightChan = nil
				continue
			}

			if rightMsg.Err != nil {
				lastErr = rightMsg.Err
				continue
			}

			// Check if this value exists in the left set first (early match)
			if _, exists := leftSeen[rightMsg.Value]; exists {
				return &check.Response{Allowed: true}, nil
			}

			// Otherwise, store for future comparison
			rightSeen[rightMsg.Value] = struct{}{}
		}
	}

	// If we get here, no match was found
	return &check.Response{Allowed: false}, lastErr
}

// setOperationSetup returns a channel with a number of elements that is >= the number of children.
// Each element is an iterator.
// The caller must wait until the channel is closed.
func (s *Weight2) setOperationSetup(ctx context.Context, req *check.Request, resolver weight2Handler, edges []*authzGraph.WeightedAuthorizationModelEdge) (chan *iterator.Msg, error) {
	iterChans := make([]storage.Iterator[string], 0, len(edges))
	for _, edge := range edges {
		if _, ok := edge.GetWeight(req.GetUserType()); !ok {
			continue
		}
		producerChan, err := s.resolveEdge(ctx, req, edge)
		if err != nil {
			return nil, err
		}
		iterChans = append(iterChans, iterator.FromChannel(producerChan))
	}

	out := make(chan *iterator.Msg, len(iterChans))
	go func() {
		recoveredError := panics.Try(func() {
			resolver(ctx, iterChans, out)
		})

		if recoveredError != nil {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: fmt.Errorf("%w: %s", check.ErrPanicRequest, recoveredError.AsError())}, out)
		}
	}()
	return out, nil
}

func (s *Weight2) resolveEdge(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge) (chan *iterator.Msg, error) {
	switch edge.GetEdgeType() {
	case authzGraph.DirectEdge:
		switch edge.GetTo().GetNodeType() {
		case authzGraph.SpecificType:
			return s.specificType(ctx, req, edge)
		case authzGraph.SpecificTypeWildcard:
			return s.specificTypeWildcard(ctx, req, edge)
		default:
			return nil, check.ErrPanicRequest
		}
	case authzGraph.RewriteEdge, authzGraph.ComputedEdge:
		return s.resolveRewrite(ctx, req, edge.GetTo())
	default:
		return nil, check.ErrPanicRequest
	}
}

// resolveRewrite returns a channel that will contain an unknown but finite number of elements.
// The channel is closed at the end.
func (s *Weight2) resolveRewrite(ctx context.Context, req *check.Request, node *authzGraph.WeightedAuthorizationModelNode) (chan *iterator.Msg, error) {
	switch node.GetNodeType() {
	case authzGraph.SpecificTypeAndRelation:
		return s.setFlattenOperation(ctx, req, node)
	case authzGraph.OperatorNode:
		switch node.GetLabel() {
		case authzGraph.UnionOperator:
			return s.setFlattenOperation(ctx, req, node)
		case authzGraph.IntersectionOperator:
			edges, ok := s.model.GetEdgesFromNode(node)
			if !ok {
				return nil, check.ErrPanicRequest
			}
			return s.setOperationSetup(ctx, req, resolveIntersection, edges)
		case authzGraph.ExclusionOperator:
			edges, ok := s.model.GetEdgesFromNode(node)
			if !ok {
				return nil, check.ErrPanicRequest
			}
			return s.setOperationSetup(ctx, req, resolveDifference, edges)
		default:
			return nil, check.ErrPanicRequest
		}
	default:
		return nil, check.ErrPanicRequest
	}
}

func (s *Weight2) setFlattenOperation(ctx context.Context, req *check.Request, node *authzGraph.WeightedAuthorizationModelNode) (chan *iterator.Msg, error) {
	edges, err := s.model.FlattenNode(node, req.GetUserType())
	if err != nil {
		return nil, err
	}
	return s.setOperationSetup(ctx, req, resolveUnion, edges)
}

// specificType assumes that req.Object + req.Relation is a directly assignable relation, e.g. define viewer: [user, user:*].
// It returns a channel with one element, and then closes the channel.
// The element is an iterator over all objects that are directly related to the user or the wildcard (if applicable).
// TODO: DETERMINE IF ITS WORTH WAITING FOR RESULTS OF RIGHT HAND SIDE TO PERFORM BOUNDED QUERIES RATHER THAN THE FULL SET OF OBJECTIDS (BASICALLY INTERSECTION AT THE DATASTORE LEVEL).
func (s *Weight2) specificType(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge) (chan *iterator.Msg, error) {
	opts := storage.ReadStartingWithUserOptions{
		WithResultsSortedAscending: true,
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}
	objectType, relation := tuple.SplitObjectRelation(edge.GetFrom().GetUniqueLabel())
	iter, err := s.datastore.ReadStartingWithUser(ctx, req.GetStoreID(),
		storage.ReadStartingWithUserFilter{
			ObjectType: objectType,
			Relation:   relation,
			UserFilter: []*openfgav1.ObjectRelation{{
				Object: req.GetTupleKey().GetUser(),
			}},
			Conditions: edge.GetConditions(),
		}, opts)
	if err != nil {
		return nil, err
	}

	iterFilters := make([]iterator.FilterFunc[*openfgav1.TupleKey], 0, 1)
	if len(edge.GetConditions()) > 1 || edge.GetConditions()[0] != authzGraph.NoCond {
		iterFilters = append(iterFilters, check.BuildConditionTupleKeyFilter(ctx, s.model, edge, req.GetContext()))
	}
	i := iterator.NewFilteredIterator(storage.NewTupleKeyIteratorFromTupleIterator(iter), iterFilters...)

	iterChan := make(chan *iterator.Msg, 1)
	if !concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.WrapIterator(storage.ObjectIDKind, i)}, iterChan) {
		iter.Stop() // will not be received to be cleaned up
	}
	close(iterChan)
	return iterChan, nil
}

func (s *Weight2) specificTypeWildcard(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge) (chan *iterator.Msg, error) {
	opts := storage.ReadStartingWithUserOptions{
		WithResultsSortedAscending: true,
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}
	objectType, relation := tuple.SplitObjectRelation(edge.GetFrom().GetUniqueLabel())
	iter, err := s.datastore.ReadStartingWithUser(ctx, req.GetStoreID(),
		storage.ReadStartingWithUserFilter{
			ObjectType: objectType,
			Relation:   relation,
			UserFilter: []*openfgav1.ObjectRelation{{
				Object: tuple.TypedPublicWildcard(req.GetUserType()),
			}},
			Conditions: edge.GetConditions(),
		}, opts)
	if err != nil {
		return nil, err
	}

	iterFilters := make([]iterator.FilterFunc[*openfgav1.TupleKey], 0, 1)
	if len(edge.GetConditions()) > 1 || edge.GetConditions()[0] != authzGraph.NoCond {
		iterFilters = append(iterFilters, check.BuildConditionTupleKeyFilter(ctx, s.model, edge, req.GetContext()))
	}
	i := iterator.NewFilteredIterator(storage.NewTupleKeyIteratorFromTupleIterator(iter), iterFilters...)

	iterChan := make(chan *iterator.Msg, 1)
	if !concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.WrapIterator(storage.ObjectIDKind, i)}, iterChan) {
		iter.Stop() // will not be received to be cleaned up
	}
	close(iterChan)
	return iterChan, nil
}

func handleStreamError(ctx context.Context, err error, batch *[]string, out chan<- *iterator.Msg) bool {
	if storage.IterIsDoneOrCancelled(err) {
		return false
	}

	concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, out)
	*batch = nil
	return true
}

func addValueToBatch(value string, batch []string, ctx context.Context, out chan<- *iterator.Msg) []string {
	if value == "" {
		return batch
	}
	batch = append(batch, value)
	// Flush batch if needed
	if len(batch) >= IteratorMinBatchThreshold {
		concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.NewStaticIterator[string](batch)}, out)
		batch = make([]string, 0, IteratorMinBatchThreshold)
	}
	return batch
}

func cleanOperation(ctx context.Context, batch []string, iters []storage.Iterator[string], out chan<- *iterator.Msg) {
	// Flush any remaining items
	if len(batch) > 0 {
		concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.NewStaticIterator[string](batch)}, out)
	}
	close(out)
	for _, it := range iters {
		it.Stop()
	}
}

// resolveUnion implements a merge-style algorithm for the union of sorted iterators
// handleStreamError sends an error through the output channel, sets batch to nil,
// and returns true if the function should terminate.
func resolveUnion(ctx context.Context, iters []storage.Iterator[string], out chan<- *iterator.Msg) {
	batch := make([]string, 0, IteratorMinBatchThreshold)

	defer func() {
		cleanOperation(ctx, batch, iters, out)
	}()

	var minValue string
	var minStreamIdx int
	var initialized bool

	/*
		collect iterators from all channels, until all drained
		start performing union algorithm across the heads, if an iterator is empty, poll once again the source
		ask to see if the channel has a new iterator, otherwise consider it done
	*/
	// Perform a merge-sort style union
	for len(iters) > 0 {
		if ctx.Err() != nil {
			return
		}

		// Find minimum value across all streams
		minValue = ""
		minStreamIdx = 0
		initialized = false

		activeIters := make([]storage.Iterator[string], 0, len(iters))
		for idx, iter := range iters {
			value, err := iter.Head(ctx)
			if err != nil {
				if handleStreamError(ctx, err, &batch, out) {
					return
				}
				continue
			} else {
				activeIters = append(activeIters, iter)
			}

			// ove any other iterators that the value is already capture in the head of the another iterator
			if initialized && value == minValue {
				_, err = iter.Next(ctx)
				if err != nil && handleStreamError(ctx, err, &batch, out) {
					return
				}
			}

			// initialize
			if !initialized || value < minValue {
				minValue = value
				minStreamIdx = idx
				initialized = true
			}
		}

		if !initialized {
			iters = activeIters
			// All streams were done or one iterator is done and we need to verify the active streams
			continue
		}
		batch = addValueToBatch(minValue, batch, ctx, out)

		// Advance the stream with the minimum value
		if _, err := iters[minStreamIdx].Next(ctx); err != nil {
			if handleStreamError(ctx, err, &batch, out) {
				return
			}
		}
		iters = activeIters
	}
}

func resolveIntersection(ctx context.Context, iters []storage.Iterator[string], out chan<- *iterator.Msg) {
	batch := make([]string, 0, IteratorMinBatchThreshold)

	defer func() {
		cleanOperation(ctx, batch, iters, out)
	}()

	// collect iterators from all channels, once none are nil
	// start performing intersection algorithm across the heads, if an iterator is drained
	// ask to see if the channel has a new iterator, otherwise consider it done
	// exit if one of the channels closes as there is no more possible intersection of all

	childrenTotal := len(iters)
	if childrenTotal == 0 {
		return
	}

	var maxValue string
	var allIters bool
	var allSameValue bool
	var initialized bool

	for len(iters) == childrenTotal {
		if ctx.Err() != nil {
			return
		}

		maxValue = ""
		allIters = true
		allSameValue = true
		initialized = false
		activeIters := make([]storage.Iterator[string], 0, len(iters))
		for _, iter := range iters {
			v, err := iter.Head(ctx)
			if err != nil {
				if handleStreamError(ctx, err, &batch, out) {
					return
				}
				allIters = false
				// we need to ensure we have all iterators at all times
				break
			}

			activeIters = append(activeIters, iter)

			if !initialized {
				maxValue = v
				initialized = true
			} else if maxValue != v {
				allSameValue = false
				if maxValue < v {
					maxValue = v
				}
			}
		}

		if !allIters {
			iters = activeIters
			// we need to ensure we have all iterators at all times
			continue
		}

		// all children have the same value
		if allSameValue {
			// All streams have the same value - it's in the intersection
			batch = addValueToBatch(maxValue, batch, ctx, out)
			// Advance all streams
			for _, iter := range iters {
				_, err := iter.Next(ctx)
				if err != nil && handleStreamError(ctx, err, &batch, out) {
					return
				}
			}
		} else {
			// Not all values are equal - advance all streams with smaller values to the max value
			for _, iter := range iters {
				err := iterator.SkipTo(ctx, iter, maxValue)
				if err != nil && handleStreamError(ctx, err, &batch, out) {
					return
				}
			}
		}

		iters = activeIters
	}
}

func resolveDifference(ctx context.Context, iters []storage.Iterator[string], out chan<- *iterator.Msg) {
	batch := make([]string, 0)

	defer func() {
		cleanOperation(ctx, batch, iters, out)
	}()

	baseIter := iters[BaseIndex]

	// both base and difference are still remaining
	for len(iters) == 2 {
		if ctx.Err() != nil {
			return
		}

		allIters := true
		baseValue := ""
		diffValue := ""
		activeIters := make([]storage.Iterator[string], 0, len(iters))
		for idx, iter := range iters {
			v, err := iter.Head(ctx)
			if err != nil {
				if handleStreamError(ctx, err, &batch, out) {
					return
				}
				allIters = false
				break
			}
			activeIters = append(activeIters, iter)
			if idx == BaseIndex {
				baseValue = v
			}
			if idx == DifferenceIndex {
				diffValue = v
			}
		}

		if !allIters {
			iters = activeIters
			// we need to ensure we have all iterators at all times
			continue
		}

		// move both iterator heads
		if baseValue == diffValue {
			// Advance all streams
			for _, i := range iters {
				_, err := i.Next(ctx)
				if err != nil && handleStreamError(ctx, err, &batch, out) {
					return
				}
			}
		} else if diffValue > baseValue {
			batch = addValueToBatch(baseValue, batch, ctx, out)
			_, err := iters[BaseIndex].Next(ctx)
			if err != nil && handleStreamError(ctx, err, &batch, out) {
				return
			}
		} else {
			// diff < base, then move the diff to catch up with base
			err := iterator.SkipTo(ctx, iters[DifferenceIndex], baseValue)
			if err != nil && handleStreamError(ctx, err, &batch, out) {
				return
			}
		}

		iters = activeIters
	}

	// drain the base
	if len(iters) == 1 && iters[BaseIndex] == baseIter {
		for {
			t, err := baseIter.Next(ctx)
			if err != nil {
				if handleStreamError(ctx, err, &batch, out) {
					return
				}
				break
			}
			batch = addValueToBatch(t, batch, ctx, out)
		}
	}
}
