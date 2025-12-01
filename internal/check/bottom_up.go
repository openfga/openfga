package check

import (
	"context"
	"errors"
	"fmt"

	"github.com/sourcegraph/conc/panics"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type strategyKind int64

const (
	normal strategyKind = iota
	recursive
)

type bottomUpHandler func(context.Context, []storage.Iterator[string], chan<- *iterator.Msg)

type bottomUp struct {
	model     *modelgraph.AuthorizationModelGraph
	datastore storage.RelationshipTupleReader
	strategy  strategyKind
}

func newBottomUp(model *modelgraph.AuthorizationModelGraph, ds storage.RelationshipTupleReader) *bottomUp {
	return &bottomUp{
		model:     model,
		datastore: ds,
		strategy:  normal,
	}
}

func newBottomUpRecursive(model *modelgraph.AuthorizationModelGraph, ds storage.RelationshipTupleReader) *bottomUp {
	return &bottomUp{
		model:     model,
		datastore: ds,
		strategy:  recursive,
	}
}

// setOperationSetup returns a channel with a number of elements that is >= the number of children.
// Each element is an iterator.
// The caller must wait until the channel is closed.
func (s *bottomUp) setOperationSetup(ctx context.Context, req *Request, resolver bottomUpHandler, edges []*authzGraph.WeightedAuthorizationModelEdge) (chan *iterator.Msg, error) {
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
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: fmt.Errorf("%w: %s", ErrPanicRequest, recoveredError.AsError())}, out)
		}
	}()
	return out, nil
}

func (s *bottomUp) resolveEdge(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) (chan *iterator.Msg, error) {
	switch edge.GetEdgeType() {
	case authzGraph.DirectEdge:
		switch edge.GetTo().GetNodeType() {
		case authzGraph.SpecificType:
			return s.specificType(ctx, req, edge)
		case authzGraph.SpecificTypeWildcard:
			return s.specificTypeWildcard(ctx, req, edge)
		default:
			return nil, ErrPanicRequest
		}
	case authzGraph.RewriteEdge, authzGraph.ComputedEdge, authzGraph.TTULogicalEdge, authzGraph.DirectLogicalEdge:
		return s.resolveRewrite(ctx, req, edge.GetTo())
	default:
		return nil, ErrPanicRequest
	}
}

// resolveRewrite returns a channel that will contain an unknown but finite number of elements.
// The channel is closed at the end.
func (s *bottomUp) resolveRewrite(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode) (chan *iterator.Msg, error) {
	switch node.GetNodeType() {
	case authzGraph.SpecificTypeAndRelation, authzGraph.LogicalDirectGrouping, authzGraph.LogicalTTUGrouping:
		return s.setFlattenOperation(ctx, req, node)
	case authzGraph.OperatorNode:
		switch node.GetLabel() {
		case authzGraph.UnionOperator:
			return s.setFlattenOperation(ctx, req, node)
		case authzGraph.IntersectionOperator:
			edges, ok := s.model.GetEdgesFromNode(node)
			if !ok {
				return nil, ErrPanicRequest
			}
			return s.setOperationSetup(ctx, req, resolveIntersection, edges)
		case authzGraph.ExclusionOperator:
			edges, ok := s.model.GetEdgesFromNode(node)
			if !ok {
				return nil, ErrPanicRequest
			}
			return s.setOperationSetup(ctx, req, resolveDifference, edges)
		default:
			return nil, ErrPanicRequest
		}
	default:
		return nil, ErrPanicRequest
	}
}

func (s *bottomUp) setFlattenOperation(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode) (chan *iterator.Msg, error) {
	var err error
	var edges []*authzGraph.WeightedAuthorizationModelEdge
	edges, err = s.model.FlattenNode(node, req.GetUserType(), s.strategy == recursive)
	if err != nil {
		return nil, err
	}
	return s.setOperationSetup(ctx, req, resolveUnion, edges)
}

// specificType assumes that req.Object + req.Relation is a directly assignable relation, e.g. define viewer: [user, user:*].
// It returns a channel with one element, and then closes the channel.
// The element is an iterator over all objects that are directly related to the user or the wildcard (if applicable).
// TODO: DETERMINE IF ITS WORTH WAITING FOR RESULTS OF RIGHT HAND SIDE TO PERFORM BOUNDED QUERIES RATHER THAN THE FULL SET OF OBJECTIDS (BASICALLY INTERSECTION AT THE DATASTORE LEVEL).
func (s *bottomUp) specificType(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) (chan *iterator.Msg, error) {
	opts := storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}
	objectType, relation := tuple.SplitObjectRelation(edge.GetRelationDefinition())
	tIter, err := s.datastore.ReadStartingWithUser(ctx, req.GetStoreID(),
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

	iter := storage.NewTupleKeyIteratorFromTupleIterator(tIter)
	if ctxTuples, ok := req.GetContextualTuplesByUserID(req.GetTupleKey().GetUser(), relation, objectType); ok {
		iter = iterator.Merge(iter, storage.NewStaticTupleKeyIterator(ctxTuples), func(a, b *openfgav1.TupleKey) int {
			if a.GetObject() < b.GetObject() {
				return -1
			}
			return 1
		})
	}
	iterFilters := make([]iterator.FilterFunc[*openfgav1.TupleKey], 0, 1)
	if len(edge.GetConditions()) > 1 || edge.GetConditions()[0] != authzGraph.NoCond {
		iterFilters = append(iterFilters, BuildConditionTupleKeyFilter(ctx, s.model, edge.GetConditions(), req.GetContext()))
	}
	i := iterator.NewFilteredIterator(iter, iterFilters...)
	iterChan := make(chan *iterator.Msg, 1)
	if !concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.WrapIterator(storage.ObjectIDKind, i)}, iterChan) {
		iter.Stop() // will not be received to be cleaned up
	}
	close(iterChan)
	return iterChan, nil
}

func (s *bottomUp) specificTypeWildcard(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) (chan *iterator.Msg, error) {
	opts := storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}
	objectType, relation := tuple.SplitObjectRelation(edge.GetRelationDefinition())
	tIter, err := s.datastore.ReadStartingWithUser(ctx, req.GetStoreID(),
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

	iter := storage.NewTupleKeyIteratorFromTupleIterator(tIter)
	if ctxTuples, ok := req.GetContextualTuplesByUserID(tuple.TypedPublicWildcard(req.GetUserType()), relation, objectType); ok {
		iter = iterator.Merge(iter, storage.NewStaticTupleKeyIterator(ctxTuples), func(a, b *openfgav1.TupleKey) int {
			if a.GetObject() < b.GetObject() {
				return -1
			}
			return 1
		})
	}
	iterFilters := make([]iterator.FilterFunc[*openfgav1.TupleKey], 0, 1)
	if len(edge.GetConditions()) > 1 || edge.GetConditions()[0] != authzGraph.NoCond {
		iterFilters = append(iterFilters, BuildConditionTupleKeyFilter(ctx, s.model, edge.GetConditions(), req.GetContext()))
	}
	i := iterator.NewFilteredIterator(iter, iterFilters...)

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
	var condErr *condition.EvaluationError
	if errors.As(err, &condErr) {
		return false
	}
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
	var minIndexValue int
	var initialized bool

	if ctx.Err() != nil {
		return
	}

	compareValues := make([]string, 0, len(iters))     // these values are the head values of each iterator
	reverseLookupIndexes := make([]int, 0, len(iters)) // these values are the iters index that are currently active, for example 0, 1, 2, 3, 4
	for idx, iter := range iters {
		value, err := iter.Next(ctx)
		if err != nil {
			if handleStreamError(ctx, err, &batch, out) {
				return
			}
		}
		compareValues = append(compareValues, value)
		reverseLookupIndexes = append(reverseLookupIndexes, idx)
	}

	/*
		collect iterators from all channels, until all drained
		start performing union algorithm across the heads, if an iterator is empty, poll once again the source
		ask to see if the channel has a new iterator, otherwise consider it done
	*/
	// Perform a merge-sort style union
	for len(reverseLookupIndexes) > 0 {
		if ctx.Err() != nil {
			return
		}

		minValue = ""
		minIndexValue = 0
		initialized = false
		newIndexes := make([]int, 0, len(reverseLookupIndexes))
		for _, idxValue := range reverseLookupIndexes {
			value := compareValues[idxValue]

			if !initialized || value < minValue {
				minValue = value
				minIndexValue = idxValue
				initialized = true
			} else if value == minValue {
				v1, err := iters[idxValue].Next(ctx)
				if err != nil {
					if handleStreamError(ctx, err, &batch, out) {
						return
					}
					continue
				} else {
					compareValues[idxValue] = v1
				}
			}
			newIndexes = append(newIndexes, idxValue)
		}

		batch = addValueToBatch(minValue, batch, ctx, out)
		// Advance the stream with the minimum value
		value, err := iters[minIndexValue].Next(ctx)
		if err != nil {
			if handleStreamError(ctx, err, &batch, out) {
				return
			}
			// Remove the value index from activeIndexes
			newActiveIndexes := make([]int, 0, len(newIndexes))
			for _, idxValue := range newIndexes {
				if idxValue != minIndexValue {
					newActiveIndexes = append(newActiveIndexes, idxValue)
				}
			}
			reverseLookupIndexes = newActiveIndexes
		} else {
			compareValues[minIndexValue] = value
			reverseLookupIndexes = newIndexes
		}
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
	if childrenTotal == 0 || ctx.Err() != nil {
		return
	}

	compareValues := make([]string, 0, len(iters)) // these values are the head values of each iterator
	for _, iter := range iters {
		value, err := iter.Next(ctx)
		if err != nil {
			handleStreamError(ctx, err, &batch, out)
			// if one iterator is empty or error, interception is impossible
			return
		}
		compareValues = append(compareValues, value)
	}

	var maxValue string
	var allSameValue bool
	var initialized bool

	for {
		if ctx.Err() != nil {
			return
		}

		maxValue = ""
		allSameValue = true
		initialized = false

		for _, value := range compareValues {
			if !initialized {
				maxValue = value
				initialized = true
			} else if maxValue != value {
				allSameValue = false
				if maxValue < value {
					maxValue = value
				}
			}
		}

		// all children have the same value
		if allSameValue {
			// All streams have the same value - it's in the intersection
			batch = addValueToBatch(maxValue, batch, ctx, out)
			// Advance all streams
			for idx, iter := range iters {
				value, err := iter.Next(ctx)
				if err != nil {
					handleStreamError(ctx, err, &batch, out)
					// if one iterator is done or error, interception cannot continue
					return
				}
				compareValues[idx] = value
			}
		} else {
			// Not all values are equal - advance all iterators with smaller values to the max value
			for idx, iter := range iters {
				if compareValues[idx] < maxValue {
					value, err := skipTo(ctx, iter, maxValue)
					if err != nil {
						handleStreamError(ctx, err, &batch, out)
						// if one iterator is done or error, interception cannot continue
						return
					}
					compareValues[idx] = value
				}
			}
		}
	}
}

func resolveDifference(ctx context.Context, iters []storage.Iterator[string], out chan<- *iterator.Msg) {
	batch := make([]string, 0)

	defer func() {
		cleanOperation(ctx, batch, iters, out)
	}()

	if ctx.Err() != nil {
		return
	}

	compareValues := make([]string, 0, len(iters))
	for idx, iter := range iters {
		value, err := iter.Next(ctx)
		compareValues = append(compareValues, value)
		if err != nil {
			if handleStreamError(ctx, err, &batch, out) {
				return
			}
			if idx == BaseIndex {
				return // if base value is done or has any error, difference cannot continue
			}
			goto drainBase
		}
	}

	if len(iters) == 1 {
		goto drainBase
	}

	// both base and difference are still remaining
	for {
		if ctx.Err() != nil {
			return
		}

		// move both iterator heads
		if compareValues[BaseIndex] == compareValues[DifferenceIndex] {
			// Advance both iterators
			for idx, it := range iters {
				value, err := it.Next(ctx)
				if err != nil {
					if handleStreamError(ctx, err, &batch, out) {
						return
					}
					if idx == DifferenceIndex {
						goto drainBase
					}
					return
				}
				compareValues[idx] = value
			}
		} else if compareValues[DifferenceIndex] > compareValues[BaseIndex] {
			// add the base value to the batch
			batch = addValueToBatch(compareValues[BaseIndex], batch, ctx, out)
			// move the iterator and update the comparable value of base
			value, err := iters[BaseIndex].Next(ctx)
			if err != nil {
				// if base has an error, the difference operation cannot continue
				handleStreamError(ctx, err, &batch, out)
				return
			}
			compareValues[BaseIndex] = value
		} else {
			// diff < base, then move the diff to catch up with base
			value, err := skipTo(ctx, iters[DifferenceIndex], compareValues[BaseIndex])
			if err != nil {
				if handleStreamError(ctx, err, &batch, out) {
					return
				}
				goto drainBase
			}
			compareValues[DifferenceIndex] = value
		}
	}

drainBase:
	// drain the base
	for {
		batch = addValueToBatch(compareValues[BaseIndex], batch, ctx, out)
		value, err := iters[BaseIndex].Next(ctx)
		if err != nil {
			handleStreamError(ctx, err, &batch, out)
			return
		}
		compareValues[BaseIndex] = value
	}
}

func skipTo(ctx context.Context, iter storage.Iterator[string], target string) (string, error) {
	for {
		t, err := iter.Next(ctx)
		if err != nil {
			return "", err
		}

		// If current head >= target, we're done
		if t >= target {
			return t, nil
		}
	}
}
