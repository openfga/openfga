package check

import (
	"context"
	"fmt"
	"slices"

	"github.com/sourcegraph/conc/panics"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
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

func (s *bottomUp) setWildcardIntersectionSetup(ctx context.Context, req *Request, edges []*authzGraph.WeightedAuthorizationModelEdge) (chan *iterator.Msg, error) {
	shortcircuitIntersection := false

	// if there is one edge that does not contain the wildcard, we can short-circuit
	for _, edge := range edges {
		if !slices.Contains(edge.GetWildcards(), req.GetUserType()) {
			shortcircuitIntersection = true
			break
		}
	}

	if !shortcircuitIntersection {
		return s.setOperationSetup(ctx, req, resolveIntersection, edges)
	}

	iterChan := make(chan *iterator.Msg)
	close(iterChan)
	return iterChan, nil
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
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: fmt.Errorf("%w: %w", ErrPanicRequest, recoveredError.AsError())}, out)
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
			// the request cannot have a wildcard if intersection is involved
			if req.IsTypedWildcard() {
				return s.setWildcardIntersectionSetup(ctx, req, edges)
			}
			return s.setOperationSetup(ctx, req, resolveIntersection, edges)
		case authzGraph.ExclusionOperator:
			edges, ok := s.model.GetEdgesFromNode(node)
			if !ok {
				return nil, ErrPanicRequest
			}
			// the request cannot have a wildcard if exclusion is involved
			if req.IsTypedWildcard() {
				return nil, ErrWildcardInvalidRequest
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
	edges, err = s.model.FlattenNode(node, req.GetUserType(), req.IsTypedWildcard(), s.strategy == recursive)
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

	iter := s.buildIterator(ctx, req, edge, tIter, req.GetTupleKey().GetUser())
	iterChan := make(chan *iterator.Msg, 1)
	if !concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: iter}, iterChan) {
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

	iter := s.buildIterator(ctx, req, edge, tIter, tuple.TypedPublicWildcard(req.GetUserType()))
	iterChan := make(chan *iterator.Msg, 1)
	if !concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: iter}, iterChan) {
		iter.Stop() // will not be received to be cleaned up
	}
	close(iterChan)
	return iterChan, nil
}

func (s *bottomUp) buildIterator(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, i storage.TupleIterator, userID string) storage.TupleMapper {
	// deduplication is only happening on the merged iterator and contextual tuples will always overwrite the base iterator
	iter := storage.NewTupleKeyIteratorFromTupleIterator(i)
	objectType, relation := tuple.SplitObjectRelation(edge.GetRelationDefinition())
	if ctxTuples, ok := req.GetContextualTuplesByUserID(userID, relation, objectType); ok {
		iter = iterator.Merge(iter, storage.NewStaticTupleKeyIterator(ctxTuples), func(a, b *openfgav1.TupleKey) int {
			if a.GetObject() < b.GetObject() {
				return -1
			}
			return 1
		})
	}
	var iterFilters []iterator.FilterFunc[*openfgav1.TupleKey]
	if len(edge.GetConditions()) > 1 || edge.GetConditions()[0] != authzGraph.NoCond {
		iterFilters = []iterator.FilterFunc[*openfgav1.TupleKey]{BuildConditionTupleKeyFilter(ctx, s.model, edge.GetConditions(), req.GetContext())}
	}
	return storage.WrapIterator(storage.ObjectIDKind, iterator.NewFilteredIterator(iter, iterFilters...))
}

type batcher struct {
	ctx   context.Context
	out   chan<- *iterator.Msg
	items []string
}

func newBatcher(ctx context.Context, out chan<- *iterator.Msg) *batcher {
	return &batcher{
		ctx:   ctx,
		out:   out,
		items: make([]string, 0, IteratorMinBatchThreshold),
	}
}

func (b *batcher) add(val string) {
	if val == "" {
		return
	}
	b.items = append(b.items, val)
	if len(b.items) >= IteratorMinBatchThreshold {
		b.flush(false)
	}
}

func (b *batcher) flush(stopped bool) {
	if len(b.items) > 0 {
		concurrency.TrySendThroughChannel(b.ctx, &iterator.Msg{Iter: storage.NewStaticIterator[string](b.items)}, b.out)
		// Get a new buffer from the pool for the next batch.
		// We cannot reuse the current one because ownership of the slice has been passed to the iterator.
		if stopped {
			b.items = nil
		} else {
			b.items = make([]string, 0, IteratorMinBatchThreshold)
		}
	}
}

// handleError provides if the error needs to be handled or not.
func (b *batcher) handleError(err error) bool {
	return !storage.IterIsDoneOrCancelled(err)
}

func (b *batcher) close(iters []storage.Iterator[string], err error) {
	// flush the items and nill the items, before sending the error in the channel, no more items after an error
	b.flush(true)
	if err != nil && !storage.IterIsDoneOrCancelled(err) {
		concurrency.TrySendThroughChannel(b.ctx, &iterator.Msg{Err: err}, b.out)
	}
	close(b.out)
	for _, it := range iters {
		it.Stop()
	}
}

// resolveUnion implements a merge-style algorithm for the union of sorted iterators
// handleStreamError sends an error through the output channel, sets batch to nil,
// and returns true if the function should terminate.
func resolveUnion(ctx context.Context, iters []storage.Iterator[string], out chan<- *iterator.Msg) {
	batch := newBatcher(ctx, out)
	lastError := error(nil)
	defer func() {
		// the closure is required to use the latest value of lastError at the time of executing close
		// lastError will be the last error that was not iteratorDone or Canceled
		// the error will only be send in the close after all the items were flushed
		batch.close(iters, lastError)
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
			// we need to store the empty value so we can keep the correlation between the index in the iterator
			// and the index in the compare values
			if batch.handleError(err) {
				lastError = err
			}
		} else {
			reverseLookupIndexes = append(reverseLookupIndexes, idx)
		}
		compareValues = append(compareValues, value)
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
					if batch.handleError(err) {
						lastError = err
					}
					continue
				} else {
					compareValues[idxValue] = v1
				}
			}
			newIndexes = append(newIndexes, idxValue)
		}

		batch.add(minValue)
		// Advance the stream with the minimum value
		value, err := iters[minIndexValue].Next(ctx)
		if err != nil {
			if batch.handleError(err) {
				lastError = err
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
	batch := newBatcher(ctx, out)
	lastError := error(nil)
	defer func() {
		// the closure is required to use the latest value of lastError at the time of executing close
		// lastError will be the last error that was not iteratorDone or Canceled
		// the error will only be send in the close after all the items were flushed
		batch.close(iters, lastError)
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
			// if one iterator is empty or error, interception is impossible
			if batch.handleError(err) {
				lastError = err
			}
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
			batch.add(maxValue)
			// Advance all streams
			for idx, iter := range iters {
				value, err := iter.Next(ctx)
				if err != nil {
					if batch.handleError(err) {
						lastError = err
					}
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
						if batch.handleError(err) {
							lastError = err
						}
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
	batch := newBatcher(ctx, out)

	lastError := error(nil)
	defer func() {
		// the closure is required to use the latest value of lastError at the time of executing close
		// lastError will be the last error that was not iteratorDone or Canceled
		// the error will only be send in the close after all the items were flushed
		batch.close(iters, lastError)
	}()

	if ctx.Err() != nil {
		return
	}

	compareValues := make([]string, 0, len(iters))
	for idx, iter := range iters {
		value, err := iter.Next(ctx)
		compareValues = append(compareValues, value)
		if err != nil {
			// if one iterator has error, we need to stop the execution
			if batch.handleError(err) {
				lastError = err
				return
			}
			if idx == BaseIndex {
				return // if base value is done or has any error, difference cannot continue
			}
			// otherwise if the error is in the difference index, drain the base
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
					// if one iterator has error, we need to stop the execution
					if batch.handleError(err) {
						lastError = err
						return
					}
					// in case the iterator Done is the difference iterator then drain the base
					if idx == DifferenceIndex {
						goto drainBase
					}
					// otherwise if the iterator Done is the base iterator then we need to stop
					return
				}
				compareValues[idx] = value
			}
		} else if compareValues[DifferenceIndex] > compareValues[BaseIndex] {
			// add the base value to the batch
			batch.add(compareValues[BaseIndex])
			// move the iterator and update the comparable value of base
			value, err := iters[BaseIndex].Next(ctx)
			if err != nil {
				// if base has an error or the base iterator is done, the difference operation cannot continue
				if batch.handleError(err) {
					lastError = err
				}
				return
			}
			compareValues[BaseIndex] = value
		} else {
			// diff < base, then move the diff to catch up with base
			value, err := skipTo(ctx, iters[DifferenceIndex], compareValues[BaseIndex])
			if err != nil {
				// if difference iterator has an error, the difference operation cannot continue
				if batch.handleError(err) {
					lastError = err
					return
				}
				// if the difference iterator is done, we need to drain the base
				goto drainBase
			}
			compareValues[DifferenceIndex] = value
		}
	}

drainBase:
	// drain the base
	for {
		if ctx.Err() != nil {
			return
		}
		batch.add(compareValues[BaseIndex])
		value, err := iters[BaseIndex].Next(ctx)
		if err != nil {
			// if there is an error capture the error
			if batch.handleError(err) {
				lastError = err
			}
			return
		}
		compareValues[BaseIndex] = value
	}
}

// TODO: move to iterator package once old one is deprecated.
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
