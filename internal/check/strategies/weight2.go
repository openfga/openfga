package strategies

import (
	"context"
	"fmt"

	"github.com/emirpasic/gods/sets/hashset"
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

// processMessage will add the id in the primarySet.
// In addition, it returns whether the id exists in secondarySet.
// This is used to find the intersection between id from user and id from object.
func processMessage(id string,
	primarySet *hashset.Set,
	secondarySet *hashset.Set) bool {
	primarySet.Add(id)
	return secondarySet.Contains(id)
}

// Weight2 attempts to find the intersection across 2 producers (channels) of ObjectIDs.
// In the case of a TTU:
// Right channel is the result set of the Read of ObjectID/Relation that yields the User's ObjectID.
// Left channel is the result set of ReadStartingWithUser of User/Relation that yields Object's ObjectID.
// From the perspective of the model, the left hand side of a TTU is the computed relationship being expanded.
func (s *Weight2) execute(ctx context.Context, leftChan chan *iterator.Msg, rightIter storage.TupleMapper) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "weight2")
	defer span.End()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer rightIter.Stop()
	rightChan := iterator.ToChannel[string](ctx, rightIter, IteratorMinBatchThreshold)
	rightOpen := true
	leftOpen := true

	res := &check.Response{
		Allowed: false,
	}

	rightSet := hashset.New()
	leftSet := hashset.New()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r, ok := <-rightChan:
		if !ok {
			return res, ctx.Err()
		}
		if r.Err != nil {
			return nil, r.Err
		}
		rightSet.Add(r.Value)
	}

	var lastErr error

ConsumerLoop:
	for leftOpen || rightOpen {
		select {
		case <-ctx.Done():
			lastErr = ctx.Err()
			break ConsumerLoop
		case msg, ok := <-leftChan:
			if !ok {
				leftOpen = false
				if leftSet.Size() == 0 {
					if ctx.Err() != nil {
						lastErr = ctx.Err()
					}
					break ConsumerLoop
				}
				break
			}
			if msg.Err != nil {
				lastErr = msg.Err
				break ConsumerLoop
			}
			for {
				t, err := msg.Iter.Next(ctx)
				if err != nil {
					msg.Iter.Stop()
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					lastErr = err
					continue
				}
				if processMessage(t, leftSet, rightSet) {
					msg.Iter.Stop()
					res.Allowed = true
					lastErr = nil
					break ConsumerLoop
				}
			}
		case msg, ok := <-rightChan:
			if !ok {
				rightOpen = false
				break
			}
			if msg.Err != nil {
				lastErr = msg.Err
				continue
			}
			if processMessage(msg.Value, rightSet, leftSet) {
				res.Allowed = true
				lastErr = nil
				break ConsumerLoop
			}
		}
	}
	return res, lastErr
}

// setOperationSetup returns a channel with a number of elements that is >= the number of children.
// Each element is an iterator.
// The caller must wait until the channel is closed.
func (s *Weight2) setOperationSetup(ctx context.Context, req *check.Request, resolver fastPathSetHandler, node *authzGraph.WeightedAuthorizationModelNode) (chan *iterator.Msg, error) {
	edges, ok := s.model.GetEdgesFromNode(node)
	if !ok {
		return nil, check.ErrPanicRequest
	}
	iterStreams := make([]*iterator.Stream, 0, len(edges))
	for idx, edge := range edges {
		if _, ok := edge.GetWeight(req.GetUserType()); !ok {
			continue
		}
		producerChan, err := s.resolveEdge(ctx, req, edge)
		if err != nil {
			return nil, err
		}
		iterStreams = append(iterStreams, iterator.NewStream(idx, producerChan))
	}

	outChan := make(chan *iterator.Msg, len(iterStreams))
	go func() {
		recoveredError := panics.Try(func() {
			resolver(ctx, iterator.NewStreams(iterStreams), outChan)
		})

		if recoveredError != nil {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: fmt.Errorf("%w: %s", check.ErrPanicRequest, recoveredError.AsError())}, outChan)
		}
	}()
	return outChan, nil
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
		return s.setOperationSetup(ctx, req, s.resolveUnion, node)
	case authzGraph.OperatorNode:
		switch node.GetLabel() {
		case authzGraph.UnionOperator:
			return s.setOperationSetup(ctx, req, s.resolveUnion, node)
		case authzGraph.IntersectionOperator:
			return s.setOperationSetup(ctx, req, s.resolveIntersection, node)
		case authzGraph.ExclusionOperator:
			return s.setOperationSetup(ctx, req, s.resolveDifference, node)
		default:
			return nil, check.ErrPanicRequest
		}
	default:
		return nil, check.ErrPanicRequest
	}
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
	objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	iter, err := s.datastore.ReadStartingWithUser(ctx, req.GetStoreID(),
		storage.ReadStartingWithUserFilter{
			ObjectType: objectType,
			Relation:   relation,
			UserFilter: []*openfgav1.ObjectRelation{{
				Object: req.GetTupleKey().GetUser(),
			}},
		}, opts)
	if err != nil {
		return nil, err
	}

	conditionEdge, err := s.model.GetConditionsEdgeForUserType(tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation()), edge.GetTo().GetUniqueLabel())
	if err != nil {
		return nil, check.ErrPanicRequest
	}

	i := storage.NewTupleKeyIteratorFromTupleIterator(iter)
	if len(conditionEdge.GetConditions()) > 1 || conditionEdge.GetConditions()[0] != "" {
		i = storage.NewConditionsFilteredTupleKeyIterator(i,
			check.BuildTupleKeyConditionFilter(ctx, s.model, conditionEdge, req.GetContext()),
		)
	}

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
	objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	iter, err := s.datastore.ReadStartingWithUser(ctx, req.GetStoreID(),
		storage.ReadStartingWithUserFilter{
			ObjectType: objectType,
			Relation:   relation,
			UserFilter: []*openfgav1.ObjectRelation{{
				Object: tuple.TypedPublicWildcard(req.GetUserType()),
			}},
		}, opts)
	if err != nil {
		return nil, err
	}

	conditionEdge, err := s.model.GetConditionsEdgeForUserType(tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation()), edge.GetTo().GetUniqueLabel())
	if err != nil {
		return nil, check.ErrPanicRequest
	}

	i := storage.NewTupleKeyIteratorFromTupleIterator(iter)
	if len(conditionEdge.GetConditions()) > 1 || conditionEdge.GetConditions()[0] != "" {
		i = storage.NewConditionsFilteredTupleKeyIterator(i,
			check.BuildTupleKeyConditionFilter(ctx, s.model, conditionEdge, req.GetContext()),
		)
	}

	iterChan := make(chan *iterator.Msg, 1)
	if !concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.WrapIterator(storage.ObjectIDKind, i)}, iterChan) {
		iter.Stop() // will not be received to be cleaned up
	}
	close(iterChan)
	return iterChan, nil
}

// add the nextItemInSliceStreams to specified batch. If batch is full, try to send batch to outChan and clear slice.
// If nextItemInSliceStreams has error, will also send message to specified outChan.
func addNextItemInSliceStreamsToBatch(ctx context.Context, streamSlices []*iterator.Stream, streamsToProcess []int, batch []string, outChan chan<- *iterator.Msg) ([]string, error) {
	item, err := iterator.NextItemInSliceStreams(ctx, streamSlices, streamsToProcess)
	if err != nil {
		concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
		return nil, err
	}
	if item != "" {
		batch = append(batch, item)
	}
	if len(batch) > IteratorMinBatchThreshold {
		concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.NewStaticIterator[string](batch)}, outChan)
		batch = make([]string, 0)
	}
	return batch, nil
}

func (s *Weight2) resolveUnion(ctx context.Context, streams *iterator.Streams, outChan chan<- *iterator.Msg) {
	batch := make([]string, 0)

	defer func() {
		// flush
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.NewStaticIterator[string](batch)}, outChan)
		}
		close(outChan)
		streams.Stop()
	}()

	/*
		collect iterators from all channels, until all drained
		start performing union algorithm across the heads, if an iterator is empty, poll once again the source
		ask to see if the channel has a new iterator, otherwise consider it done
	*/

	for streams.GetActiveStreamsCount() > 0 {
		if ctx.Err() != nil {
			return
		}
		iterStreams, err := streams.CleanDone(ctx)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
			return
		}
		allIters := true
		minObject := ""
		itersWithEqualObject := make([]int, 0)
		for idx, stream := range iterStreams {
			v, err := stream.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
				return
			}
			// initialize
			if idx == 0 {
				minObject = v
			}

			if minObject == v {
				itersWithEqualObject = append(itersWithEqualObject, idx)
			} else if minObject > v {
				minObject = v
				itersWithEqualObject = []int{idx}
			}
		}

		if !allIters {
			// we need to ensure we have all iterators at all times
			continue
		}

		// all iterators with the same value move forward
		batch, err = addNextItemInSliceStreamsToBatch(ctx, iterStreams, itersWithEqualObject, batch, outChan)
		if err != nil {
			// We are relying on the fact that we have called .Head(ctx) earlier
			// and no one else should have called the iterator (especially since it is
			// protected by mutex). Therefore, it is impossible for the iterator to return
			// Done here. Hence, any error received here should be considered as legitimate
			// errors.
			return
		}
	}
}

func (s *Weight2) resolveIntersection(ctx context.Context, streams *iterator.Streams, outChan chan<- *iterator.Msg) {
	batch := make([]string, 0)

	defer func() {
		// flush
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.NewStaticIterator[string](batch)}, outChan)
		}
		close(outChan)
		streams.Stop()
	}()
	/*
		collect iterators from all channels, once none are nil
		start performing intersection algorithm across the heads, if an iterator is drained
		ask to see if the channel has a new iterator, otherwise consider it done
		exit if one of the channels closes as there is no more possible intersection of all
	*/

	childrenTotal := streams.GetActiveStreamsCount()
	for streams.GetActiveStreamsCount() == childrenTotal {
		if ctx.Err() != nil {
			return
		}
		iterStreams, err := streams.CleanDone(ctx)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
			return
		}
		if len(iterStreams) != childrenTotal {
			// short circuit
			return
		}

		maxObject := ""
		itersWithEqualObject := make([]int, 0)
		allIters := true
		for idx, stream := range iterStreams {
			v, err := stream.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
				return
			}

			if idx == 0 {
				maxObject = v
			}

			if maxObject == v {
				itersWithEqualObject = append(itersWithEqualObject, idx)
			} else if maxObject < v {
				maxObject = v
				itersWithEqualObject = []int{idx}
			}
		}
		if !allIters {
			// we need to ensure we have all iterators at all times
			continue
		}

		// all children have the same value
		if len(itersWithEqualObject) == childrenTotal {
			// all iterators have the same value thus flush entry and move iterators
			batch, err = addNextItemInSliceStreamsToBatch(ctx, iterStreams, itersWithEqualObject, batch, outChan)
			if err != nil {
				// We are relying on the fact that we have called .Head(ctx) earlier
				// and no one else should have called the iterator (especially since it is
				// protected by mutex). Therefore, it is impossible for the iterator to return
				// Done here. Hence, any error received here should be considered as legitimate
				// errors.
				return
			}
			continue
		}

		// move all iterators to less than the MAX to be >= than MAX
		for _, stream := range iterStreams {
			err = stream.SkipToTargetObject(ctx, maxObject)
			if err != nil {
				concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
				return
			}
		}
	}
}

func (s *Weight2) resolveDifference(ctx context.Context, streams *iterator.Streams, outChan chan<- *iterator.Msg) {
	batch := make([]string, 0)

	defer func() {
		// flush
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.NewStaticIterator[string](batch)}, outChan)
		}
		close(outChan)
		streams.Stop()
	}()

	// both base and difference are still remaining
	for streams.GetActiveStreamsCount() == 2 {
		if ctx.Err() != nil {
			return
		}
		iterStreams, err := streams.CleanDone(ctx)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
			return
		}
		if len(iterStreams) != 2 {
			// short circuit
			break
		}

		allIters := true
		base := ""
		diff := ""
		for idx, stream := range iterStreams {
			v, err := stream.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
				return
			}
			if idx == BaseIndex {
				base = v
			}
			if idx == DifferenceIndex {
				diff = v
			}
		}

		if !allIters {
			// we need to ensure we have all iterators at all times
			continue
		}

		// move both iterator heads
		if base == diff {
			_, err = iterator.NextItemInSliceStreams(ctx, iterStreams, []int{BaseIndex, DifferenceIndex})
			if err != nil {
				// We are relying on the fact that we have called .Head(ctx) earlier
				// and no one else should have called the iterator (especially since it is
				// protected by mutex). Therefore, it is impossible for the iterator to return
				// Done here. Hence, any error received here should be considered as legitimate
				// errors.
				concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
				return
			}
			continue
		}

		if diff > base {
			batch, err = addNextItemInSliceStreamsToBatch(ctx, iterStreams, []int{BaseIndex}, batch, outChan)
			if err != nil {
				// We are relying on the fact that we have called .Head(ctx) earlier
				// and no one else should have called the iterator (especially since it is
				// protected by mutex). Therefore, it is impossible for the iterator to return
				// Done here. Hence, any error received here should be considered as legitimate
				// errors.
				return
			}
			continue
		}

		// diff < base, then move the diff to catch up with base
		err = iterStreams[DifferenceIndex].SkipToTargetObject(ctx, base)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
			return
		}
	}

	iterStreams, err := streams.CleanDone(ctx)
	if err != nil {
		concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
		return
	}

	// drain the base
	if len(iterStreams) == 1 && iterStreams[BaseIndex].Idx() == BaseIndex {
		for len(iterStreams) == 1 {
			stream := iterStreams[BaseIndex]
			items, err := stream.Drain(ctx)
			if err != nil {
				concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
				return
			}
			batch = append(batch, items...)
			if len(batch) > IteratorMinBatchThreshold {
				concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: storage.NewStaticIterator[string](batch)}, outChan)
				batch = make([]string, 0)
			}
			iterStreams, err = streams.CleanDone(ctx)
			if err != nil {
				concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: err}, outChan)
				return
			}
		}
	}
}
