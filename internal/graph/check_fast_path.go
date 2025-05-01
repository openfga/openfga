package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/sourcegraph/conc/panics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph/iterator"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const IteratorMinBatchThreshold = 1000
const BaseIndex = 0
const DifferenceIndex = 1

type fastPathSetHandler func(context.Context, *iterator.Streams, chan<- *iterator.Msg)

func fastPathNoop(_ context.Context, _ *ResolveCheckRequest) (chan *iterator.Msg, error) {
	iterChan := make(chan *iterator.Msg)
	close(iterChan)
	return iterChan, nil
}

// fastPathDirect assumes that req.Object + req.Relation is a directly assignable relation, e.g. define viewer: [user, user:*].
// It returns a channel with one element, and then closes the channel.
// The element is an iterator over all objects that are directly related to the user or the wildcard (if applicable).
func fastPathDirect(ctx context.Context, req *ResolveCheckRequest) (chan *iterator.Msg, error) {
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)
	tk := req.GetTupleKey()
	objRel := tuple.ToObjectRelationString(tuple.GetType(tk.GetObject()), tk.GetRelation())
	i, err := checkutil.IteratorReadStartingFromUser(ctx, typesys, ds, req, objRel, nil, true)
	if err != nil {
		return nil, err
	}
	iterChan := make(chan *iterator.Msg, 1)
	iter := storage.WrapIterator(storage.ObjectIDKind, i)
	if !concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: iter}, iterChan) {
		iter.Stop() // will not be received to be cleaned up
	}
	close(iterChan)
	return iterChan, nil
}

func fastPathComputed(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset) (chan *iterator.Msg, error) {
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	computedRelation := rewrite.GetComputedUserset().GetRelation()

	childRequest := req.clone()
	childRequest.TupleKey.Relation = computedRelation

	objectType := tuple.GetType(childRequest.GetTupleKey().GetObject())
	rel, err := typesys.GetRelation(objectType, computedRelation)
	if err != nil {
		return nil, err
	}

	return fastPathRewrite(ctx, childRequest, rel.GetRewrite())
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

func fastPathUnion(ctx context.Context, streams *iterator.Streams, outChan chan<- *iterator.Msg) {
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

func fastPathIntersection(ctx context.Context, streams *iterator.Streams, outChan chan<- *iterator.Msg) {
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

func fastPathDifference(ctx context.Context, streams *iterator.Streams, outChan chan<- *iterator.Msg) {
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

// fastPathOperationSetup returns a channel with a number of elements that is >= the number of children.
// Each element is an iterator.
// The caller must wait until the channel is closed.
func fastPathOperationSetup(ctx context.Context, req *ResolveCheckRequest, resolver fastPathSetHandler, children ...*openfgav1.Userset) (chan *iterator.Msg, error) {
	iterStreams := make([]*iterator.Stream, 0, len(children))
	for idx, child := range children {
		producerChan, err := fastPathRewrite(ctx, req, child)
		if err != nil {
			return nil, err
		}
		iterStreams = append(iterStreams, iterator.NewStream(idx, producerChan))
	}

	outChan := make(chan *iterator.Msg, len(children))
	go func() {
		recoveredError := panics.Try(func() {
			resolver(ctx, iterator.NewStreams(iterStreams), outChan)
		})

		if recoveredError != nil {
			concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Err: fmt.Errorf("%w: %s", ErrPanic, recoveredError.AsError())}, outChan)
		}
	}()
	return outChan, nil
}

// fastPathRewrite returns a channel that will contain an unknown but finite number of elements.
// The channel is closed at the end.
func fastPathRewrite(
	ctx context.Context,
	req *ResolveCheckRequest,
	rewrite *openfgav1.Userset,
) (chan *iterator.Msg, error) {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return fastPathDirect(ctx, req)
	case *openfgav1.Userset_ComputedUserset:
		return fastPathComputed(ctx, req, rewrite)
	case *openfgav1.Userset_Union:
		return fastPathOperationSetup(ctx, req, fastPathUnion, rw.Union.GetChild()...)
	case *openfgav1.Userset_Intersection:
		return fastPathOperationSetup(ctx, req, fastPathIntersection, rw.Intersection.GetChild()...)
	case *openfgav1.Userset_Difference:
		return fastPathOperationSetup(ctx, req, fastPathDifference, rw.Difference.GetBase(), rw.Difference.GetSubtract())
	case *openfgav1.Userset_TupleToUserset:
		return fastPathNoop(ctx, req)
	default:
		return nil, ErrUnknownSetOperator
	}
}

// resolveFastPath attempts to find the intersection across 2 producers (channels) of ObjectIDs.
// In the case of a TTU:
// Right channel is the result set of the Read of ObjectID/Relation that yields the User's ObjectID.
// Left channel is the result set of ReadStartingWithUser of User/Relation that yields Object's ObjectID.
// From the perspective of the model, the left hand side of a TTU is the computed relationship being expanded.
func (c *LocalChecker) resolveFastPath(ctx context.Context, leftChans []chan *iterator.Msg, iter storage.TupleMapper) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "resolveFastPath", trace.WithAttributes(
		attribute.Int("sources", len(leftChans)),
		attribute.Bool("allowed", false),
	))
	defer span.End()
	cancellableCtx, cancel := context.WithCancel(ctx)
	leftChan := fanInIteratorChannels(cancellableCtx, leftChans)
	rightChan := streamedLookupUsersetFromIterator(cancellableCtx, iter)
	rightOpen := true
	leftOpen := true

	defer func() {
		cancel()
		iter.Stop()
		if !leftOpen {
			return
		}

		go func() {
			// Returning an error on a panic here would cause us to wait for the channel to be drained,
			// slowing the overall function down.
			defer func() {
				if r := recover(); r != nil {
					c.logger.ErrorWithContext(ctx, "panic in resolveFastPath",
						zap.Any("error", r),
					)
				}
			}()

			drainIteratorChannel(leftChan)
		}()
	}()

	res := &ResolveCheckResponse{
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
		if r.err != nil {
			return nil, r.err
		}
		rightSet.Add(r.userset)
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
				if processUsersetMessage(t, leftSet, rightSet) {
					msg.Iter.Stop()
					res.Allowed = true
					span.SetAttributes(attribute.Bool("allowed", true))
					lastErr = nil
					break ConsumerLoop
				}
			}
		case msg, ok := <-rightChan:
			if !ok {
				rightOpen = false
				break
			}
			if msg.err != nil {
				lastErr = msg.err
				continue
			}
			if processUsersetMessage(msg.userset, rightSet, leftSet) {
				res.Allowed = true
				span.SetAttributes(attribute.Bool("allowed", true))
				lastErr = nil
				break ConsumerLoop
			}
		}
	}
	return res, lastErr
}

func constructLeftChannels(ctx context.Context,
	logger logger.Logger,
	req *ResolveCheckRequest,
	relationReferences []*openfgav1.RelationReference,
	relationFunc checkutil.V2RelationFunc) ([]chan *iterator.Msg, error) {
	typesys, _ := typesystem.TypesystemFromContext(ctx)

	leftChans := make([]chan *iterator.Msg, 0, len(relationReferences))
	for _, parentType := range relationReferences {
		relation := relationFunc(parentType)
		rel, err := typesys.GetRelation(parentType.GetType(), relation)
		if err != nil {
			continue
		}
		r := req.clone()
		r.TupleKey = &openfgav1.TupleKey{
			Object: tuple.BuildObject(parentType.GetType(), "ignore"),
			// depending on relationFunc, it will return the parentType's relation (userset) or computedRelation (TTU)
			Relation: relation,
			User:     r.GetTupleKey().GetUser(),
		}
		leftChan, err := fastPathRewrite(ctx, r, rel.GetRewrite())
		if err != nil {
			// if the resolver already started it needs to be drained
			if len(leftChans) > 0 {
				go func() {
					// Returning an error on a panic here would cause us to wait for the channel to be drained,
					// slowing the overall function down.
					defer func() {
						if r := recover(); r != nil {
							logger.ErrorWithContext(ctx, "panic in constructLeftChannels",
								zap.Any("error", r),
							)
						}
					}()

					drainIteratorChannel(fanInIteratorChannels(ctx, leftChans))
				}()
			}
			return nil, err
		}
		leftChans = append(leftChans, leftChan)
	}
	return leftChans, nil
}

func (c *LocalChecker) checkUsersetFastPathV2(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkUsersetFastPathV2")
	defer span.End()

	typesys, _ := typesystem.TypesystemFromContext(ctx)
	objectType := tuple.GetType(req.GetTupleKey().GetObject())

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	directlyRelatedUsersetTypes, _ := typesys.DirectlyRelatedUsersets(objectType, req.GetTupleKey().GetRelation())

	leftChans, err := constructLeftChannels(cancellableCtx, c.logger, req, directlyRelatedUsersetTypes, checkutil.BuildUsersetV2RelationFunc())
	if err != nil {
		return nil, err
	}

	if len(leftChans) == 0 {
		return &ResolveCheckResponse{
			Allowed: false,
		}, nil
	}

	return c.resolveFastPath(ctx, leftChans, storage.WrapIterator(storage.UsersetKind, iter))
}

func (c *LocalChecker) checkTTUFastPathV2(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkTTUFastPathV2")
	defer span.End()
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	objectType := tuple.GetType(req.GetTupleKey().GetObject())
	tuplesetRelation := rewrite.GetTupleToUserset().GetTupleset().GetRelation()
	computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()

	possibleParents, err := typesys.GetDirectlyRelatedUserTypes(objectType, tuplesetRelation)
	if err != nil {
		return nil, err
	}

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	leftChans, err := constructLeftChannels(cancellableCtx, c.logger, req, possibleParents, checkutil.BuildTTUV2RelationFunc(computedRelation))
	if err != nil {
		return nil, err
	}

	if len(leftChans) == 0 {
		return &ResolveCheckResponse{
			Allowed: false,
		}, nil
	}

	return c.resolveFastPath(ctx, leftChans, storage.WrapIterator(storage.TTUKind, iter))
}

// NOTE: Can we make this generic and move it to concurrency pkg?
func fanInIteratorChannels(ctx context.Context, chans []chan *iterator.Msg) chan *iterator.Msg {
	limit := len(chans)

	out := make(chan *iterator.Msg, limit)

	if limit == 0 {
		close(out)
		return out
	}
	pool := concurrency.NewPool(ctx, limit)

	for _, c := range chans {
		if c == nil {
			continue
		}
		pool.Go(func(ctx context.Context) error {
			for v := range c {
				if v != nil && !concurrency.TrySendThroughChannel(ctx, v, out) {
					if v.Iter != nil {
						v.Iter.Stop()
					}
				}
			}

			return nil
		})
	}

	go func() {
		// NOTE: the consumer of this channel will block waiting for it to close
		_ = pool.Wait()
		close(out)
	}()

	return out
}

func drainIteratorChannel(c chan *iterator.Msg) {
	for msg := range c {
		if msg != nil && msg.Iter != nil {
			msg.Iter.Stop()
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

	chans := make([]chan *iterator.Msg, 0, currentUsersetLevel.Size())
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
			// TODO: TBD if we hard exit here, if yes, the channel needs to be closed
			continue
		}
		c := make(chan *iterator.Msg, 1)
		if !concurrency.TrySendThroughChannel(ctx, &iterator.Msg{Iter: mapper}, c) {
			mapper.Stop() // will not be received to be cleaned up
		}
		close(c)
		chans = append(chans, c)
	}
	leftChan := fanInIteratorChannels(ctx, chans)
	leftOpen := true
	defer func() {
		if leftOpen {
			go func() {
				// Returning an error on a panic here would cause us to wait for the channel to be drained,
				// slowing the overall function down.
				defer func() {
					if r := recover(); r != nil {
						c.logger.ErrorWithContext(ctx, "panic in breadthFirstRecursiveMatch",
							zap.Any("error", r),
						)
					}
				}()

				drainIteratorChannel(leftChan)
			}()
		}
	}()
	nextUsersetLevel := hashset.New()

ConsumerLoop:
	for {
		select {
		case <-ctx.Done():
			close(checkOutcomeChan)
			return
		case msg, ok := <-leftChan:
			if !ok {
				// nothing was found in this level, break to proceed into the next one
				leftOpen = false
				break ConsumerLoop
			}
			for {
				t, err := msg.Iter.Next(ctx)
				if err != nil {
					msg.Iter.Stop()
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					concurrency.TrySendThroughChannel(ctx, checkOutcome{err: err}, checkOutcomeChan)
					close(checkOutcomeChan)
					return
				}

				if usersetFromUser.Contains(t) {
					concurrency.TrySendThroughChannel(ctx, checkOutcome{resp: &ResolveCheckResponse{Allowed: true}}, checkOutcomeChan)
					close(checkOutcomeChan)
					return
				}

				nextUsersetLevel.Add(t)
			}
		}
	}

	c.breadthFirstRecursiveMatch(ctx, req, mapping, visitedUserset, nextUsersetLevel, usersetFromUser, checkOutcomeChan)
}

func (c *LocalChecker) recursiveMatchUserUserset(ctx context.Context, req *ResolveCheckRequest, mapping *recursiveMapping, currentLevelFromObject *hashset.Set, usersetFromUser *hashset.Set) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "recursiveMatchUserUserset", trace.WithAttributes(
		attribute.Int("first_level_size", currentLevelFromObject.Size()),
		attribute.Int("terminal_type_size", usersetFromUser.Size()),
	))
	defer span.End()
	checkOutcomeChan := make(chan checkOutcome, c.concurrencyLimit)

	cancellableCtx, cancel := context.WithCancel(ctx)
	pool := concurrency.NewPool(cancellableCtx, 1)
	defer func() {
		cancel()
		// We need to wait always to avoid a goroutine leak.
		_ = pool.Wait()
	}()
	pool.Go(func(ctx context.Context) error {
		c.breadthFirstRecursiveMatch(ctx, req, mapping, &sync.Map{}, currentLevelFromObject, usersetFromUser, checkOutcomeChan)
		return nil
	})

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
	cancel() // prevent further processing of other checks
	// context cancellation from upstream (e.g. client)
	if ctx.Err() != nil {
		finalErr = ctx.Err()
	}

	if finalErr != nil {
		return nil, finalErr
	}

	return finalResult, nil
}

type recursiveMapping struct {
	kind                        storage.TupleMapperKind
	tuplesetRelation            string
	allowedUserTypeRestrictions []*openfgav1.RelationReference
}

func (c *LocalChecker) recursiveFastPath(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator, mapping *recursiveMapping, objectProvider objectProvider) (*ResolveCheckResponse, error) {
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
		case objectToUsersetMessage, ok := <-objectToUsersetMessageChan:
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

func (c *LocalChecker) recursiveTTUFastPath(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "recursiveTTUFastPath")
	defer span.End()

	typesys, _ := typesystem.TypesystemFromContext(ctx)
	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)

	objectProvider := newRecursiveObjectProvider(c.logger, typesys, ds)

	return c.recursiveFastPath(ctx, req, iter, &recursiveMapping{
		kind:             storage.TTUKind,
		tuplesetRelation: rewrite.GetTupleToUserset().GetTupleset().GetRelation(),
	}, objectProvider)
}

// recursiveTTUFastPathV2 solves a union relation of the form "{operand1} OR ... {operandN} OR {recursive TTU}"
// rightIter gives the iterator for the recursive TTU.
func (c *LocalChecker) recursiveTTUFastPathV2(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, rightIter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "recursiveTTUFastPathV2")
	defer span.End()

	typesys, _ := typesystem.TypesystemFromContext(ctx)

	ttu := rewrite.GetTupleToUserset()

	objectProvider := newRecursiveTTUObjectProvider(c.logger, typesys, ttu)

	return c.recursiveFastPath(ctx, req, rightIter, &recursiveMapping{
		kind:             storage.TTUKind,
		tuplesetRelation: ttu.GetTupleset().GetRelation(),
	}, objectProvider)
}

func (c *LocalChecker) recursiveUsersetFastPath(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "recursiveUsersetFastPath")
	defer span.End()

	typesys, _ := typesystem.TypesystemFromContext(ctx)
	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)

	objectProvider := newRecursiveObjectProvider(c.logger, typesys, ds)

	directlyRelatedUsersetTypes, _ := typesys.DirectlyRelatedUsersets(tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation())
	return c.recursiveFastPath(ctx, req, iter, &recursiveMapping{
		kind:                        storage.UsersetKind,
		allowedUserTypeRestrictions: directlyRelatedUsersetTypes,
	}, objectProvider)
}

func (c *LocalChecker) recursiveUsersetFastPathV2(ctx context.Context, req *ResolveCheckRequest, rightIter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "recursiveTTUFastPathV2")
	defer span.End()

	typesys, _ := typesystem.TypesystemFromContext(ctx)

	directlyRelatedUsersetTypes, _ := typesys.DirectlyRelatedUsersets(tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation())
	objectProvider := newRecursiveUsersetObjectProvider(c.logger, typesys)

	return c.recursiveFastPath(ctx, req, rightIter, &recursiveMapping{
		kind:                        storage.UsersetKind,
		allowedUserTypeRestrictions: directlyRelatedUsersetTypes,
	}, objectProvider)
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
