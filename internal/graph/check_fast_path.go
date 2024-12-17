package graph

import (
	"context"
	"slices"

	"github.com/emirpasic/gods/sets/hashset"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const IteratorMinBatchThreshold = 1000
const BaseIndex = 0
const DifferenceIndex = 1

type fastPathSetHandler func(context.Context, *iteratorStreams, chan<- *iteratorMsg)

type iteratorMsg struct {
	iter storage.TupleKeyIterator
	err  error
}

type iteratorStream struct {
	idx    int
	buffer storage.TupleKeyIterator
	done   bool
	source chan *iteratorMsg
}

type iteratorStreams struct {
	streams []*iteratorStream
}

// getActiveStreamsCount will return the active streams from the last time getActiveStreams was called.
func (s *iteratorStreams) getActiveStreamsCount() int {
	return len(s.streams)
}

// Stop will drain all streams completely to avoid leaving dangling resources
// NOTE: caller should consider running this in a goroutine to not block.
func (s *iteratorStreams) Stop() {
	for _, stream := range s.streams {
		if stream.buffer != nil {
			stream.buffer.Stop()
		}
		for msg := range stream.source {
			if msg.iter != nil {
				msg.iter.Stop()
			}
		}
	}
}

// getActiveStreams will return a list of the remaining active streams.
// To be considered active your source channel must still be open.
func (s *iteratorStreams) getActiveStreams(ctx context.Context) ([]*iteratorStream, error) {
	for _, stream := range s.streams {
		if stream.buffer != nil || stream.done {
			// no need to poll further
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case i, ok := <-stream.source:
			if !ok {
				stream.done = true
				break
			}
			if i.err != nil {
				return nil, i.err
			}
			stream.buffer = i.iter
		}
	}
	// TODO: in go1.23 compare performance vs slices.Collect
	// clean up all empty entries that are both done and drained
	s.streams = slices.DeleteFunc(s.streams, func(entry *iteratorStream) bool {
		return entry.done && entry.buffer == nil
	})
	return s.streams, nil
}

func (c *LocalChecker) fastPathDirect(ctx context.Context,
	req *ResolveCheckRequest) (chan *iteratorMsg, error) {
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)
	tk := req.GetTupleKey()
	objRel := tuple.ToObjectRelationString(tuple.GetType(tk.GetObject()), tk.GetRelation())
	i, err := checkutil.IteratorReadStartingFromUser(ctx, typesys, ds, req, objRel, nil)
	if err != nil {
		return nil, err
	}
	iterChan := make(chan *iteratorMsg, 1)
	if !concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: i}, iterChan) {
		i.Stop() // will not be received to be cleaned up
	}
	close(iterChan)
	return iterChan, nil
}

func (c *LocalChecker) fastPathComputed(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset) (chan *iteratorMsg, error) {
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	computedRelation := rewrite.GetComputedUserset().GetRelation()

	childRequest := req.clone()
	childRequest.TupleKey.Relation = computedRelation

	objectType := tuple.GetType(childRequest.GetTupleKey().GetObject())
	rel, err := typesys.GetRelation(objectType, computedRelation)
	if err != nil {
		return nil, err
	}

	return c.fastPathRewrite(ctx, childRequest, rel.GetRewrite())
}

func fastPathUnion(ctx context.Context, streams *iteratorStreams, outChan chan<- *iteratorMsg) {
	batch := make([]*openfgav1.TupleKey, 0)

	defer func() {
		// flush
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, outChan)
		}
		close(outChan)
		streams.Stop()
	}()

	/*
		collect iterators from all channels, until all drained
		start performing union algorithm across the heads, if an iterator is empty, poll once again the source
		ask to see if the channel has a new iterator, otherwise consider it done
	*/

	for streams.getActiveStreamsCount() > 0 {
		if ctx.Err() != nil {
			return
		}
		iterStreams, err := streams.getActiveStreams(ctx)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
			return
		}
		allIters := true
		minObject := ""
		itersWithEqualObject := make([]int, 0)
		for idx, stream := range iterStreams {
			v, err := stream.buffer.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					stream.buffer.Stop()
					stream.buffer = nil
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
				return
			}
			// initialize
			if idx == 0 {
				minObject = v.GetObject()
			}

			if minObject == v.GetObject() {
				itersWithEqualObject = append(itersWithEqualObject, idx)
			} else if minObject > v.GetObject() {
				minObject = v.GetObject()
				itersWithEqualObject = []int{idx}
			}
		}

		if !allIters {
			// we need to ensure we have all iterators at all times
			continue
		}

		// all iterators with the same value move forward
		for idx, iterIdx := range itersWithEqualObject {
			t, err := iterStreams[iterIdx].buffer.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iterStreams[iterIdx].buffer.Stop()
					iterStreams[iterIdx].buffer = nil
					continue
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
				return
			}
			// only have to send the value once
			if idx == 0 {
				batch = append(batch, t)
			}
		}
		if len(batch) > IteratorMinBatchThreshold {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, outChan)
			batch = make([]*openfgav1.TupleKey, 0)
		}
	}
}

func fastPathIntersection(ctx context.Context, streams *iteratorStreams, outChan chan<- *iteratorMsg) {
	batch := make([]*openfgav1.TupleKey, 0)

	defer func() {
		// flush
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, outChan)
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

	childrenTotal := streams.getActiveStreamsCount()
	for streams.getActiveStreamsCount() == childrenTotal {
		if ctx.Err() != nil {
			return
		}
		iterStreams, err := streams.getActiveStreams(ctx)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
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
			v, err := stream.buffer.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					stream.buffer.Stop()
					stream.buffer = nil
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
				return
			}

			if idx == 0 {
				maxObject = v.GetObject()
			}

			if maxObject == v.GetObject() {
				itersWithEqualObject = append(itersWithEqualObject, idx)
			} else if maxObject < v.GetObject() {
				maxObject = v.GetObject()
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
			for idx, iterIdx := range itersWithEqualObject {
				t, err := iterStreams[iterIdx].buffer.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						iterStreams[iterIdx].buffer.Stop()
						iterStreams[iterIdx].buffer = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
					return
				}
				// only have to send the value once
				if idx == 0 {
					batch = append(batch, t)
				}
			}

			if len(batch) > IteratorMinBatchThreshold {
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, outChan)
				batch = make([]*openfgav1.TupleKey, 0)
			}
			continue
		}

		// move all iterators to less than the MAX to be >= than MAX
		for _, stream := range iterStreams {
			t, err := stream.buffer.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					// this is highly unlikely due to the previous check
					stream.buffer.Stop()
					stream.buffer = nil
					continue
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
				return
			}
			tmpKey := t.GetObject()
			for tmpKey < maxObject {
				_, _ = stream.buffer.Next(ctx)
				t, err := stream.buffer.Head(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						stream.buffer.Stop()
						stream.buffer = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
					return
				}
				tmpKey = t.GetObject()
			}
		}
	}
}

func fastPathDifference(ctx context.Context, streams *iteratorStreams, outChan chan<- *iteratorMsg) {
	batch := make([]*openfgav1.TupleKey, 0)

	defer func() {
		// flush
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, outChan)
		}
		close(outChan)
		streams.Stop()
	}()

	// both base and difference are still remaining
	for streams.getActiveStreamsCount() == 2 {
		if ctx.Err() != nil {
			return
		}
		iterStreams, err := streams.getActiveStreams(ctx)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
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
			v, err := stream.buffer.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					stream.buffer.Stop()
					stream.buffer = nil
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
				return
			}
			if idx == BaseIndex {
				base = v.GetObject()
			}
			if idx == DifferenceIndex {
				diff = v.GetObject()
			}
		}

		if !allIters {
			// we need to ensure we have all iterators at all times
			continue
		}

		// move both iterator heads
		if base == diff {
			for _, stream := range iterStreams {
				_, err := stream.buffer.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						stream.buffer.Stop()
						stream.buffer = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
					return
				}
			}
			continue
		}

		if diff > base {
			t, err := iterStreams[BaseIndex].buffer.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iterStreams[BaseIndex].buffer.Stop()
					iterStreams[BaseIndex].buffer = nil
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
				return
			}
			batch = append(batch, t)
			if len(batch) > IteratorMinBatchThreshold {
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, outChan)
				batch = make([]*openfgav1.TupleKey, 0)
			}
			continue
		}

		// diff < base, then move the diff to catch up with base
		for diff < base {
			_, _ = iterStreams[DifferenceIndex].buffer.Next(ctx)
			t, err := iterStreams[DifferenceIndex].buffer.Head(ctx)

			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iterStreams[DifferenceIndex].buffer.Stop()
					iterStreams[DifferenceIndex].buffer = nil
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
				return
			}
			diff = t.GetObject()
		}
	}

	iterStreams, err := streams.getActiveStreams(ctx)
	if err != nil {
		concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
		return
	}

	// drain the base
	if len(iterStreams) == 1 && iterStreams[BaseIndex].idx == BaseIndex {
		for len(iterStreams) == 1 {
			stream := iterStreams[BaseIndex]
			for {
				t, err := stream.buffer.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						stream.buffer.Stop()
						stream.buffer = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
					return
				}
				batch = append(batch, t)
			}
			if len(batch) > IteratorMinBatchThreshold {
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, outChan)
				batch = make([]*openfgav1.TupleKey, 0)
			}
			iterStreams, err = streams.getActiveStreams(ctx)
			if err != nil {
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, outChan)
				return
			}
		}
	}
}

func (c *LocalChecker) fastPathOperationSetup(ctx context.Context, req *ResolveCheckRequest, op setOperatorType, children ...*openfgav1.Userset) (chan *iteratorMsg, error) {
	iterStreams := make([]*iteratorStream, 0, len(children))
	for idx, child := range children {
		producerChan, err := c.fastPathRewrite(ctx, req, child)
		if err != nil {
			return nil, err
		}
		iterStreams = append(iterStreams, &iteratorStream{idx: idx, source: producerChan})
	}
	var resolver fastPathSetHandler
	switch op {
	case unionSetOperator:
		resolver = fastPathUnion
	case intersectionSetOperator:
		resolver = fastPathIntersection
	case exclusionSetOperator:
		resolver = fastPathDifference
	default:
		return nil, ErrUnknownSetOperator
	}
	outChan := make(chan *iteratorMsg, len(children))
	go resolver(ctx, &iteratorStreams{streams: iterStreams}, outChan)
	return outChan, nil
}

func (c *LocalChecker) fastPathRewrite(
	ctx context.Context,
	req *ResolveCheckRequest,
	rewrite *openfgav1.Userset,
) (chan *iteratorMsg, error) {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return c.fastPathDirect(ctx, req)
	case *openfgav1.Userset_ComputedUserset:
		return c.fastPathComputed(ctx, req, rewrite)
	case *openfgav1.Userset_Union:
		return c.fastPathOperationSetup(ctx, req, unionSetOperator, rw.Union.GetChild()...)
	case *openfgav1.Userset_Intersection:
		return c.fastPathOperationSetup(ctx, req, intersectionSetOperator, rw.Intersection.GetChild()...)
	case *openfgav1.Userset_Difference:
		return c.fastPathOperationSetup(ctx, req, exclusionSetOperator, rw.Difference.GetBase(), rw.Difference.GetSubtract())
	default:
		return nil, ErrUnknownSetOperator
	}
}

// resolveFastPath attempts to find the intersection across 2 producers (channels) of ObjectIDs.
// In the case of a TTU:
// Right channel is the result set of the Read of ObjectID/Relation that yields the User's ObjectID.
// Left channel is the result set of ReadStartingWithUser of User/Relation that yields Object's ObjectID.
// From the perspective of the model, the left hand side of a TTU is the computed relationship being expanded.
func (c *LocalChecker) resolveFastPath(ctx context.Context, leftChans []chan *iteratorMsg, iter TupleMapper) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "resolveFastPath", trace.WithAttributes(
		attribute.Int("sources", len(leftChans)),
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
			for msg := range leftChan {
				if msg.iter != nil {
					msg.iter.Stop()
				}
			}
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

	for leftOpen || rightOpen {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-leftChan:
			if !ok {
				leftOpen = false
				if leftSet.Size() == 0 {
					return res, ctx.Err()
				}
				break
			}
			if msg.err != nil {
				return nil, msg.err
			}
			for {
				t, err := msg.iter.Next(ctx)
				if err != nil {
					msg.iter.Stop()
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					return nil, err
				}
				if processUsersetMessage(t.GetObject(), leftSet, rightSet) {
					msg.iter.Stop()
					res.Allowed = true
					return res, ctx.Err()
				}
			}
		case msg, ok := <-rightChan:
			if !ok {
				rightOpen = false
				break
			}
			if msg.err != nil {
				return nil, msg.err
			}
			if processUsersetMessage(msg.userset, rightSet, leftSet) {
				res.Allowed = true
				return res, nil
			}
		}
	}
	return res, ctx.Err()
}

func (c *LocalChecker) constructLeftChannels(ctx context.Context,
	req *ResolveCheckRequest,
	relationReferences []*openfgav1.RelationReference,
	relationFunc checkutil.V2RelationFunc) ([]chan *iteratorMsg, error) {
	typesys, _ := typesystem.TypesystemFromContext(ctx)

	leftChans := make([]chan *iteratorMsg, 0, len(relationReferences))
	for _, parentType := range relationReferences {
		r := req.clone()
		r.TupleKey = &openfgav1.TupleKey{
			Object: tuple.BuildObject(parentType.GetType(), "ignore"),
			// depending on relationFunc, it will return the parentType's relation (userset) or computedRelation (TTU)
			Relation: relationFunc(parentType),
			User:     r.GetTupleKey().GetUser(),
		}
		rel, err := typesys.GetRelation(parentType.GetType(), relationFunc(parentType))
		if err != nil {
			// NOTE: is there a better way to check and filter rather than skipping?
			// other paths can be reachable
			continue
		}
		leftChan, err := c.fastPathRewrite(ctx, r, rel.GetRewrite())
		if err != nil {
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

	leftChans, err := c.constructLeftChannels(cancellableCtx, req, directlyRelatedUsersetTypes, checkutil.BuildUsersetV2RelationFunc())
	if err != nil {
		return nil, err
	}

	if len(leftChans) == 0 {
		return &ResolveCheckResponse{
			Allowed: false,
		}, nil
	}

	return c.resolveFastPath(ctx, leftChans, wrapIterator(UsersetKind, iter))
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

	leftChans, err := c.constructLeftChannels(cancellableCtx, req, possibleParents, checkutil.BuildTTUV2RelationFunc(computedRelation))
	if err != nil {
		return nil, err
	}

	if len(leftChans) == 0 {
		return &ResolveCheckResponse{
			Allowed: false,
		}, nil
	}

	return c.resolveFastPath(ctx, leftChans, wrapIterator(TTUKind, iter))
}

// NOTE: Can we make this generic and move it to concurrency pkg?
func fanInIteratorChannels(ctx context.Context, chans []chan *iteratorMsg) chan *iteratorMsg {
	limit := len(chans)
	pool := concurrency.NewPool(ctx, limit)
	out := make(chan *iteratorMsg, limit)

	for _, c := range chans {
		pool.Go(func(ctx context.Context) error {
			for v := range c {
				if !concurrency.TrySendThroughChannel(ctx, v, out) {
					if v.iter != nil {
						v.iter.Stop()
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
