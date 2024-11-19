package graph

import (
	"context"
	"slices"

	"github.com/emirpasic/gods/sets/hashset"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const IteratorMinBatchThreshold = 1000

type fastPathSetHandler func(ctx context.Context, iterQueue []*iteratorProducer, iterChan chan *iteratorMsg)

type iteratorMsg struct {
	iter storage.TupleKeyIterator
	err  error
}

type iteratorProducer struct {
	idx      int
	iter     storage.TupleKeyIterator
	done     bool
	producer chan *iteratorMsg
}

// NOTE: caller should consider running this in a goroutine to not block.
func cleanupIteratorProducers(iterProducers []*iteratorProducer) {
	for _, iterProducer := range iterProducers {
		if iterProducer.iter != nil {
			iterProducer.iter.Stop()
		}
		for msg := range iterProducer.producer {
			if msg.iter != nil {
				msg.iter.Stop()
			}
		}
	}
}

// pollIteratorProducers will return a list of the remaining active producers.
// To be considered active your producer channel must still be open.
func pollIteratorProducers(ctx context.Context, iterProducers []*iteratorProducer) ([]*iteratorProducer, error) {
	for _, producer := range iterProducers {
		if producer.iter != nil || producer.done {
			// no need to poll further
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case i, ok := <-producer.producer:
			if !ok {
				producer.done = true
				break
			}
			if i.err != nil {
				return nil, i.err
			}
			producer.iter = i.iter
		}
	}
	// TODO: in go1.23 compare performance vs slices.Collect
	// clean up all empty entries that are both done and drained
	return slices.DeleteFunc(iterProducers, func(entry *iteratorProducer) bool {
		return entry.done && entry.iter == nil
	}), nil
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
	tk := req.GetTupleKey()
	computedRelation := rewrite.GetComputedUserset().GetRelation()
	rewrittenTupleKey := tuple.NewTupleKey(
		tk.GetObject(),
		computedRelation,
		tk.GetUser(),
	)

	childRequest := req.clone()
	childRequest.TupleKey = rewrittenTupleKey
	objectType := tuple.GetType(tk.GetObject())
	rel, err := typesys.GetRelation(objectType, computedRelation)
	if err != nil {
		return nil, err
	}

	return c.fastPathRewrite(ctx, childRequest, rel.GetRewrite())
}

func fastPathUnion(ctx context.Context, iterProducers []*iteratorProducer, iterChan chan *iteratorMsg) {
	batch := make([]*openfgav1.TupleKey, 0)

	defer func() {
		// flush
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
		}
		close(iterChan)
		cleanupIteratorProducers(iterProducers)
	}()

	/*
		collect iterators from all channels, until all drained
		start performing union algorithm across the heads, if an iterator is empty, poll once again the producer
		ask to see if the channel has a new iterator, otherwise consider it done
	*/

	for len(iterProducers) > 0 {
		if ctx.Err() != nil {
			return
		}
		var err error
		iterProducers, err = pollIteratorProducers(ctx, iterProducers)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
			return
		}
		allIters := true
		minKey := ""
		indexes := make([]int, 0)
		for idx, producer := range iterProducers {
			v, err := producer.iter.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					producer.iter.Stop()
					producer.iter = nil
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
				return
			}
			// initialize
			if idx == 0 {
				minKey = v.GetObject()
			}

			if minKey == v.GetObject() {
				indexes = append(indexes, idx)
			} else if minKey > v.GetObject() {
				minKey = v.GetObject()
				indexes = []int{idx}
			}
		}

		if !allIters {
			// we need to ensure we have all iterators at all times
			continue
		}

		// all entries with the same value move forward, but only have to send the value once
		for idx, iterIdx := range indexes {
			t, err := iterProducers[iterIdx].iter.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iterProducers[iterIdx].iter.Stop()
					iterProducers[iterIdx].iter = nil
					continue
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
				return
			}
			if idx == 0 {
				batch = append(batch, t)
			}
		}
		if len(batch) > IteratorMinBatchThreshold {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
			batch = make([]*openfgav1.TupleKey, 0)
		}
	}
}

func fastPathIntersection(ctx context.Context, iterProducers []*iteratorProducer, iterChan chan *iteratorMsg) {
	batch := make([]*openfgav1.TupleKey, 0)

	defer func() {
		// flush
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
		}
		close(iterChan)
		cleanupIteratorProducers(iterProducers)
	}()
	/*
		collect iterators from all channels, once none are nil
		start performing intersection algorithm across the heads, if an iterator is drained
		ask to see if the channel has a new iterator, otherwise consider it done
		exit if one of the channels closes as there is no more possible intersection of all
	*/

	childrenTotal := len(iterProducers)
	for len(iterProducers) == childrenTotal {
		if ctx.Err() != nil {
			return
		}
		var err error
		iterProducers, err = pollIteratorProducers(ctx, iterProducers)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
			return
		}
		if len(iterProducers) != childrenTotal {
			// short circuit
			return
		}

		maxKey := ""
		indexes := make([]int, 0)
		allIters := true
		for idx, producer := range iterProducers {
			v, err := producer.iter.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					producer.iter.Stop()
					producer.iter = nil
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
				return
			}

			if idx == 0 {
				maxKey = v.GetObject()
			}

			if maxKey == v.GetObject() {
				indexes = append(indexes, idx)
			} else if maxKey < v.GetObject() {
				maxKey = v.GetObject()
				indexes = []int{idx}
			}
		}
		if !allIters {
			// we need to ensure we have all iterators at all times
			continue
		}

		// all children have the same value
		if len(indexes) == childrenTotal {
			// all entries are the same thus flush entry and move iterators
			// there should only be 1 value
			for idx, iterIdx := range indexes {
				t, err := iterProducers[iterIdx].iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						iterProducers[iterIdx].iter.Stop()
						iterProducers[iterIdx].iter = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
					return
				}
				if idx == 0 {
					batch = append(batch, t)
				}
			}

			if len(batch) > IteratorMinBatchThreshold {
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
				batch = make([]*openfgav1.TupleKey, 0)
			}
			continue
		}

		// move all entries to less than the MAX to be >= than MAX
		for _, producer := range iterProducers {
			t, err := producer.iter.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					// this is highly unlikely due to the previous check
					producer.iter.Stop()
					producer.iter = nil
					continue
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
				return
			}
			tmpKey := t.GetObject()
			for tmpKey < maxKey {
				t, err := producer.iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						producer.iter.Stop()
						producer.iter = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
					return
				}
				tmpKey = t.GetObject()
			}
		}
	}
}

func fastPathDifference(ctx context.Context, iterProducers []*iteratorProducer, iterChan chan *iteratorMsg) {
	batch := make([]*openfgav1.TupleKey, 0)

	defer func() {
		// flush
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
		}
		close(iterChan)
		cleanupIteratorProducers(iterProducers)
	}()

	// both base and difference are still remaining
	for len(iterProducers) == 2 {
		if ctx.Err() != nil {
			return
		}
		var err error
		iterProducers, err = pollIteratorProducers(ctx, iterProducers)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
			return
		}
		if len(iterProducers) != 2 {
			// short circuit
			break
		}

		allIters := true
		base := ""
		diff := ""
		for idx, producer := range iterProducers {
			v, err := producer.iter.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					producer.iter.Stop()
					producer.iter = nil
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
				return
			}
			if idx == 0 {
				base = v.GetObject()
			}
			if idx == 1 {
				diff = v.GetObject()
			}
		}

		if !allIters {
			// we need to ensure we have all iterators at all times
			continue
		}

		// move both heads
		if base == diff {
			for _, iter := range iterProducers {
				_, err := iter.iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						iter.iter.Stop()
						iter.iter = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
					return
				}
			}
			continue
		}

		if diff > base {
			t, err := iterProducers[0].iter.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iterProducers[0].iter.Stop()
					iterProducers[0].iter = nil
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
				return
			}
			batch = append(batch, t)
			if len(batch) > IteratorMinBatchThreshold {
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
				batch = make([]*openfgav1.TupleKey, 0)
			}
			continue
		}

		// diff < base, then move the diff to catch up with base
		for diff < base {
			t, err := iterProducers[1].iter.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iterProducers[1].iter.Stop()
					iterProducers[1].iter = nil
					break
				}
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
				return
			}
			diff = t.GetObject()
		}
	}

	// drain the base
	if len(iterProducers) == 1 && iterProducers[0].idx == 0 {
		for len(iterProducers) == 1 && iterProducers[0].iter != nil {
			for {
				t, err := iterProducers[0].iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						iterProducers[0].iter.Stop()
						iterProducers[0].iter = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
					return
				}
				batch = append(batch, t)
			}
			if len(batch) > IteratorMinBatchThreshold {
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
				batch = make([]*openfgav1.TupleKey, 0)
			}
			var err error
			iterProducers, err = pollIteratorProducers(ctx, iterProducers)
			if err != nil {
				concurrency.TrySendThroughChannel(ctx, &iteratorMsg{err: err}, iterChan)
				return
			}
		}
	}
}

func (c *LocalChecker) fastPathOperationSetup(ctx context.Context, req *ResolveCheckRequest, op setOperatorType, children ...*openfgav1.Userset) (chan *iteratorMsg, error) {
	iterProducers := make([]*iteratorProducer, 0, len(children))
	for idx, child := range children {
		producerChan, err := c.fastPathRewrite(ctx, req, child)
		if err != nil {
			return nil, err
		}
		iterProducers = append(iterProducers, &iteratorProducer{idx: idx, producer: producerChan})
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
	resultChan := make(chan *iteratorMsg, len(children))
	go resolver(ctx, iterProducers, resultChan)
	return resultChan, nil
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
	rightOpen := true
	leftOpen := true

	cancellableCtx, cancel := context.WithCancel(ctx)
	leftChan := fanInIteratorChannels(cancellableCtx, leftChans)
	rightChan := streamedLookupUsersetFromIterator(cancellableCtx, iter)

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

	r, ok := <-rightChan
	if !ok {
		return res, nil
	}
	if r.err != nil {
		return nil, r.err
	}
	rightSet.Add(r.userset)
	for leftOpen || rightOpen {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-leftChan:
			if !ok {
				leftOpen = false
				if leftSet.Size() == 0 {
					return res, nil
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
					return res, nil
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
	return res, nil
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

	leftChans := make([]chan *iteratorMsg, 0)
	for _, parentType := range possibleParents {
		r := req.clone()
		r.TupleKey = &openfgav1.TupleKey{
			Object:   tuple.BuildObject(parentType.GetType(), "ignore"),
			Relation: computedRelation,
			User:     r.GetTupleKey().GetUser(),
		}
		rel, err := typesys.GetRelation(parentType.GetType(), computedRelation)
		if err != nil {
			// NOTE: is there a better way to check and filter rather than skipping?
			// other paths can be reachable
			continue
		}
		leftChan, err := c.fastPathRewrite(cancellableCtx, r, rel.GetRewrite())
		if err != nil {
			return nil, err
		}
		leftChans = append(leftChans, leftChan)
	}

	if len(leftChans) == 0 {
		// NOTE: this should be an error right?
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
		_ = pool.Wait()
		close(out)
	}()

	return out
}
