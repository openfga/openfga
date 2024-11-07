package graph

import (
	"context"
	"fmt"
	"slices"

	"github.com/emirpasic/gods/sets/hashset"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	openfgaErrors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type fastPathSetHandler func(ctx context.Context, iterQueue []*iteratorProducerEntry, iterChan chan iteratorMsg)

type iteratorMsg struct {
	iter storage.TupleKeyIterator
	err  error
}

func (c *LocalChecker) fastPathDirect(ctx context.Context,
	req *ResolveCheckRequest) (chan iteratorMsg, error) {
	iterChan := make(chan iteratorMsg, 1)
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	ds, _ := storage.RelationshipTupleReaderFromContext(ctx)
	tk := req.GetTupleKey()
	objRel := tuple.ToObjectRelationString(tuple.GetType(tk.GetObject()), tk.GetRelation())
	i, err := checkutil.IteratorReadStartingFromUser(ctx, typesys, ds, req, objRel, nil)
	if err != nil {
		return nil, err
	}

	concurrency.TrySendThroughChannel(ctx, iteratorMsg{iter: i}, iterChan)
	close(iterChan)
	return iterChan, nil
}

func (c *LocalChecker) fastPathComputed(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset) (chan iteratorMsg, error) {
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
		return nil, fmt.Errorf("relation '%s' undefined for object type '%s'", computedRelation, rel)
	}

	return c.fastPathRewrite(ctx, childRequest, rel.GetRewrite())
}

type iteratorProducerEntry struct {
	idx          int
	iter         storage.TupleKeyIterator
	producerDone bool
	producer     chan iteratorMsg
}

func pollIteratorQueues(iterQueue []*iteratorProducerEntry) ([]*iteratorProducerEntry, []storage.TupleKeyIterator, error) {
	newIters := make([]storage.TupleKeyIterator, 0)
	for _, iter := range iterQueue {
		// no need to poll further
		if iter.producerDone || iter.iter != nil {
			continue
		}
		i, ok := <-iter.producer
		if !ok {
			iter.producerDone = true
			continue
		}
		if i.err != nil {
			// NOTE: how do we want to handle missing iterators? full error out?
			return nil, nil, i.err
		}
		newIters = append(newIters, i.iter)
		// in the first instance the iterator is empty but not nil
		if iter.iter == nil {
			iter.iter = i.iter
		} else {
			iter.iter = storage.NewCombinedIterator(iter.iter, i.iter)
		}
	}
	// TODO: in go1.23 compare performance vs slices.Collect
	// clean up all empty entries that are both done and drained
	return slices.DeleteFunc(iterQueue, func(entry *iteratorProducerEntry) bool {
		return entry.producerDone && entry.iter == nil
	}), newIters, nil
}

func fastPathUnion(ctx context.Context, iterQueue []*iteratorProducerEntry, iterChan chan iteratorMsg) {
	defer func() {
		close(iterChan)
		for _, iter := range iterQueue {
			close(iter.producer)
		}
	}()
	/*
		collect iterators from all channels, once none are nil
		start performing union algorithm across the heads, if an iterator is drained
		ask to see if the channel has a new iterator, otherwise consider it done
	*/
	var newIters []storage.TupleKeyIterator
	batch := make([]*openfgav1.TupleKey, 0)
	for len(iterQueue) > 0 {
		if ctx.Err() != nil {
			return
		}
		var err error
		iterQueue, newIters, err = pollIteratorQueues(iterQueue)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
			return
		}
		for _, iter := range newIters {
			defer iter.Stop()
		}
		allIters := true
		minKey := ""
		indexes := make([]int, 0)
		for idx, iter := range iterQueue {
			v, err := iter.iter.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iter.iter = nil
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
				return
			}
			// initialize
			if idx == 0 {
				minKey = v.GetObject()
			}

			if minKey == v.GetObject() {
				indexes = append(indexes, idx)
			}

			if minKey > v.GetObject() {
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
			t, err := iterQueue[iterIdx].iter.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iterQueue[iterIdx].iter = nil
					continue
				}
				concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
				return
			}
			if idx == 0 {
				batch = append(batch, t)
			}
		}
		// TODO: determine this size
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
			batch = make([]*openfgav1.TupleKey, 0)
		}
	}
	if len(batch) > 0 {
		concurrency.TrySendThroughChannel(ctx, iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
	}
}

func fastPathIntersection(ctx context.Context, iterQueue []*iteratorProducerEntry, iterChan chan iteratorMsg) {
	defer func() {
		close(iterChan)
		for _, iter := range iterQueue {
			close(iter.producer)
		}
	}()
	/*
		collect iterators from all channels, once none are nil
		start performing intersection algorithm across the heads, if an iterator is drained
		ask to see if the channel has a new iterator, otherwise consider it done
		exit if one of the channels closes as there is no more possible intersection of all
	*/

	childrenTotal := len(iterQueue)
	var newIters []storage.TupleKeyIterator
	batch := make([]*openfgav1.TupleKey, 0)
	for len(iterQueue) == childrenTotal {
		if ctx.Err() != nil {
			return
		}
		var err error
		iterQueue, newIters, err = pollIteratorQueues(iterQueue)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
			return
		}
		if len(iterQueue) != childrenTotal {
			// short circuit
			return
		}
		for _, iter := range newIters {
			defer iter.Stop()
		}

		maxKey := ""
		indexes := make([]int, 0)
		allIters := true
		for idx, iter := range iterQueue {
			v, err := iter.iter.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iter.iter = nil
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
				return
			}

			if idx == 0 {
				maxKey = v.GetObject()
			}

			if maxKey == v.GetObject() {
				indexes = append(indexes, idx)
			}

			if maxKey < v.GetObject() {
				maxKey = v.GetObject()
				indexes = []int{idx}
			}
		}

		if !allIters {
			// we need to ensure we have all iterators at all times
			continue
		}

		// all children have the same value
		if len(indexes) == len(iterQueue) {
			// all entries are the same thus flush entry and move iterators
			// there should only be 1 value
			for idx, iterIdx := range indexes {
				t, err := iterQueue[iterIdx].iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						iterQueue[iterIdx].iter = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
					return
				}
				if idx == 0 {
					batch = append(batch, t)
				}
			}
			// TODO: determine this size
			if len(batch) > 0 {
				concurrency.TrySendThroughChannel(ctx, iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
				batch = make([]*openfgav1.TupleKey, 0)
			}
			continue
		}

		// move all entries to less than the MAX to be >= than MAX
		for _, iter := range iterQueue {
			tmpKey := ""
			for tmpKey == "" || tmpKey < maxKey {
				t, err := iter.iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						iter.iter = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
					return
				}
				tmpKey = t.GetObject()
			}
		}
	}
	if len(batch) > 0 {
		concurrency.TrySendThroughChannel(ctx, iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
	}
}

func fastPathDifference(ctx context.Context, iterQueue []*iteratorProducerEntry, iterChan chan iteratorMsg) {
	defer func() {
		close(iterChan)
		for _, iter := range iterQueue {
			close(iter.producer)
		}
	}()

	var newIters []storage.TupleKeyIterator
	batch := make([]*openfgav1.TupleKey, 0)
	for len(iterQueue) == 2 {
		if ctx.Err() != nil {
			return
		}
		var err error
		iterQueue, newIters, err = pollIteratorQueues(iterQueue)
		if err != nil {
			concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
			return
		}
		if len(iterQueue) != 2 {
			// short circuit
			break
		}
		for _, iter := range newIters {
			defer iter.Stop()
		}

		allIters := true
		base := ""
		diff := ""
		for idx, iter := range iterQueue {
			v, err := iter.iter.Head(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					iter.iter = nil
					allIters = false
					// we need to ensure we have all iterators at all times
					break
				}
				concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
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
			for _, iter := range iterQueue {
				_, err := iter.iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						iter.iter = nil
						continue
					}
					concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
					return
				}
			}
			continue
		}
		if diff > base {
			_, err := iterQueue[1].iter.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					// this would be weird
					concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
					return
				}
			}
			continue
		}

		t, err := iterQueue[0].iter.Next(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				// this would be weird
				concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
				return
			}
		}
		batch = append(batch, t)
		// TODO: determine this size
		if len(batch) > 0 {
			concurrency.TrySendThroughChannel(ctx, iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
			batch = make([]*openfgav1.TupleKey, 0)
		}
	}

	// drain the base
	if len(iterQueue) == 1 && iterQueue[0].idx == 0 {
		for len(iterQueue) == 1 {
			if ctx.Err() != nil {
				return
			}
			for {
				t, err := iterQueue[0].iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						iterQueue[0].iter = nil
						break
					}
					concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
					return
				}
				batch = append(batch, t)
			}
			var err error
			iterQueue, newIters, err = pollIteratorQueues(iterQueue)
			if err != nil {
				concurrency.TrySendThroughChannel(ctx, iteratorMsg{err: err}, iterChan)
			}
			for _, iter := range newIters {
				defer iter.Stop()
			}
		}
	}
	if len(batch) > 0 {
		concurrency.TrySendThroughChannel(ctx, iteratorMsg{iter: storage.NewStaticTupleKeyIterator(batch)}, iterChan)
	}
}

func (c *LocalChecker) fastPathOperationSetup(ctx context.Context, req *ResolveCheckRequest, op setOperatorType, children ...*openfgav1.Userset) (chan iteratorMsg, error) {
	resultChan := make(chan iteratorMsg, 1)
	iterQueue := make([]*iteratorProducerEntry, len(children))
	for idx, child := range children {
		c, err := c.fastPathRewrite(ctx, req, child)
		if err != nil {
			return nil, err
		}
		iterQueue = append(iterQueue, &iteratorProducerEntry{idx: idx, iter: storage.NewStaticTupleKeyIterator(nil), producer: c, producerDone: false})
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
		return nil, fmt.Errorf("%w: unexpected set operator type encountered", openfgaErrors.ErrUnknown)
	}
	go resolver(ctx, iterQueue, resultChan)
	return resultChan, nil
}

func (c *LocalChecker) fastPathRewrite(
	ctx context.Context,
	req *ResolveCheckRequest,
	rewrite *openfgav1.Userset,
) (chan iteratorMsg, error) {
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
		return nil, fmt.Errorf("%w: unexpected set operator type encountered", openfgaErrors.ErrUnknown)
	}
}

func (c *LocalChecker) resolveFastPath(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter TupleMapper) (*ResolveCheckResponse, error) {
	rightChan := streamedLookupUsersetFromIterator(ctx, iter)
	leftChan, err := c.fastPathRewrite(ctx, req, rewrite)
	if err != nil {
		return nil, err
	}

	res := &ResolveCheckResponse{
		Allowed: false,
	}

	rightSet := hashset.New()
	leftSet := hashset.New()

	// check to see if there are any tuplesetrelation assigned. If not,
	// we don't even need to check the computedrelation side.
	r, ok := <-rightChan
	if !ok {
		return res, nil
	}
	if r.err != nil {
		return nil, r.err
	}
	rightSet.Add(r.userset)

	for {
		select {
		case <-ctx.Done():
		case msg, ok := <-rightChan:
			if !ok {
				break
			}
			if msg.err != nil {
				return nil, msg.err
			}
			if processUsersetMessage(msg.userset, rightSet, leftSet) {
				res.Allowed = true
				return res, nil
			}
		case msg, ok := <-leftChan:
			if !ok {
				if leftSet.Size() == 0 {
					return res, nil
				}
				break
			}
			if msg.err != nil {
				return nil, msg.err
			}
			// this could be a goroutine
			for {
				t, err := msg.iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					return nil, err
				}
				if processUsersetMessage(t.GetObject(), leftSet, rightSet) {
					res.Allowed = true
					return res, nil
				}
			}
		}
	}
}

func (c *LocalChecker) checkTTUFastPathV2(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "checkTTUFastPathV2")
	defer span.End()
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	objectType := tuple.GetType(req.GetTupleKey().GetObject())
	computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()
	rel, err := typesys.GetRelation(objectType, computedRelation)
	if err != nil {
		return nil, fmt.Errorf("relation '%s' undefined for object type '%s'", computedRelation, objectType)
	}

	rw := rel.GetRewrite()
	// V1 (directly assignable), the computed relation is a terminal type (no nested usersets/TTU)
	if _, ok := rw.GetUserset().(*openfgav1.Userset_This); ok {
		usersetDetails := checkutil.BuildUsersetDetailsTTU(typesys, computedRelation)
		return c.checkMembership(ctx, req, iter, usersetDetails)
	}
	return c.resolveFastPath(ctx, req, rw, wrapIterator(TTUKind, iter))
}
