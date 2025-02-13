package graph

import (
	"context"
	"github.com/sourcegraph/conc/pool"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph/iterator"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// objectProvider is an interface that abstracts the building of a channel that holds object IDs or usersets.
// It must close the channel when there are no more results.
type objectProvider interface {
	End()
	Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error)
}

type recursiveObjectProvider struct {
	mapper storage.TupleMapper
	ts     *typesystem.TypeSystem
	ds     storage.RelationshipTupleReader
}

func newRecursiveObjectProvider(ts *typesystem.TypeSystem, ds storage.RelationshipTupleReader) *recursiveObjectProvider {
	return &recursiveObjectProvider{ts: ts, ds: ds}
}

var _ objectProvider = (*recursiveObjectProvider)(nil)

func (s *recursiveObjectProvider) End() {
	if s.mapper != nil {
		s.mapper.Stop()
	}
}

func (s *recursiveObjectProvider) Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
	// Note: we set sortContextualTuples to false because we don't care about ordering of results,
	// since the consumer is using hashsets to check for intersection.
	userIter, err := checkutil.IteratorReadStartingFromUser(ctx, s.ts, s.ds, req,
		tuple.ToObjectRelationString(tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation()),
		nil, false)
	if err != nil {
		return nil, err
	}
	usersetFromUserIter := storage.WrapIterator(storage.ObjectIDKind, userIter)
	s.mapper = usersetFromUserIter

	// note: this function will close the channel
	userToUsersetMessageChan := streamedLookupUsersetFromIterator(ctx, usersetFromUserIter)

	return userToUsersetMessageChan, nil
}

type recursiveTTUObjectProvider struct {
	ts               *typesystem.TypeSystem
	tuplesetRelation string
	computedRelation string
	cancel           context.CancelFunc
	pool             *pool.ContextPool
}

func newRecursiveTTUObjectProvider(ts *typesystem.TypeSystem, ttu *openfgav1.TupleToUserset) *recursiveTTUObjectProvider {
	tuplesetRelation := ttu.GetTupleset().GetRelation()
	computedRelation := ttu.GetComputedUserset().GetRelation()
	return &recursiveTTUObjectProvider{ts: ts, tuplesetRelation: tuplesetRelation, computedRelation: computedRelation}
}

var _ objectProvider = (*recursiveTTUObjectProvider)(nil)

func (c *recursiveTTUObjectProvider) End() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.pool != nil {
		_ = c.pool.Wait()
	}
}

func (c *recursiveTTUObjectProvider) Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
	objectType := tuple.GetType(req.GetTupleKey().GetObject())

	possibleParents, err := c.ts.GetDirectlyRelatedUserTypes(objectType, c.tuplesetRelation)
	if err != nil {
		return nil, err
	}

	leftChannels, err := constructLeftChannels(ctx, req, possibleParents, checkutil.BuildTTUV2RelationFunc(c.computedRelation))
	if err != nil {
		return nil, err
	}
	outChannel := make(chan usersetMessage, len(leftChannels))
	leftChannel := fanInIteratorChannels(ctx, leftChannels)
	poolCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.pool = concurrency.NewPool(poolCtx, 1)
	c.pool.Go(iteratorToUserset(leftChannel, outChannel))

	return outChannel, nil
}

type recursiveUsersetObjectProvider struct {
	ts     *typesystem.TypeSystem
	cancel context.CancelFunc
	pool   *pool.ContextPool
}

func newRecursiveUsersetObjectProvider(ts *typesystem.TypeSystem) *recursiveUsersetObjectProvider {
	return &recursiveUsersetObjectProvider{ts: ts}
}

var _ objectProvider = (*recursiveUsersetObjectProvider)(nil)

func (c *recursiveUsersetObjectProvider) End() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.pool != nil {
		_ = c.pool.Wait()
	}
}

func (c *recursiveUsersetObjectProvider) Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
	objectType := tuple.GetType(req.GetTupleKey().GetObject())
	reference := []*openfgav1.RelationReference{{Type: objectType, RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: req.GetTupleKey().GetRelation()}}}
	leftChans, err := constructLeftChannels(ctx, req, reference, checkutil.BuildUsersetV2RelationFunc())
	if err != nil {
		return nil, err
	}
	outChannel := make(chan usersetMessage, len(leftChans))
	leftChannel := fanInIteratorChannels(ctx, leftChans)
	poolCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.pool = concurrency.NewPool(poolCtx, 1)
	c.pool.Go(iteratorToUserset(leftChannel, outChannel))

	return outChannel, nil
}

func iteratorToUserset(src chan *iterator.Msg, dst chan usersetMessage) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		leftOpen := true
		defer func() {
			close(dst)
			if !leftOpen {
				return
			}
			go drainIteratorChannel(src)
		}()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-src:
			if !ok {
				leftOpen = false
				return nil
			}
			if msg.Err != nil {
				concurrency.TrySendThroughChannel(ctx, usersetMessage{err: msg.Err}, dst)
				return msg.Err
			}
			for {
				t, err := msg.Iter.Next(ctx)
				if err != nil {
					msg.Iter.Stop()
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					concurrency.TrySendThroughChannel(ctx, usersetMessage{err: err}, dst)
					return err
				}
				userset := t.GetObject()
				concurrency.TrySendThroughChannel(ctx, usersetMessage{userset: userset}, dst)
			}
		}
		return nil
	}
}
