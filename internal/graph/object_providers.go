package graph

import (
	"context"
	"fmt"

	"github.com/sourcegraph/conc/pool"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	openfgaErrors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// objectProvider is an interface that abstracts the building of a channel that holds object IDs or usersets.
// It must close the channel when there are no more results.
type objectProvider interface {
	End()
	Begin(cancellableCtx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error)
}

type simpleRecursiveObjectProvider struct {
	mapper storage.TupleMapper
	ts     *typesystem.TypeSystem
	ds     storage.RelationshipTupleReader
}

func newSimpleRecursiveObjectProvider(ts *typesystem.TypeSystem, ds storage.RelationshipTupleReader) (*simpleRecursiveObjectProvider, error) {
	if ts == nil || ds == nil {
		return nil, fmt.Errorf("%w: nil arguments", openfgaErrors.ErrUnknown)
	}
	return &simpleRecursiveObjectProvider{ts: ts, ds: ds}, nil
}

var _ objectProvider = (*simpleRecursiveObjectProvider)(nil)

func (s *simpleRecursiveObjectProvider) End() {
	if s.mapper != nil {
		s.mapper.Stop()
	}
}

func (s *simpleRecursiveObjectProvider) Begin(cancellableCtx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", openfgaErrors.ErrUnknown)
	}
	// Note: we set sortContextualTuples to false because we don't care about ordering of results,
	// since the consumer is using hashsets to check for intersection.
	userIter, err := checkutil.IteratorReadStartingFromUser(cancellableCtx, s.ts, s.ds, req,
		tuple.ToObjectRelationString(tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation()),
		nil, false)
	if err != nil {
		return nil, err
	}
	usersetFromUserIter := storage.WrapIterator(storage.ObjectIDKind, userIter)
	s.mapper = usersetFromUserIter

	// note: this function will close the channel
	userToUsersetMessageChan := streamedLookupUsersetFromIterator(cancellableCtx, usersetFromUserIter)

	return userToUsersetMessageChan, nil
}

type complexRecursiveTTUObjectProvider struct {
	ts               *typesystem.TypeSystem
	tuplesetRelation string
	computedRelation string
	cancel           context.CancelFunc
	pool             *pool.ContextPool
}

func newComplexTTURecursiveObjectProvider(ts *typesystem.TypeSystem, rewrite *openfgav1.Userset) (*complexRecursiveTTUObjectProvider, error) {
	if ts == nil {
		return nil, fmt.Errorf("%w: nil typesystem", openfgaErrors.ErrUnknown)
	}
	if rewrite == nil {
		return nil, fmt.Errorf("%w: nil rewrite", openfgaErrors.ErrUnknown)
	}
	if rewrite.GetTupleToUserset() == nil {
		return nil, fmt.Errorf("%w: rewrite must be a tupletouserset", openfgaErrors.ErrUnknown)
	}

	tuplesetRelation := rewrite.GetTupleToUserset().GetTupleset().GetRelation()
	computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()
	return &complexRecursiveTTUObjectProvider{ts: ts, tuplesetRelation: tuplesetRelation, computedRelation: computedRelation}, nil
}

var _ objectProvider = (*complexRecursiveTTUObjectProvider)(nil)

func (c *complexRecursiveTTUObjectProvider) End() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.pool != nil {
		_ = c.pool.Wait()
	}
}

func (c *complexRecursiveTTUObjectProvider) Begin(cancellableCtx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", openfgaErrors.ErrUnknown)
	}
	objectType := tuple.GetType(req.GetTupleKey().GetObject())

	possibleParents, err := c.ts.GetDirectlyRelatedUserTypes(objectType, c.tuplesetRelation)
	if err != nil {
		return nil, err
	}

	leftChannels, err := constructLeftChannels(cancellableCtx, req, possibleParents, checkutil.BuildTTUV2RelationFunc(c.computedRelation))
	if err != nil {
		return nil, err
	}
	outChannel := make(chan usersetMessage, len(leftChannels))
	leftChannel := fanInIteratorChannels(cancellableCtx, leftChannels)
	poolCtx, cancel := context.WithCancel(cancellableCtx)
	c.cancel = cancel

	c.pool = concurrency.NewPool(poolCtx, 1)
	c.pool.Go(func(ctx context.Context) error {
		leftOpen := true
		defer func() {
			close(outChannel)
			if !leftOpen {
				return
			}
			go func() {
				for msg := range leftChannel {
					if msg.Iter != nil {
						msg.Iter.Stop()
					}
				}
			}()
		}()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-leftChannel:
			if !ok {
				leftOpen = false
				return nil
			}
			if msg.Err != nil {
				concurrency.TrySendThroughChannel(ctx, usersetMessage{err: msg.Err}, outChannel)
				return msg.Err
			}
			t, err := msg.Iter.Next(ctx)
			if err != nil {
				msg.Iter.Stop()
				if storage.IterIsDoneOrCancelled(err) {
					break
				}
				concurrency.TrySendThroughChannel(ctx, usersetMessage{err: err}, outChannel)
				return err
			}
			userset := t.GetObject()
			concurrency.TrySendThroughChannel(ctx, usersetMessage{userset: userset}, outChannel)
		}
		return nil
	})

	return outChannel, nil
}
