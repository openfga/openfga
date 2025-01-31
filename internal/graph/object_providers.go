package graph

import (
	"context"
	"fmt"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	openfgaErrors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/sourcegraph/conc/pool"
)

// objectProvider is an interface that abstracts the building of a channel that holds object IDs or usersets.
// It must close the channel when there are no more results.
type objectProvider interface {
	End()
	Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error)
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

func (s *simpleRecursiveObjectProvider) Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", openfgaErrors.ErrUnknown)
	}
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

type complexRecursiveTTUObjectProvider struct {
	concurrencyLimit uint32
	ts               *typesystem.TypeSystem
	rewrite          *openfgav1.Userset
	outChannel       chan usersetMessage
	cancel           context.CancelFunc
	pool             *pool.ContextPool
}

func newComplexTTURecursiveObjectProvider(concurrencyLimit uint32, ts *typesystem.TypeSystem, rewrite *openfgav1.Userset) (*complexRecursiveTTUObjectProvider, error) {
	if ts == nil {
		return nil, fmt.Errorf("%w: nil typesystem", openfgaErrors.ErrUnknown)
	}
	return &complexRecursiveTTUObjectProvider{concurrencyLimit: concurrencyLimit, ts: ts, rewrite: rewrite}, nil
}

var _ objectProvider = (*complexRecursiveTTUObjectProvider)(nil)

func (c *complexRecursiveTTUObjectProvider) End() {
	if c.cancel != nil {
		c.cancel()
		_ = c.pool.Wait()
	}
}

func (c *complexRecursiveTTUObjectProvider) Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", openfgaErrors.ErrUnknown)
	}
	objectType := tuple.GetType(req.GetTupleKey().GetObject())
	tuplesetRelation := c.rewrite.GetTupleToUserset().GetTupleset().GetRelation()
	computedRelation := c.rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()

	possibleParents, err := c.ts.GetDirectlyRelatedUserTypes(objectType, tuplesetRelation)
	if err != nil {
		return nil, err
	}

	leftChannels, err := constructLeftChannels(ctx, req, possibleParents, checkutil.BuildTTUV2RelationFunc(computedRelation))
	if err != nil {
		return nil, err
	}
	outChannel := make(chan usersetMessage, len(leftChannels))
	leftChannel := fanInIteratorChannels(ctx, leftChannels)
	poolCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.pool = concurrency.NewPool(poolCtx, 1)
	c.pool.Go(func(ctx context.Context) error {
		leftOpen := true
		defer func() {
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
			return nil
		case msg, ok := <-leftChannel:
			if !ok {
				leftOpen = false
				return nil
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
