package graph

import (
	"context"
	"fmt"
	"sync"

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

type complexRecursiveObjectProvider struct {
	concurrencyLimit uint32
	wg               sync.WaitGroup
	ts               *typesystem.TypeSystem
}

func newComplexRecursiveObjectProvider(concurrencyLimit uint32, ts *typesystem.TypeSystem) (*complexRecursiveObjectProvider, error) {
	if ts == nil {
		return nil, fmt.Errorf("%w: nil typesystem", openfgaErrors.ErrUnknown)
	}
	return &complexRecursiveObjectProvider{concurrencyLimit: concurrencyLimit, wg: sync.WaitGroup{}, ts: ts}, nil
}

var _ objectProvider = (*complexRecursiveObjectProvider)(nil)

func (c *complexRecursiveObjectProvider) End() {
	c.wg.Wait()
}

func (c *complexRecursiveObjectProvider) Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", openfgaErrors.ErrUnknown)
	}
	objectType, relation := tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation()
	userType := tuple.GetType(req.GetTupleKey().GetUser())
	operands, ok := c.ts.IsRelationWithRecursiveTTUAndAlgebraicOperations(objectType, relation, userType)
	if !ok {
		return nil, fmt.Errorf("%w: unsupported model", openfgaErrors.ErrUnknown)
	}

	outChannel := make(chan usersetMessage, len(operands)) // buffered so that the first writes don't block on the caller reading from the channel
	pool := concurrency.NewPool(ctx, max(int(c.concurrencyLimit), len(operands)))

	c.wg.Add(1)
	go func() {
		defer func() {
			_ = pool.Wait()
			close(outChannel)
			c.wg.Done()
		}()

		for operandRelationName, operandRewrite := range operands {
			pool.Go(func(ctx context.Context) error {
				newReq := req.clone()
				newReq.TupleKey.Relation = operandRelationName
				chanIterator, err := fastPathRewrite(ctx, newReq, operandRewrite)
				if err != nil {
					concurrency.TrySendThroughChannel(ctx, usersetMessage{err: err}, outChannel)
					return err
				}
				for iteratorMessage := range chanIterator {
					if iteratorMessage.err != nil {
						concurrency.TrySendThroughChannel(ctx, usersetMessage{err: err}, outChannel)
						return iteratorMessage.err
					}
					for {
						t, err := iteratorMessage.iter.Next(ctx)
						if err != nil {
							iteratorMessage.iter.Stop()
							if storage.IterIsDoneOrCancelled(err) {
								break
							}
							concurrency.TrySendThroughChannel(ctx, usersetMessage{err: err}, outChannel)
							return err
						}
						userset := t.GetObject()
						concurrency.TrySendThroughChannel(ctx, usersetMessage{userset: userset}, outChannel)
					}
				}
				return nil
			})
		}
	}()

	return outChannel, nil
}
