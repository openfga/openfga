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

// ObjectProvider is an interface that abstracts the building of a channel that holds object IDs or usersets.
// It must close the channel when there are no more results.
type ObjectProvider interface {
	End()
	Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error)
}

type SimpleRecursiveObjectProvider struct {
	mapper storage.TupleMapper
	ts     *typesystem.TypeSystem
	ds     storage.RelationshipTupleReader
}

func NewSimpleRecursiveObjectProvider(ts *typesystem.TypeSystem, ds storage.RelationshipTupleReader) (*SimpleRecursiveObjectProvider, error) {
	if ts == nil || ds == nil {
		return nil, fmt.Errorf("%w: nil arguments", openfgaErrors.ErrUnknown)
	}
	return &SimpleRecursiveObjectProvider{ts: ts, ds: ds}, nil
}

var _ ObjectProvider = (*SimpleRecursiveObjectProvider)(nil)

func (s *SimpleRecursiveObjectProvider) End() {
	if s.mapper != nil {
		s.mapper.Stop()
	}
}

//nolint:revive // TODO make usersetMessage an exported struct
func (s *SimpleRecursiveObjectProvider) Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
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
	userToUsersetMessageChan := streamedLookupUsersetFromIterator(ctx, usersetFromUserIter)

	return userToUsersetMessageChan, nil
}

type ComplexRecursiveObjectProvider struct {
	concurrencyLimit uint32
	wg               sync.WaitGroup
	ts               *typesystem.TypeSystem
}

func NewComplexRecursiveObjectProvider(concurrencyLimit uint32, ts *typesystem.TypeSystem) (*ComplexRecursiveObjectProvider, error) {
	if ts == nil {
		return nil, fmt.Errorf("%w: nil typesystem", openfgaErrors.ErrUnknown)
	}
	return &ComplexRecursiveObjectProvider{concurrencyLimit: concurrencyLimit, wg: sync.WaitGroup{}, ts: ts}, nil
}

var _ ObjectProvider = (*ComplexRecursiveObjectProvider)(nil)

func (c *ComplexRecursiveObjectProvider) End() {
	c.wg.Wait()
}

//nolint:revive // TODO make usersetMessage an exported struct
func (c *ComplexRecursiveObjectProvider) Begin(ctx context.Context, req *ResolveCheckRequest) (chan usersetMessage, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", openfgaErrors.ErrUnknown)
	}
	objectType, relation := tuple.GetType(req.GetTupleKey().GetObject()), req.GetTupleKey().GetRelation()
	userType := tuple.GetType(req.GetTupleKey().GetUser())
	operands, ok := c.ts.IsRelationWithRecursiveTTUAndAlgebraicOperations(objectType, relation, userType)
	if !ok {
		return nil, fmt.Errorf("%w: unsupported model", openfgaErrors.ErrUnknown)
	}

	outChannel := make(chan usersetMessage, 1)
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
