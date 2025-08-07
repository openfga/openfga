package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/sourcegraph/conc/panics"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type dispatchParams struct {
	parentReq *ResolveCheckRequest
	tk        *openfgav1.TupleKey
}

type dispatchMsg struct {
	err            error
	shortCircuit   bool
	dispatchParams *dispatchParams
}

// defaultUserset will check userset path.
// This is the slow path as it requires dispatch on all its children.
func (c *LocalChecker) defaultUserset(ctx context.Context, req *ResolveCheckRequest, iter storage.TupleKeyIterator) (resp *ResolveCheckResponse, err error) {
	dispatchChan := make(chan dispatchMsg, c.concurrencyLimit)

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	pool := concurrency.NewPool(cancellableCtx, 1)
	defer func() {
		cancelFunc()
		// We need to wait always to avoid a goroutine leak.
		poolErr := pool.Wait()
		if poolErr != nil {
			err = poolErr
			resp = nil
		}
	}()
	pool.Go(func(ctx context.Context) error {
		recoveredError := panics.Try(func() {
			c.produceUsersetDispatches(ctx, req, dispatchChan, iter)
		})

		if recoveredError != nil {
			return fmt.Errorf("%w: %s", ErrPanic, recoveredError.AsError())
		}
		return nil
	})

	resp, err = c.consumeDispatches(ctx, c.concurrencyLimit, dispatchChan)
	if err != nil {
		return
	}

	return
}

func (c *LocalChecker) produceUsersetDispatches(ctx context.Context, req *ResolveCheckRequest, dispatches chan dispatchMsg, iter storage.TupleKeyIterator) {
	defer close(dispatches)
	reqTupleKey := req.GetTupleKey()
	typesys, _ := typesystem.TypesystemFromContext(ctx)
	for {
		t, err := iter.Next(ctx)
		if err != nil {
			// cancelled doesn't need to flush nor send errors back to main routine
			if storage.IterIsDoneOrCancelled(err) {
				break
			}
			concurrency.TrySendThroughChannel(ctx, dispatchMsg{err: err}, dispatches)
			break
		}

		usersetObject, usersetRelation := tuple.SplitObjectRelation(t.GetUser())

		// if the user value is a typed wildcard and the type of the wildcard
		// matches the target user objectType, then we're done searching
		if tuple.IsTypedWildcard(usersetObject) && typesystem.IsSchemaVersionSupported(typesys.GetSchemaVersion()) {
			wildcardType := tuple.GetType(usersetObject)

			if tuple.GetType(reqTupleKey.GetUser()) == wildcardType {
				concurrency.TrySendThroughChannel(ctx, dispatchMsg{shortCircuit: true}, dispatches)
				break
			}
		}

		if usersetRelation != "" {
			tupleKey := tuple.NewTupleKey(usersetObject, usersetRelation, reqTupleKey.GetUser())
			concurrency.TrySendThroughChannel(ctx, dispatchMsg{dispatchParams: &dispatchParams{parentReq: req, tk: tupleKey}}, dispatches)
		}
	}
}

// defaultTTU is the slow path for checkTTU where we cannot short-circuit TTU evaluation and
// resort to dispatch check on its children.
func (c *LocalChecker) defaultTTU(ctx context.Context, req *ResolveCheckRequest, rewrite *openfgav1.Userset, iter storage.TupleKeyIterator) (*ResolveCheckResponse, error) {
	computedRelation := rewrite.GetTupleToUserset().GetComputedUserset().GetRelation()

	dispatchChan := make(chan dispatchMsg, c.concurrencyLimit)

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	// sending to channel in batches up to a pre-configured value to subsequently checkMembership for.
	pool := concurrency.NewPool(cancellableCtx, 1)
	defer func() {
		cancelFunc()
		// We need to wait always to avoid a goroutine leak.
		_ = pool.Wait()
	}()
	pool.Go(func(ctx context.Context) error {
		c.produceTTUDispatches(ctx, computedRelation, req, dispatchChan, iter)
		return nil
	})

	resp, err := c.consumeDispatches(ctx, c.concurrencyLimit, dispatchChan)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *LocalChecker) produceTTUDispatches(ctx context.Context, computedRelation string, req *ResolveCheckRequest, dispatches chan dispatchMsg, iter storage.TupleKeyIterator) {
	defer close(dispatches)
	reqTupleKey := req.GetTupleKey()
	typesys, _ := typesystem.TypesystemFromContext(ctx)

	for {
		t, err := iter.Next(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				break
			}
			concurrency.TrySendThroughChannel(ctx, dispatchMsg{err: err}, dispatches)
			break
		}

		userObj, _ := tuple.SplitObjectRelation(t.GetUser())
		if _, err := typesys.GetRelation(tuple.GetType(userObj), computedRelation); err != nil {
			if errors.Is(err, typesystem.ErrRelationUndefined) {
				continue // skip computed relations on tupleset relationships if they are undefined
			}
		}

		tupleKey := &openfgav1.TupleKey{
			Object:   userObj,
			Relation: computedRelation,
			User:     reqTupleKey.GetUser(),
		}

		concurrency.TrySendThroughChannel(ctx, dispatchMsg{dispatchParams: &dispatchParams{parentReq: req, tk: tupleKey}}, dispatches)
	}
}

func (c *LocalChecker) consumeDispatches(ctx context.Context, limit int, dispatchChan chan dispatchMsg) (*ResolveCheckResponse, error) {
	cancellableCtx, cancel := context.WithCancel(ctx)
	outcomeChannel := c.processDispatches(cancellableCtx, limit, dispatchChan)

	var finalErr error
	finalResult := &ResolveCheckResponse{
		Allowed: false,
	}

ConsumerLoop:
	for {
		select {
		case <-ctx.Done():
			break ConsumerLoop
		case outcome, ok := <-outcomeChannel:
			if !ok {
				break ConsumerLoop
			}
			if outcome.err != nil {
				finalErr = outcome.err
				break // continue
			}

			if outcome.resp.GetResolutionMetadata().CycleDetected {
				finalResult.ResolutionMetadata.CycleDetected = true
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

// processDispatches returns a channel where the outcomes of the dispatched checks are sent, and begins sending messages to this channel.
func (c *LocalChecker) processDispatches(ctx context.Context, limit int, dispatchChan chan dispatchMsg) <-chan checkOutcome {
	outcomes := make(chan checkOutcome, limit)
	dispatchPool := concurrency.NewPool(ctx, limit)

	go func() {
		defer func() {
			// We need to wait always to avoid a goroutine leak.
			_ = dispatchPool.Wait()
			close(outcomes)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-dispatchChan:
				if !ok {
					return
				}
				if msg.err != nil {
					concurrency.TrySendThroughChannel(ctx, checkOutcome{err: msg.err}, outcomes)
					break // continue
				}
				if msg.shortCircuit {
					resp := &ResolveCheckResponse{
						Allowed: true,
					}
					concurrency.TrySendThroughChannel(ctx, checkOutcome{resp: resp}, outcomes)
					return
				}

				if msg.dispatchParams != nil {
					dispatchPool.Go(func(ctx context.Context) error {
						recoveredError := panics.Try(func() {
							resp, err := c.dispatch(ctx, msg.dispatchParams.parentReq, msg.dispatchParams.tk)(ctx)
							concurrency.TrySendThroughChannel(ctx, checkOutcome{resp: resp, err: err}, outcomes)
						})
						if recoveredError != nil {
							concurrency.TrySendThroughChannel(
								ctx,
								checkOutcome{err: fmt.Errorf("%w: %s", ErrPanic, recoveredError.AsError())},
								outcomes,
							)
						}
						return nil
					})
				}
			}
		}
	}()

	return outcomes
}
