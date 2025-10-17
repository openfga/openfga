package strategies

import (
	"context"
	"errors"
	"fmt"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/graph"
	"github.com/sourcegraph/conc/panics"
	"go.opentelemetry.io/otel"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var tracer = otel.Tracer("internal/check/strategies")

const defaultResolver = "default"

var defaultPlan = &planner.KeyPlanStrategy{
	Type:         defaultResolver,
	InitialGuess: 50 * time.Millisecond,
	// Low Lambda: Represents zero confidence. It's a pure guess.
	Lambda: 1,
	// With α = 0.5 ≤ 1, it means maximum uncertainty about variance; with λ = 1, we also have weak confidence in the mean.
	// These values will encourage strong exploration of other strategies. Having these values for the default strategy helps to enforce the usage of the "faster" strategies,
	// helping out with the cold start when we don't have enough data.
	Alpha: 0.5,
	Beta:  0.5,
}

var defaultRecursivePlan = &planner.KeyPlanStrategy{
	Type:         defaultResolver,
	InitialGuess: 300 * time.Millisecond, // Higher initial guess for recursive checks
	// Low Lambda: Represents zero confidence. It's a pure guess.
	Lambda: 1,
	// With α = 0.5 ≤ 1, it means maximum uncertainty about variance; with λ = 1, we also have weak confidence in the mean.
	// These values will encourage strong exploration of other strategies. Having these values for the default strategy helps to enforce the usage of the "faster" strategies,
	// helping out with the cold start when we don't have enough data.
	Alpha: 0.5,
	Beta:  0.5,
}

type requestMsg struct {
	err          error
	shortCircuit bool
	req          *check.Request
}

type DefaultStrategy struct {
	resolver *check.Resolver
}

func NewDefaultStrategy(resolver *check.Resolver) *DefaultStrategy {
	return &DefaultStrategy{
		resolver: resolver,
	}
}

// defaultUserset will check userset path.
// This is the slow path as it requires dispatch on all its children.
func (s *DefaultStrategy) Userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	return defaultStrategy(ctx, req, edge, iter, s.userset)
}

func (s *DefaultStrategy) userset(ctx context.Context, req *check.Request, iter storage.TupleKeyIterator, out chan requestMsg) {
	defer close(out)
	for {
		t, err := iter.Next(ctx)
		if err != nil {
			// cancelled doesn't need to flush nor send errors back to main routine
			if storage.IterIsDoneOrCancelled(err) {
				return
			}
			concurrency.TrySendThroughChannel(ctx, requestMsg{err: err}, out)
			return
		}

		usersetObject, usersetRelation := tuple.SplitObjectRelation(t.GetUser())

		// if the user value is a typed wildcard and the type of the wildcard
		// matches the target user objectType, then we're done searching
		if tuple.IsTypedWildcard(usersetObject) {
			wildcardType := tuple.GetType(usersetObject)

			if req.GetUserType() == wildcardType {
				concurrency.TrySendThroughChannel(ctx, requestMsg{shortCircuit: true}, out)
				return
			}
		}

		if usersetRelation != "" {
			tupleKey := tuple.NewTupleKey(usersetObject, usersetRelation, req.GetTupleKey().GetUser())
			childReq, err := check.NewRequest(check.RequestParams{
				StoreID:                   req.GetStoreID(),
				TupleKey:                  tupleKey,
				ContextualTuples:          req.GetContextualTuples(),
				Context:                   req.GetContext(),
				Consistency:               req.GetConsistency(),
				LastCacheInvalidationTime: req.GetLastCacheInvalidationTime(),
				AuthorizationModelID:      req.GetAuthorizationModelID(),
			})
			concurrency.TrySendThroughChannel(ctx, requestMsg{err: err, req: childReq}, out)
		}
	}
}

func (s *DefaultStrategy) TTU(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	return defaultStrategy(ctx, req, edge, iter, s.ttu)
}

func (s *DefaultStrategy) ttu(ctx context.Context, req *check.Request, iter storage.TupleKeyIterator, out chan requestMsg) {
	defer close(out)
	for {
		t, err := iter.Next(ctx)
		if err != nil {
			// cancelled doesn't need to flush nor send errors back to main routine
			if storage.IterIsDoneOrCancelled(err) {
				return
			}
			concurrency.TrySendThroughChannel(ctx, requestMsg{err: err}, out)
			return
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
			User:     req.GetTupleKey().GetUser(),
		}

		concurrency.TrySendThroughChannel(ctx, requestMsg{dispatchParams: &dispatchParams{parentReq: req, tk: tupleKey}}, out)
	}
}

func defaultStrategy(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, handler strategyHandler) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "defaultStrategy")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dispatchChan := make(chan requestMsg)

	go func() {
		handler(ctx, req, iter, dispatchChan)
	}()

	return
}

func (c *LocalChecker) consumeDispatches(ctx context.Context, limit int, dispatchChan chan requestMsg) (*ResolveCheckResponse, error) {
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
func (c *LocalChecker) processDispatches(ctx context.Context, limit int, dispatchChan chan requestMsg) <-chan checkOutcome {
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
					concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{err: msg.err}, outcomes)
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
