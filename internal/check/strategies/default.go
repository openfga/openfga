package strategies

import (
	"context"
	"fmt"

	"github.com/openfga/openfga/internal/graph"
	"github.com/sourcegraph/conc/panics"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

var tracer = otel.Tracer("internal/check/strategies")

type requestMsg struct {
	err error
	req *check.Request
}

type DefaultStrategy struct {
	resolver         graph.CheckResolver
	concurrencyLimit int
}

func NewDefault(resolver graph.CheckResolver, limit int) *DefaultStrategy {
	return &DefaultStrategy{
		concurrencyLimit: limit,
		resolver:         resolver,
	}
}

// defaultUserset will check userset path.
// This is the slow path as it requires dispatch on all its children.
func (s *DefaultStrategy) Userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "default.Userset")
	defer span.End()

	return s.execute(ctx, req, edge, iter, s.userset)
}

func (s *DefaultStrategy) userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, out chan requestMsg) {
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
		childReq, err := check.NewRequest(check.RequestParams{
			StoreID:                   req.GetStoreID(),
			TupleKey:                  tuple.NewTupleKey(usersetObject, usersetRelation, req.GetTupleKey().GetUser()),
			ContextualTuples:          req.GetContextualTuples(),
			Context:                   req.GetContext(),
			Consistency:               req.GetConsistency(),
			LastCacheInvalidationTime: req.GetLastCacheInvalidationTime(),
			AuthorizationModelID:      req.GetAuthorizationModelID(),
		})
		concurrency.TrySendThroughChannel(ctx, requestMsg{req: childReq, err: err}, out)
	}
}

func (s *DefaultStrategy) TTU(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "default.TTU")
	defer span.End()

	return s.execute(ctx, req, edge, iter, s.ttu)
}

func (s *DefaultStrategy) ttu(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, out chan requestMsg) {
	_, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
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
		childReq, err := check.NewRequest(check.RequestParams{
			StoreID:                   req.GetStoreID(),
			TupleKey:                  tuple.NewTupleKey(userObj, computedRelation, req.GetTupleKey().GetUser()),
			ContextualTuples:          req.GetContextualTuples(),
			Context:                   req.GetContext(),
			Consistency:               req.GetConsistency(),
			LastCacheInvalidationTime: req.GetLastCacheInvalidationTime(),
			AuthorizationModelID:      req.GetAuthorizationModelID(),
		})
		concurrency.TrySendThroughChannel(ctx, requestMsg{req: childReq, err: err}, out)
	}
}

func (s *DefaultStrategy) execute(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, handler defaultStrategyHandler) (*check.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	requestsChan := make(chan requestMsg)
	responsesChan := make(chan check.ResponseMsg, 100) // needs to be buffered to prevent out of order closed events

	go func() {
		handler(ctx, req, edge, iter, requestsChan)
		close(requestsChan)
	}()

	go func() {
		s.processRequests(ctx, requestsChan, responsesChan)
		close(responsesChan)
	}()

	var err error
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case outcome, ok := <-responsesChan:
			if !ok {
				return &check.Response{Allowed: false}, err
			}
			if outcome.Err != nil {
				err = outcome.Err
				continue
			}

			if outcome.Res.Allowed {
				return outcome.Res, nil
			}
		}
	}
}

// processDispatches returns a channel where the outcomes of the dispatched checks are sent, and begins sending messages to this channel.
func (s *DefaultStrategy) processRequests(ctx context.Context, requests chan requestMsg, out chan check.ResponseMsg) {
	var pool errgroup.Group
	pool.SetLimit(s.concurrencyLimit)
	defer func() {
		_ = pool.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-requests:
			if !ok {
				return
			}
			if msg.err != nil {
				concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Err: msg.err}, out)
				continue
			}

			pool.Go(func() error {
				var res *check.Response
				var err error
				recoveredErr := panics.Try(func() {
					res, err = s.resolver.GetDelegate().ResolveCheck(ctx, msg.req)
				})
				if recoveredErr != nil {
					err = fmt.Errorf("%w: %s", check.ErrPanicRequest, recoveredErr.AsError())
				}
				concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Err: err, Res: res}, out)
				return nil
			})
		}
	}
}
