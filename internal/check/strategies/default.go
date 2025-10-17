package strategies

import (
	"context"
	"errors"

	authzGraph "github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/internal/check"
	"go.opentelemetry.io/otel"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var tracer = otel.Tracer("internal/check/strategies")

type requestMsg struct {
	err          error
	shortCircuit bool
	req          *check.Request
}

type defaultStrategy struct {
	resolver *check.Resolver
}

func NewDefault(resolver *check.Resolver) check.Strategy {
	return &defaultStrategy{
		resolver: resolver,
	}
}

// defaultUserset will check userset path.
// This is the slow path as it requires dispatch on all its children.
func (s *defaultStrategy) Userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	return s.execute(ctx, req, edge, iter, s.userset)
}

func (s *defaultStrategy) userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, out chan requestMsg) {
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

func (s *defaultStrategy) TTU(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	return s.execute(ctx, req, edge, iter, s.ttu)
}

func (s *defaultStrategy) ttu(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, out chan requestMsg) {

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
		tupleKey := tuple.NewTupleKey(userObj, computedRelation, req.GetTupleKey().GetUser())
		childReq, err := check.NewRequest(check.RequestParams{
			StoreID:                   req.GetStoreID(),
			TupleKey:                  tupleKey,
			ContextualTuples:          req.GetContextualTuples(),
			Context:                   req.GetContext(),
			Consistency:               req.GetConsistency(),
			LastCacheInvalidationTime: req.GetLastCacheInvalidationTime(),
			AuthorizationModelID:      req.GetAuthorizationModelID(),
		})
		concurrency.TrySendThroughChannel(ctx, requestMsg{req: childReq}, out)
	}
}

func (s *defaultStrategy) execute(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, handler defaultStrategyHandler) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "defaultStrategy")
	defer span.End()

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
				continue // continue
			}

			if outcome.Res.Allowed {
				return outcome.Res, nil
			}
		}
	}
}

// processDispatches returns a channel where the outcomes of the dispatched checks are sent, and begins sending messages to this channel.
func (s *defaultStrategy) processRequests(ctx context.Context, requests chan requestMsg, out chan check.ResponseMsg) {
	// TODO: do we want the Resolver to control the concurrency internally instead? if so, we can just create a buffered channel with a fixed size here
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
				continue // TODO: continue or return?
			}
			if msg.shortCircuit {
				concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Res: &check.Response{Allowed: true}}, out)
				return
			}

			res, err := s.resolver.ResolveCheck(ctx, msg.req)
			concurrency.TrySendThroughChannel(ctx, check.ResponseMsg{Err: err, Res: res}, out)
		}
	}
}
