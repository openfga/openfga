package check

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/sourcegraph/conc/panics"
	"golang.org/x/sync/errgroup"

	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type defaultStrategyHandler func(context.Context, *Request, *authzGraph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator, chan requestMsg)

type requestMsg struct {
	err       error
	req       *Request
	tupleKey  string // The tuple key that led to this request (for resolution tracing)
	tupleUser string // The user from the tuple (for resolution tracing)
}

type DefaultStrategy struct {
	model            *modelgraph.AuthorizationModelGraph
	resolver         CheckResolver
	concurrencyLimit int
}

func NewDefault(model *modelgraph.AuthorizationModelGraph, resolver CheckResolver, limit int) *DefaultStrategy {
	return &DefaultStrategy{
		model:            model,
		resolver:         resolver,
		concurrencyLimit: limit,
	}
}

// defaultUserset will check userset path.
// This is the slow path as it requires dispatch on all its children.
func (s *DefaultStrategy) Userset(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, visited *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "default.Userset")
	defer span.End()

	return s.execute(ctx, req, edge, iter, s.userset, visited)
}

func (s *DefaultStrategy) userset(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, out chan requestMsg) {
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
		childReq := req.cloneWithTupleKey(tuple.NewTupleKey(usersetObject, usersetRelation, req.GetTupleKey().GetUser()))

		msg := requestMsg{req: childReq}
		if req.GetTraceResolution() {
			msg.tupleKey = tuple.TupleKeyToString(t)
			msg.tupleUser = t.GetUser()
		}
		concurrency.TrySendThroughChannel(ctx, msg, out)
	}
}

func (s *DefaultStrategy) TTU(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, visited *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "default.TTU")
	defer span.End()

	return s.execute(ctx, req, edge, iter, s.ttu, visited)
}

func (s *DefaultStrategy) ttu(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, out chan requestMsg) {
	defer close(out)
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
		childReq := req.cloneWithTupleKey(tuple.NewTupleKey(userObj, computedRelation, req.GetTupleKey().GetUser()))

		msg := requestMsg{req: childReq}
		if req.GetTraceResolution() {
			msg.tupleKey = tuple.TupleKeyToString(t)
			msg.tupleUser = t.GetUser()
		}
		concurrency.TrySendThroughChannel(ctx, msg, out)
	}
}

func (s *DefaultStrategy) execute(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, handler defaultStrategyHandler, visited *sync.Map) (*Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	requestsChan := make(chan requestMsg)
	responsesChan := make(chan ResponseMsg, s.concurrencyLimit)

	tracing := req.GetTraceResolution()
	_, relation := tuple.SplitObjectRelation(edge.GetRelationDefinition())

	go func() {
		handler(ctx, req, edge, iter, requestsChan)
	}()

	go func() {
		s.processRequests(ctx, requestsChan, visited, responsesChan)
	}()

	var err error
	var tupleNodes []*TupleNode
	if tracing {
		tupleNodes = make([]*TupleNode, 0)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case outcome, ok := <-responsesChan:
			if !ok {
				// Channel closed - return with collected tuples
				res := &Response{Allowed: false}
				if tracing {
					resNode := NewResolutionNode(req.GetTupleKey().GetObject(), relation)
					resNode.Tuples = tupleNodes
					resNode.Complete(false, 0)
					res.Resolution = &ResolutionTree{Tree: resNode}
				}
				return res, err
			}
			if outcome.Err != nil {
				err = outcome.Err
				continue
			}

			// Track tuple and its computed resolution if tracing
			if tracing && outcome.ID != "" {
				parts := strings.SplitN(outcome.ID, "|", 2)
				if len(parts) == 2 && parts[0] != "" {
					tupleNode := NewTupleNodeFromString(parts[0])
					if outcome.Res.GetResolutionNode() != nil {
						tupleNode.Computed = outcome.Res.GetResolutionNode()
					}
					tupleNodes = append(tupleNodes, tupleNode)
				}
			}

			if outcome.Res.Allowed {
				res := &Response{Allowed: true}
				if tracing {
					resNode := NewResolutionNode(req.GetTupleKey().GetObject(), relation)
					resNode.Tuples = tupleNodes
					resNode.Complete(true, len(tupleNodes))
					res.Resolution = &ResolutionTree{Tree: resNode}
				}
				return res, nil
			}
		}
	}
}

// processDispatches returns a channel where the outcomes of the dispatched checks are sent, and begins sending messages to this channel.
func (s *DefaultStrategy) processRequests(ctx context.Context, requests chan requestMsg, visited *sync.Map, out chan ResponseMsg) {
	var pool errgroup.Group
	pool.SetLimit(s.concurrencyLimit)
	defer func() {
		_ = pool.Wait()
		close(out)
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
				concurrency.TrySendThroughChannel(ctx, ResponseMsg{Err: msg.err}, out)
				continue
			}

			node, ok := s.model.GetNodeByID(tuple.ToObjectRelationString(msg.req.GetObjectType(), msg.req.GetTupleKey().GetRelation()))
			if !ok {
				concurrency.TrySendThroughChannel(ctx, ResponseMsg{Err: modelgraph.ErrGraphError}, out)
				continue
			}

			// Capture tuple info for resolution tracing
			tupleKey := msg.tupleKey
			tupleUser := msg.tupleUser

			pool.Go(func() error {
				var res *Response
				var err error
				recoveredErr := panics.Try(func() {
					res, err = s.resolver.ResolveUnion(ctx, msg.req, node, visited)
				})
				if recoveredErr != nil {
					err = fmt.Errorf("%w: %w", ErrPanicRequest, recoveredErr.AsError())
				}
				// Use ID field to pass tuple key for resolution tracing
				concurrency.TrySendThroughChannel(ctx, ResponseMsg{ID: tupleKey + "|" + tupleUser, Err: err, Res: res}, out)
				return nil
			})
		}
	}
}
