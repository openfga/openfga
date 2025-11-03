package strategies

import (
	"context"

	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

const IteratorMinBatchThreshold = 100
const BaseIndex = 0
const DifferenceIndex = 1

type Weight2 struct {
	bottomUp  *bottomUp
	model     *check.AuthorizationModelGraph
	datastore storage.RelationshipTupleReader
}

func NewWeight2(model *check.AuthorizationModelGraph, ds storage.RelationshipTupleReader) *Weight2 {
	return &Weight2{
		bottomUp:  newBottomUp(model, ds),
		model:     model,
		datastore: ds,
	}
}

func (s *Weight2) Userset(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "weight2.Userset")
	defer span.End()

	objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	childReq, err := check.NewRequest(check.RequestParams{
		StoreID:                   req.GetStoreID(),
		TupleKey:                  tuple.NewTupleKey(tuple.BuildObject(objectType, "ignore"), relation, req.GetTupleKey().GetUser()),
		ContextualTuples:          req.GetContextualTuples(),
		Context:                   req.GetContext(),
		Consistency:               req.GetConsistency(),
		LastCacheInvalidationTime: req.GetLastCacheInvalidationTime(),
		AuthorizationModelID:      req.GetAuthorizationModelID(),
	})
	if err != nil {
		return nil, err
	}
	leftChan, err := s.bottomUp.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}
	return s.execute(ctx, leftChan, storage.WrapIterator(storage.UsersetKind, iter))
}

func (s *Weight2) TTU(ctx context.Context, req *check.Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator) (*check.Response, error) {
	ctx, span := tracer.Start(ctx, "weight2.Userset")
	defer span.End()

	objectType, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	childReq, err := check.NewRequest(check.RequestParams{
		StoreID:                   req.GetStoreID(),
		TupleKey:                  tuple.NewTupleKey(objectType, computedRelation, req.GetTupleKey().GetUser()),
		ContextualTuples:          req.GetContextualTuples(),
		Context:                   req.GetContext(),
		Consistency:               req.GetConsistency(),
		LastCacheInvalidationTime: req.GetLastCacheInvalidationTime(),
		AuthorizationModelID:      req.GetAuthorizationModelID(),
	})
	if err != nil {
		return nil, err
	}

	leftChan, err := s.bottomUp.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}

	return s.execute(ctx, leftChan, storage.WrapIterator(storage.TTUKind, iter))
}

// Weight2 attempts to find the intersection across 2 producers (channels) of ObjectIDs.
// In the case of a TTU:
// Right channel is the result set of the Read of ObjectID/Relation that yields the User's ObjectID.
// Left channel is the result set of ReadStartingWithUser of User/Relation that yields Object's ObjectID.
// From the perspective of the model, the left hand side of a TTU is the computed relationship being expanded.
func (s *Weight2) execute(ctx context.Context, leftChan chan *iterator.Msg, rightIter storage.TupleMapper) (*check.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer rightIter.Stop()
	defer iterator.Drain(leftChan)

	// Set to store already seen values from each side
	// We use maps for O(1) lookup complexity, consistent with hashset implementation
	leftSeen := make(map[string]struct{})
	rightSeen := make(map[string]struct{})

	// Convert right iterator to channel for uniform processing
	rightChan := iterator.ToChannel[string](ctx, rightIter, IteratorMinBatchThreshold)

	var lastErr error

	// Process both channels concurrently
	for leftChan != nil || rightChan != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		// Process an item from the left channel
		case leftMsg, ok := <-leftChan:
			if !ok {
				leftChan = nil
				if len(leftSeen) == 0 {
					// If we've processed nothing from the left side, we can't have any intersection
					return &check.Response{Allowed: false}, lastErr
				}
				continue
			}

			if leftMsg.Err != nil {
				lastErr = leftMsg.Err
				continue
			}

			// Process all items from this iterator message
			if leftMsg.Iter != nil {
				for {
					t, err := leftMsg.Iter.Next(ctx)
					if err != nil {
						leftMsg.Iter.Stop()
						if storage.IterIsDoneOrCancelled(err) {
							break
						}
						lastErr = err
						continue
					}

					// Check if this value exists in the right set first (early match)
					if _, exists := rightSeen[t]; exists {
						leftMsg.Iter.Stop() // Stop the iterator early
						return &check.Response{Allowed: true}, nil
					}

					// Otherwise, store for future comparison
					leftSeen[t] = struct{}{}
				}
			}

		// Process an item from the right channel
		case rightMsg, ok := <-rightChan:
			if !ok {
				rightChan = nil
				continue
			}

			if rightMsg.Err != nil {
				lastErr = rightMsg.Err
				continue
			}

			// Check if this value exists in the left set first (early match)
			if _, exists := leftSeen[rightMsg.Value]; exists {
				return &check.Response{Allowed: true}, nil
			}

			// Otherwise, store for future comparison
			rightSeen[rightMsg.Value] = struct{}{}
		}
	}

	// If we get here, no match was found
	return &check.Response{Allowed: false}, lastErr
}
