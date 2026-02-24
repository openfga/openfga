package check

import (
	"context"
	"sync"

	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

const IteratorMinBatchThreshold = 100
const BaseIndex = 0
const DifferenceIndex = 1

type Weight2 struct {
	bottomUp  *bottomUp
	model     *modelgraph.AuthorizationModelGraph
	datastore storage.RelationshipTupleReader
}

func NewWeight2(model *modelgraph.AuthorizationModelGraph, ds storage.RelationshipTupleReader) *Weight2 {
	return &Weight2{
		bottomUp:  newBottomUp(model, ds),
		model:     model,
		datastore: ds,
	}
}

func (s *Weight2) Userset(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, _ *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "weight2.Userset")
	defer span.End()

	objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	childReq := req.cloneWithTupleKey(tuple.NewTupleKey(tuple.BuildObject(objectType, "ignore"), relation, req.GetTupleKey().GetUser()))

	leftChan, err := s.bottomUp.resolveRewrite(ctx, childReq, edge.GetTo())
	if err != nil {
		return nil, err
	}
	return s.execute(ctx, leftChan, storage.WrapIterator(storage.UsersetKind, iter))
}

func (s *Weight2) TTU(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, _ *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "weight2.TTU")
	defer span.End()

	objectType, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	childReq := req.cloneWithTupleKey(tuple.NewTupleKey(tuple.BuildObject(objectType, "ignore"), computedRelation, req.GetTupleKey().GetUser()))

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
func (s *Weight2) execute(ctx context.Context, leftChan chan *iterator.Msg, rightIter storage.TupleMapper) (*Response, error) {
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
	lastLeftVal := ""
	lastRightVal := ""

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
					return &Response{Allowed: false}, lastErr
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
						return &Response{Allowed: true}, nil
					}

					// we can take this solution to reduce storage because the data is delivered ordered in each channel
					// if this is the first value always store it
					// if there is not right value yet, then we should always store it
					// if the value is greater than the last right value, then we should store it
					// otherwise do not store it, it will never match.
					if lastLeftVal == "" || lastRightVal == "" || t > lastRightVal {
						lastLeftVal = t
						leftSeen[t] = struct{}{}
					}
				}
			}

		// Process an item from the right channel
		case rightMsg, ok := <-rightChan:
			if !ok {
				rightChan = nil
				if len(rightSeen) == 0 {
					// If we've processed nothing from the right side, we can't have any intersection
					return &Response{Allowed: false}, lastErr
				}
				continue
			}

			if rightMsg.Err != nil {
				lastErr = rightMsg.Err
				continue
			}

			// Check if this value exists in the left set first (early match)
			if _, exists := leftSeen[rightMsg.Value]; exists {
				return &Response{Allowed: true}, nil
			}

			// we can take this solution to reduce storage because the data is delivered ordered in each channel
			// if this is the first value always store it
			// if there is not left value yet, then we should always store it
			// if the value is greater than the last left value, then we should store it
			// otherwise do not store it, it will never match.
			if lastLeftVal == "" || lastRightVal == "" || rightMsg.Value > lastLeftVal {
				lastRightVal = rightMsg.Value
				rightSeen[rightMsg.Value] = struct{}{}
			}
		}
	}

	// If we get here, no match was found
	return &Response{Allowed: false}, lastErr
}
