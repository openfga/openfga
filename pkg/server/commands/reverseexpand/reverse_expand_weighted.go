package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"sync"
)

// TODO: this stack might need to be a stack of UserRefObject or UserRefObjectRelation or something
// to be able to handle usersets and TTUs properly
// relationStack is a stack of type#rel strings we build while traversing the graph to locate leaf nodes
type relationStack []string

func (r *relationStack) Push(value string) {
	*r = append(*r, value)
}

func (r *relationStack) Pop() string {
	element := (*r)[len(*r)-1]
	*r = (*r)[0 : len(*r)-1]
	return element
}

func (r *relationStack) Copy() []string {
	dst := make(relationStack, len(*r)) // Create a new slice with the same length
	copy(dst, *r)
	return dst
}

func (r *relationStack) Len() int {
	return len(*r)
}

// loopOverWeightedEdges iterates over a set of weightedGraphEdges that can resolve a particular
// object-relation pair. It acts as a dispatcher, processing each edge according to its type to continue the
// reverse expansion process. It will continue to dispatch and traverse the graph until it reaches a DirectEdge, which
// leads to a leaf node in the authorization graph. Once a DirectEdge is found, loopOverWeightedEdges invokes
// queryForTuples, passing it the stack of relations it constructed on the way to that particular leaf.
//
// For each edge, it creates a new ReverseExpandRequest, preserving the context of the overall query
// but updating the traversal state (the 'stack') based on the edge being processed.
//
// The behavior is determined by the edge type:
//
//   - DirectEdge: This represents a direct path to data. The function initiates a concurrent
//     `queryForTuples` call to query the datastore for tuples that match the relationship path
//     accumulated in the stack. This is a terminal path in the graph traversal.
//
//   - ComputedEdge, RewriteEdge, and TTUEdge: These represent indirections in the authorization model.
//     The function modifies the traversal 'stack' to reflect the next relationship that needs to be resolved.
//     For example, for a TTU (Tuple-to-Userset) edge, it pushes both the tupleset relation and the
//     target userset onto the stack. It then recursively calls `dispatch` to continue resolving the
//     model with this new state until it reaches a DirectEdge.
//
// The function uses a concurrency pool to execute the `queryForTuples` calls in parallel,
// and it aggregates any errors encountered during the dispatch or querying phases.
func (c *ReverseExpandQuery) loopOverWeightedEdges(
	ctx context.Context,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	needsCheck bool,
	req *ReverseExpandRequest,
	resolutionMetadata *ResolutionMetadata,
	resultChan chan<- *ReverseExpandResult,
) error {
	pool := concurrency.NewPool(ctx, 1)

	var errs error

	for _, edge := range edges {
		r := &ReverseExpandRequest{
			Consistency:      req.Consistency,
			Context:          req.Context,
			ContextualTuples: req.ContextualTuples,
			ObjectType:       req.ObjectType,
			Relation:         req.Relation,
			StoreID:          req.StoreID,
			User:             req.User,

			weightedEdge:        edge,
			weightedEdgeTypeRel: edge.GetTo().GetUniqueLabel(),
			edge:                req.edge,
			stack:               req.stack.Copy(),
		}
		fmt.Printf("Justin Edge: %s --> %s Stack: %s\n",
			edge.GetFrom().GetUniqueLabel(),
			edge.GetTo().GetUniqueLabel(),
			r.stack.Copy(),
		)
		toNode := edge.GetTo()
		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			fmt.Printf("Justin Direct Edge: \n\t%s\n\t%s\n\t%s\n",
				edge.GetFrom().GetUniqueLabel(),
				toNode.GetUniqueLabel(),
				r.stack.Copy(),
			)
			// Now kick off queries for tuples based on the stack of relations we have built to get to this leaf
			pool.Go(func(ctx context.Context) error {
				return c.queryForTuples(
					ctx,
					r,
					needsCheck,
					resultChan,
				)
			})
		case weightedGraph.ComputedEdge:
			// TODO: removed logic in here that transformed the usersets to trigger bail out case
			// and prevent infinite loops. need to rebuild that loop prevention with weighted graph data.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				_ = r.stack.Pop()
				r.stack.Push(toNode.GetUniqueLabel())
				if toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation {
					fmt.Printf("JUSTIN computed edge going to USERSET: %s\n", toNode.GetUniqueLabel())
				}
			}

			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.TTUEdge:
			fmt.Printf("Justin TTU Edge before: \n\t%s\n\t%s\n\t%s\n",
				edge.GetFrom().GetUniqueLabel(),
				toNode.GetUniqueLabel(),
				r.stack.Copy(),
			)
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				// Replace the existing type#rel on the stack with the TTU relation
				_ = r.stack.Pop()
				r.stack.Push(edge.GetTuplesetRelation())
				r.stack.Push(toNode.GetUniqueLabel())
				if toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation {
					fmt.Printf("JUSTIN TTU edge going to USERSET: %s\n", toNode.GetUniqueLabel())
				}
			}
			fmt.Printf("Justin TTU Edge after: \n\t%s\n\t%s\n\t%s\n",
				edge.GetFrom().GetUniqueLabel(),
				toNode.GetUniqueLabel(),
				r.stack.Copy(),
			)
			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.RewriteEdge:
			// bc operator nodes are not real types
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				_ = r.stack.Pop()
				r.stack.Push(toNode.GetUniqueLabel())
				if toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation {
					fmt.Printf("JUSTIN REWRITE edge going to USERSET: %s\n", toNode.GetUniqueLabel())
				}
			}
			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		default:
			panic("unsupported edge type")
		}
	}

	return errors.Join(errs, pool.Wait())
}

// queryForTuples performs the core logic of the reverse expansion. After a leaf node has been found in loopOverWeightedEdges,
// this function works backwards from a specified user (using the stack created in loopOverWeightedEdges)
// and an initial relationship edge to find all the objects that the given user has the given relationship with.
//
// The function defines a recursive inner function, `queryFunc`, which is executed concurrently for different
// branches of the relationship graph.
//
// On its initial execution, it constructs a database query filter based on the starting user and the "To"
// part of the initial DirectEdge, which can be a direct user, a wildcard user, or a userset.
//
// In subsequent recursive calls, it takes an `objectOverride`—the object found in the previous step—and
// pops the next relation from the stack. It then queries for tuples where the `objectOverride` is the user,
// effectively continuing the traversal up the graph.
//
// For each tuple found from the datastore, it:
// 1. Filters out any invalid tuples based on the type system.
// 2. Evaluates any conditions associated with the tuple.
// 3. If the relation stack is empty (the base case), the object from the tuple is a potential result and is sent to the result channel.
// 4. If the relation stack is not empty (the recursive step), it spawns a new goroutine to continue querying with the found object.
func (c *ReverseExpandQuery) queryForTuples(
	ctx context.Context,
	req *ReverseExpandRequest,
	needsCheck bool,
	resultChan chan<- *ReverseExpandResult,
) error {
	// TODO: don't forget telemetry

	// Direct edges after TTUs can come in looking like this, with the To of a userset
	//	Justin Direct Edge:
	//	organization#repo_admin // From()
	//	organization#member // This was the To(). So in this case we'd need to NOT pop from the stack and query for
	//	[repo#owner]		// organizations this user is a member of right off the bat, then use the stack

	//if to.GetNodeType() != weightedGraph.SpecificTypeAndRelation {
	//	typeRel = to.GetUniqueLabel()
	//} else {
	//	typeRel = req.stack.Pop()
	//}

	var wg sync.WaitGroup
	errChan := make(chan error, 100) // random value here, needs tuning

	var queryFunc func(context.Context, *ReverseExpandRequest, string)

	queryFunc = func(qCtx context.Context, r *ReverseExpandRequest, objectOverride string) {
		defer wg.Done()
		if qCtx.Err() != nil {
			return
		}

		var typeRel string
		var userFilter []*openfgav1.ObjectRelation
		// this is true on every call Except the first
		if objectOverride != "" {
			typeRel = r.stack.Pop()
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: objectOverride,
			})
		} else {
			var userID string
			var userType string
			// We will always have a UserRefObject here. Queries that come in for pure usersets do not take this code path.
			// e.g. ListObjects(team:fga#member, document, viewer) will not make it here.
			if val, ok := req.User.(*UserRefObject); ok {
				userType = val.GetObjectType()
				userID = val.Object.GetId()
			}

			to := req.weightedEdge.GetTo()

			switch to.GetNodeType() {
			case weightedGraph.SpecificType: // Direct User Reference. To() -> "user"
				typeRel = req.stack.Pop()
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: tuple.BuildObject(to.GetUniqueLabel(), userID)})

			case weightedGraph.SpecificTypeWildcard: // Wildcard Referece To() -> "user:*"
				typeRel = req.stack.Pop()
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: to.GetUniqueLabel()})

			case weightedGraph.SpecificTypeAndRelation: // Userset, To() -> "group#member"
				typeRel = to.GetUniqueLabel()

				// For this case, we can't build the ObjectRelation with the To() from the edge, because it doesn't
				// Point at an actual type like "user". So we have to use the userType from the original request
				// to determine whether the requested user is a member of this userset
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: tuple.BuildObject(userType, userID)})
			}
		}

		objectType, relation := tuple.SplitObjectRelation(typeRel)
		fmt.Printf("JUSTIN querying: \n\tUserfilter: %s, relation: %s, objectType: %s\n",
			userFilter, relation, objectType,
		)
		iter, err := c.datastore.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
			ObjectType: objectType,
			Relation:   relation,
			UserFilter: userFilter,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: req.Consistency,
			},
		})

		if err != nil {
			errChan <- err
			return
		}

		// filter out invalid tuples yielded by the database iterator
		filteredIter := storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(iter),
			validation.FilterInvalidTuples(c.typesystem),
		)
		defer filteredIter.Stop()

	LoopOnIterator:
		for {
			tk, err := filteredIter.Next(ctx)
			if err != nil {
				if !errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				errChan <- err
				break LoopOnIterator
			}
			fmt.Printf("JUSTIN TK FOUND: %s\n", tk.String())

			condEvalResult, err := eval.EvaluateTupleCondition(ctx, tk, c.typesystem, req.Context)
			if err != nil {
				errChan <- err
				continue
			}

			if !condEvalResult.ConditionMet {
				if len(condEvalResult.MissingParameters) > 0 {
					errChan <- condition.NewEvaluationError(
						tk.GetCondition().GetName(),
						fmt.Errorf("tuple '%s' is missing context parameters '%v'",
							tuple.TupleKeyToString(tk),
							condEvalResult.MissingParameters),
					)
				}

				continue
			}
			// If there are no more type#rel to look for in the stack that means we have hit the base case
			// and this object is a candidate for return to the user.
			if r.stack.Len() == 0 {
				_ = c.trySendCandidate(ctx, needsCheck, tk.GetObject(), resultChan)
				continue
			} else {
				// if there are more relations in the stack, we need to evaluate the object found here against
				// the next type#rel one level higher in the tree.
				fmt.Println("Recursion")
				foundObject := tk.GetObject() // This will be a "type:id" e.g. "document:roadmap"

				newReq := &ReverseExpandRequest{
					StoreID:          r.StoreID,
					Consistency:      r.Consistency,
					Context:          r.Context,
					ContextualTuples: r.ContextualTuples,
					User:             r.User,
					stack:            r.stack.Copy(),
					weightedEdge:     r.weightedEdge, // Inherited but not used by the override path
				}

				wg.Add(1)
				go queryFunc(qCtx, newReq, foundObject)
				// so now we need to query this object against the last stack type#rel
				// e.g. organization:jz#member@user:justin
				// now we need organization:jz
			}
		}
	}

	// Now kick off the querying without an explicit object override for the first call
	wg.Add(1)
	go queryFunc(ctx, req, "")

	wg.Wait()
	close(errChan)
	var errs error
	for err := range errChan {
		errs = errors.Join(errs, err)
	}
	return errs
}
