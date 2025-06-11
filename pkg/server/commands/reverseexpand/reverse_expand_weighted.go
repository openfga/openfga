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

type typeRelEntry struct {
	typeRel string // e.g. "organization#admin"

	// Only present for userset relations. Useful for cases like "rel admin: [user, team#member]"
	// where we need to know to check for "team:fga#member"
	usersetRelation string
}

// TODO: this stack might need to be a stack of UserRefObject or UserRefObjectRelation or something
// to be able to handle usersets and TTUs properly
// relationStack is a stack of type#rel strings we build while traversing the graph to locate leaf nodes
type relationStack []typeRelEntry

func (r *relationStack) Push(value typeRelEntry) {
	*r = append(*r, value)
}

func (r *relationStack) Pop() typeRelEntry {
	element := (*r)[len(*r)-1]
	*r = (*r)[0 : len(*r)-1]
	return element
}

func (r *relationStack) Copy() []typeRelEntry {
	dst := make(relationStack, len(*r)) // Create a new slice with the same length
	copy(dst, *r)
	return dst
}

// loopOverWeightedEdges iterates over a set of weightedGraphEdges and acts as a dispatcher,
// processing each edge according to its type to continue the reverse expansion process.
//
// While traversing, loopOverWeightedEdges appends relation entries to a stack for use in querying after traversal is complete.
// It will continue to dispatch and traverse the graph until it reaches a DirectEdge, which
// leads to a leaf node in the authorization graph. Once a DirectEdge is found, loopOverWeightedEdges invokes
// queryForTuples, passing it the stack of relations it constructed on the way to that particular leaf.
//
// For each edge, it creates a new ReverseExpandRequest, preserving the context of the overall query
// but updating the traversal state (the 'stack') based on the edge being processed.
//
// The behavior is determined by the edge type:
//
//   - DirectEdge: This represents a direct path to data. Here we initiate a call to
//     `queryForTuples` to query the datastore for tuples that match the relationship path
//     accumulated in the stack. This is the end of the traversal.
//
//   - ComputedEdge, RewriteEdge, and TTUEdge: These represent indirections in the authorization model.
//     The function modifies the traversal 'stack' to reflect the next relationship that needs to be resolved.
//     It then calls `dispatch` to continue traversing the graph with this new state until it reaches a DirectEdge.
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

		// combination of From() and To() for usersets should fix the infinite looping issue
		if toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation {
			key := edge.GetFrom().GetUniqueLabel() + toNode.GetUniqueLabel()
			if _, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{}); loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle
				wt, _ := edge.GetWeight("user")
				fmt.Printf("JUSTIN WEIGHT INF: %t\n", wt == weightedGraph.Infinite)
				fmt.Println("ABORTING INFINITE LOOP")
				continue
			}
		}

		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			// TODO: maybe for direct edges which point to usersets we should be continuing the traversal
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
				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})
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
				r.stack.Push(typeRelEntry{typeRel: edge.GetTuplesetRelation()})
				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})
			}
			// TODO: I think we can determine whether it's recursive here, and if it is we should indicate that
			// on the stack for this branch somehow. Maybe for those cases we trigger two additional jobs if we find a result
			// one for the recursive relation and one with the stack modified to continue up the chain
			// It would just continually loop over the #parent or whatever relationship until it stopped finding tuples
			// All recursive nodes are basically terminal nodes, since there's no escape, so just query and emit all findings
			// until we run out. will require adjustment in the final query block
			// if we still have stack:
			//		if this is recursive:
			//			trySendCandidate anyway, and then retrigger with the same relation again forever, until we stop finding tuples
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
				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})
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

// queryForTuples performs all datastore-related reverse expansion logic. After a leaf node has been found in loopOverWeightedEdges,
// this function works backwards from a specified user (using the stack created in loopOverWeightedEdges)
// and an initial relationship edge to find all the objects that the given user has the given relationship with.
// The function defines a recursive inner function, `queryFunc`, which is executed concurrently for different
// branches of the relationship graph.
//
// On its initial execution, it constructs a database query filter based on the starting user and the "To"
// part of the initial DirectEdge, which can be a direct user, a wildcard user, or a userset.
// In subsequent recursive calls, it takes a `foundObject`—the object found in the previous step—and
// that foundObject becomes the 'user' in the next query in the stack. Take this model for example:
//
//		type user
//		type organization
//		  relations
//			define member: [user]
//			define repo_admin: [organization#member]
//		type repo
//		  relations
//	        define admin: repo_admin from owner
//	        define owner: [organization]
//
// When searching for repos which user:bob has #admin relation to, queryFunc behaves like so:
//
//  1. Search for organizations where user:bob is a member. We find this tuple: organization:fga#member@user:bob
//  2. Take that foundObject, `organization:fga` and pass it to the next call of queryFunc.
//  3. Query for tuples matching `organization#repo_admin@organization:fga#member` (because this is a userset relation).
//  4. If we found another object in step 3, pass that into the next queryFunc call to be evaluated against the next element in the stack.
//
// We continue doing this recursively until we hit one of the two cases below:
//
//  1. We cannot locate a tuple for a query—this means this branch of the tree yielded no results.
//  2. The stack is empty—this means there are no more queries to run, and this object is a candidate to be returned
//     to ListObjects through resultChan.
func (c *ReverseExpandQuery) queryForTuples(
	ctx context.Context,
	req *ReverseExpandRequest,
	needsCheck bool,
	resultChan chan<- *ReverseExpandResult,
) error {
	// TODO: don't forget telemetry
	var wg sync.WaitGroup
	errChan := make(chan error, 100) // TODO: random value here, needs tuning

	var queryFunc func(context.Context, *ReverseExpandRequest, string)

	queryFunc = func(qCtx context.Context, r *ReverseExpandRequest, foundObject string) {
		defer wg.Done()
		if qCtx.Err() != nil {
			return
		}

		var typeRel string
		var userFilter []*openfgav1.ObjectRelation

		// This is true on every call except the first
		if foundObject != "" {
			entry := r.stack.Pop()
			typeRel = entry.typeRel
			filter := &openfgav1.ObjectRelation{Object: foundObject}
			if entry.usersetRelation != "" {
				filter.Relation = entry.usersetRelation
			}
			userFilter = append(userFilter, filter)
		} else {
			// this else block ONLY hits on the first call
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
				typeRel = req.stack.Pop().typeRel
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: tuple.BuildObject(to.GetUniqueLabel(), userID)})

			case weightedGraph.SpecificTypeWildcard: // Wildcard Referece To() -> "user:*"
				typeRel = req.stack.Pop().typeRel
				userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: to.GetUniqueLabel()})

			case weightedGraph.SpecificTypeAndRelation: // Userset, To() -> "group#member"
				typeRel = to.GetUniqueLabel()

				// For a terminal userset edge, we need to attach an additional relation to the last
				// element in the stack.
				//
				// Using this model as an example:
				//	  type organization
				//		define repo_admin: [team#member]
				//
				// To resolve organization#repo_admin above for user:bob, we first have to see if user:bob is a member of any teams.
				// If we find this tuple: team:fga#member@user:bob, subsequent recursive calls of this method need to know
				// to append the #member relation when querying further. Without it, the query isn't meaningful, there cannot be
				// tuples of the form "organization#repo_admin@team:fga", that isn't valid in this model.
				// We must append #member to the 'user' in the subsequent query, so it becomes: "organization#repo_admin@team:fga#member".
				relation := tuple.GetRelation(typeRel)
				req.stack[len(req.stack)-1].usersetRelation = relation

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
				if errors.Is(err, storage.ErrIteratorDone) {
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
			if len(r.stack) == 0 {
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
