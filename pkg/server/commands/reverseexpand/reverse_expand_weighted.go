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
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"sync"
	"sync/atomic"
)

type typeRelEntry struct {
	typeRel string // e.g. "organization#admin"

	// Only present for userset relations. Useful for cases like "rel admin: [user, team#member]"
	// where we need to know to check for "team:fga#member"
	usersetRelation string

	isRecursive bool
}

// TODO: update this comment
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

func (r *relationStack) Peek() typeRelEntry {
	element := (*r)[len(*r)-1]
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
	pool := concurrency.NewPool(ctx, 1) // TODO: this is not a real value

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
		//fmt.Printf("Justin Edge: %s --> %s Stack: %+v\n",
		//	edge.GetFrom().GetUniqueLabel(),
		//	edge.GetTo().GetUniqueLabel(),
		//	r.stack.Copy(),
		//)
		toNode := edge.GetTo()

		// combination of From() and To() for usersets should fix the infinite looping issue
		if toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation {
			key := edge.GetFrom().GetUniqueLabel() + toNode.GetUniqueLabel()
			if _, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{}); loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle
				continue
			}
		}

		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			//fmt.Printf("Justin Direct Edge: \n\t%s\n\t%s\n\t%+v\n",
			//	edge.GetFrom().GetUniqueLabel(),
			//	toNode.GetUniqueLabel(),
			//	r.stack.Copy(),
			//)
			// TODO: it might be that you need to apply the userset relation to the PREVIOUS entry in the stack
			// check the logic with the simple TTU test
			// before popping from the stack, you can Peek() to see if the topmost thingy has a userset relation
			// if it does, you can run the query against that thing without popping, and then any results can be run
			// as a userset query With Pop()ing to go up the chain
			// once you hit the terminal type for real "user", "user:*"
			if toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation {
				// Attach the userset relation to the prior stack entry
				//  type team:
				//		define member: [user]
				//	type org:
				//		define teammate: [team#member]
				// A direct edge here is org#teammate --> team#member
				// so if we find team:fga for this user, we need to know to check for
				// team:fga#member when we check the org
				r.stack[len(r.stack)-1].usersetRelation = tuple.GetRelation(toNode.GetUniqueLabel())

				// TODO: will this loop infinitely?
				// We also need to check if this userset is recursive
				wt, _ := edge.GetWeight(req.User.GetObjectType())
				if wt == weightedGraph.Infinite { // this means this is a recursive userset
					r.stack[len(r.stack)-1].isRecursive = true
				}

				// And then push the new entry, e.g. team#member
				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})

				// Now continue traversing
				err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
				if err != nil {
					errs = errors.Join(errs, err)
					return errs
				}
				continue
			}
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
			//fmt.Printf("Justin TTU Edge before: \n\t%s\n\t%s\n\t%+v\n",
			//	edge.GetFrom().GetUniqueLabel(),
			//	toNode.GetUniqueLabel(),
			//	r.stack.Copy(),
			//)
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				// Replace the existing type#rel on the stack with the TTU relation
				_ = r.stack.Pop()

				tuplesetRel := typeRelEntry{typeRel: edge.GetTuplesetRelation()}
				weight, _ := edge.GetWeight("user") // TODO: Make this more legit
				if weight == weightedGraph.Infinite {
					tuplesetRel.isRecursive = true
				}
				r.stack.Push(tuplesetRel)
				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})
			}

			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.RewriteEdge:
			// bc operator nodes are not real types
			//fmt.Printf("Justin Rewrite: %s --> %s Stack: %+v\n",
			//	edge.GetFrom().GetUniqueLabel(),
			//	edge.GetTo().GetUniqueLabel(),
			//	r.stack.Copy(),
			//)
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
	errChan := make(chan error, 100) // TODO: random value here, gotta do this another way

	// This map has to live in this scope, as each leaf kicks off its own queries
	// TODO: figure out why this causes 1 single recursive TTU matrix test to fail
	jobDedupeMap := new(sync.Map)
	var queryFunc func(context.Context, *ReverseExpandRequest, string)
	numJobs := atomic.Int32{} // TODO: remove, this is for debugging
	queryFunc = func(qCtx context.Context, r *ReverseExpandRequest, foundObject string) {
		defer wg.Done()
		if qCtx.Err() != nil {
			return
		}
		numJobs.Add(1)

		var typeRel string
		var userFilter []*openfgav1.ObjectRelation

		// This is true on every call except the first
		if foundObject != "" {
			entry := r.stack.Peek()
			// For recursive relations, don't actually pop the relation off the stack.
			if !entry.isRecursive {
				// If it is *not* recursive (most cases), remove the last element
				r.stack.Pop()
			}
			typeRel = entry.typeRel
			filter := &openfgav1.ObjectRelation{Object: foundObject}
			if entry.usersetRelation != "" {
				filter.Relation = entry.usersetRelation
			}
			userFilter = append(userFilter, filter)
		} else {
			// this else block ONLY hits on the first call
			var userID string
			// TODO: we might be able to handle pure userset queries now
			// We will always have a UserRefObject here. Queries that come in for pure usersets do not take this code path.
			// e.g. ListObjects(team:fga#member, document, viewer) will not make it here.
			if val, ok := req.User.(*UserRefObject); ok {
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
			}
		}

		objectType, relation := tuple.SplitObjectRelation(typeRel)
		fmt.Printf("JUSTIN querying: \n\tUserfilter: %s, relation: %s, objectType: %s\n",
			userFilter, relation, objectType,
		)

		// TODO: polish this bit
		// so we can bail out of duplicate work earlier
		key := utils.Reduce(userFilter, "", func(accumulator string, current *openfgav1.ObjectRelation) string {
			return current.String() + accumulator
		})
		key += relation + objectType
		if _, loaded := jobDedupeMap.LoadOrStore(key, struct{}{}); loaded {
			fmt.Printf("KEY was already queried: %s, aborting\n", key)
			//fmt.Println("This query was already run, bailing")
			return
		}

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

			foundObject = tk.GetObject() // This will be a "type:id" e.g. "document:roadmap"

			// If there are no more type#rel to look for in the stack that means we have hit the base case
			// and this object is a candidate for return to the user.
			if len(r.stack) == 0 {
				_ = c.trySendCandidate(ctx, needsCheck, foundObject, resultChan)
				continue
			}

			// TODO: explain what's happening here in great detail
			if r.stack.Peek().isRecursive {
				// If recursive, check the tuple against the original query
				// then dispatch two additional queries, one hitting the same recursive relation
				// and one popping it off and going for the next one

				// This can happen if the query asked explicitly for a recursive relation
				//if typeRel == tuple.ToObjectRelationString(req.ObjectType, req.Relation) {
				if len(r.stack) == 1 {
					_ = c.trySendCandidate(ctx, needsCheck, foundObject, resultChan)
				}
				//}

				// For recursive, we need to kick off two additional queries
				// One for exactly the same relation (recursion), and one for the next relation in the stack
				// In case the foundObject has a relation higher up the tree
				wg.Add(1)
				go queryFunc(qCtx, r, foundObject)

				// len(stack) must be greater than 1 in case the *only* relation left
				// in the stack is the recursive relation
				if len(r.stack) > 1 {
					// Now remove the recursive relation and send this job again
					newReq := r.clone()
					_ = newReq.stack.Pop()
					wg.Add(1)
					go queryFunc(qCtx, newReq, foundObject)
				}
			}

			// if there are more relations in the stack, we need to evaluate the object found here against
			// the next type#rel one level higher in the tree.
			wg.Add(1)
			go queryFunc(qCtx, r.clone(), foundObject)
		}
	}

	// Now kick off the querying without an explicit object override for the first call
	wg.Add(1)
	go queryFunc(ctx, req, "")

	wg.Wait()
	fmt.Printf("JUstin ran %d jobs\n", numJobs.Load())
	close(errChan)
	var errs error
	for err := range errChan {
		errs = errors.Join(errs, err)
	}
	return errs
}
