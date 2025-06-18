package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type typeRelEntry struct {
	typeRel string // e.g. "organization#admin"

	// Only present for userset relations. Will be the userset relation string itself.
	// For `rel admin: [team#member]`, usersetRelation is "member"
	usersetRelation string

	isRecursive bool
}

// TODO: add these where appropriate, not here
var typeToCycleMap = make(map[string]relationStack)
var globalCyclesMap = new(sync.Map)

// relationStack represents the path of queryable relationships encountered on the way to a terminal type.
// As reverseExpand traverses from a requested type#rel to its leaf nodes, it pushes to this stack.
// Each entry is a `typeRelEntry` struct, which contains not only the `type#relation`
// but also crucial metadata:
//   - `usersetRelation`: To handle transitions through usersets (e.g. `[team#member]`).
//   - `isRecursive`: To correctly process recursive relationship definitions (e.g. `define member: [user] or group#member`).
//
// After reaching a leaf, this stack is consumed by the `queryForTuples` function to build the precise chain of
// database queries needed to find the resulting objects.
// To avoid races, every leaf node receives its own copy of the stack.
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

func (r *relationStack) Copy() relationStack {
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
	// pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

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

		toNode := edge.GetTo()

		fmt.Printf("%s --> %s\n", edge.GetFrom().GetUniqueLabel(), toNode.GetUniqueLabel())

		// Going to a userset presents risk of infinite loop. Using from + to ensures
		// we don't traverse the exact same edge more than once.
		goingToUserset := toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation
		if goingToUserset {
			key := edge.GetFrom().GetUniqueLabel() + toNode.GetUniqueLabel()
			if _, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{}); loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle

				var objType string
				if edge.GetTuplesetRelation() != "" {
					// if there is a tupleset relation, we should use it when we put this in the map
					objType, _ = tuple.SplitObjectRelation(edge.GetTuplesetRelation())
				} else {
					objType, _ = tuple.SplitObjectRelation(toNode.GetUniqueLabel())
				}

				fmt.Printf("JUSTIN HIT A CYCLE %s, %+v\n", key, r.stack)
				if cycle, alreadyExists := globalCyclesMap.Load(key); alreadyExists {
					typeToCycleMap[objType] = cycle.(relationStack)
				} else {
					stackForStorage := r.stack.Copy()
					stackForStorage.Pop() // TODO: explain why this is again
					globalCyclesMap.Store(key, stackForStorage)
					typeToCycleMap[objType] = stackForStorage
					fmt.Printf("STORING CYCLE: for objectType: %s, %+v\n", objType, stackForStorage)
				}

				continue
			}
		}

		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			if goingToUserset {
				// Attach the userset relation to the previous stack entry
				//  type team:
				//		define member: [user]
				//	type org:
				//		define teammate: [team#member]
				// A direct edge here is org#teammate --> team#member
				// so if we find team:fga for this user, we need to know to check for
				// team:fga#member when we check org#teammate
				r.stack[len(r.stack)-1].usersetRelation = tuple.GetRelation(toNode.GetUniqueLabel())

				// We also need to check if this userset is recursive
				// e.g. `define member: [user] or team#member`
				wt, _ := edge.GetWeight(req.User.GetObjectType())
				if wt == weightedGraph.Infinite {
					r.stack[len(r.stack)-1].isRecursive = true
				}

				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				fmt.Printf("Pushing userset to stack: %s\n", toNode.GetUniqueLabel())

				// Now continue traversing
				err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
				if err != nil {
					errs = errors.Join(errs, err)
					return errs
				}
				continue
			}

			//fmt.Printf("JUSTIN LEAF NODE: %s\n, stack: %+v\n", toNode.GetUniqueLabel(), r.stack)
			// We have reached a leaf node in the graph (e.g. `user` or `user:*`),
			// and the traversal for this path is complete. Now we use the stack of relations
			// we've built to query the datastore for matching tuples.
			pool.Go(func(ctx context.Context) error {
				return c.queryForTuples(
					ctx,
					r,
					needsCheck,
					resultChan,
				)
			})
		case weightedGraph.ComputedEdge:
			// A computed edge is an alias (e.g., `define viewer: editor`).
			// We replace the current relation on the stack (`viewer`) with the computed one (`editor`),
			// as tuples are only written against `editor`.
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
			// Replace the existing type#rel on the stack with the tuple-to-userset relation:
			//
			// 	type document
			//		define parent: [folder]
			//		define viewer: admin from parent
			//
			// We need to remove document#viewer from the stack and replace it with the tupleset relation (`document#parent`).
			// Then we have to add the .To() relation `folder#admin`.
			// The stack becomes `[document#parent, folder#admin]`, and on evaluation we will first
			// query for folder#admin, then if folders exist we will see if they are related to
			// any documents as #parent.
			_ = r.stack.Pop()

			tuplesetRel := typeRelEntry{typeRel: edge.GetTuplesetRelation()}
			weight, _ := edge.GetWeight(req.User.GetObjectType())
			if weight == weightedGraph.Infinite {
				tuplesetRel.isRecursive = true
			}
			// Push tupleset relation (`document#parent`)
			r.stack.Push(tuplesetRel)
			// Push target type#rel (`folder#admin`)
			r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})

			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.RewriteEdge:
			// Behaves just like ComputedEdge above
			// Operator nodes (union, intersection, exclusion) are not real types, they never get added
			// to the stack.
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

	// This map is used for memoization within this query path. It prevents re-running the exact
	// same database query for a given object type, relation, and user filter.
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

		// TODO: polish this bit
		// Create a unique key for the current query to avoid duplicate work.
		key := utils.Reduce(userFilter, "", func(accumulator string, current *openfgav1.ObjectRelation) string {
			return current.String() + accumulator
		})
		key += relation + objectType
		if _, loaded := jobDedupeMap.LoadOrStore(key, struct{}{}); loaded {
			// If this exact query has been run before in this path, abort.
			return
		}

		filteredIter, err := c.buildFilteredIterator(ctx, req.StoreID, objectType, relation, userFilter, req.Consistency)
		if err != nil {
			errChan <- err
			return
		}
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
			fmt.Printf("Justin queryForTuples:"+
				"\n\tObjectType: %s"+
				"\n\tRelation: %s"+
				"\n\tUser filter: %s"+
				"\n\tstack: %+v"+
				"\n----------------------\n"+
				"Found: %s"+
				"\n----------------------\n",
				objectType, relation, userFilter, r.stack, foundObject)

			// If there are no more type#rel to look for in the stack that means we have hit the base case
			// and this object is a candidate for return to the user.
			if len(r.stack) == 0 {
				_ = c.trySendCandidate(ctx, needsCheck, foundObject, resultChan)
				continue
			}

			// This is the logic for handling recursive relations, such as `define member: [user] or group#member`.
			// When we find a tuple related to a recursive relation, we need to explore two paths concurrently:
			// 1. Continue the recursion: The user in the found tuple might be a member of another group.
			// 2. Exit the recursion: The object of the found tuple might satisfy the next relation in the stack.
			if r.stack.Peek().isRecursive {
				// If the recursive relation is the last one in the stack, foundObject could be a final result.
				if len(r.stack) == 1 && tuple.GetType(tk.GetObject()) == req.ObjectType {
					_ = c.trySendCandidate(ctx, needsCheck, foundObject, resultChan)
				}

				// TODO: Testing cycle stuff
				//fmt.Printf("Recursive relation hit: %s\n", r.stack.Peek().typeRel)

				//if cycleStack, ok := typeToCycleMap[tuple.GetType(tk.GetObject())]; ok {
				if cycleStack, ok := typeToCycleMap[tuple.GetType(tk.GetObject())]; ok {
					fmt.Printf("--------------------------HIT----------------------"+
						"\n\tObject: %s"+
						"\n\tThe stack: %+v\n",
						tk.GetObject(),
						cycleStack,
					)

					// Kick off query loop over cycle
					c.queryCycle(ctx, req, foundObject, cycleStack, resultChan, errChan)
				}

				// Path 1: Continue the recursive search.
				// We call `queryFunc` again with the same request (`r`), which keeps the recursive
				// relation on the stack, to find further nested relationships.
				wg.Add(1)
				go queryFunc(qCtx, r, foundObject)

				//// Path 2: Exit the recursion and move up the hierarchy.
				//// This is only possible if there are other relations higher up in the stack.
				if len(r.stack) > 1 {
					// Create a new request, pop the recursive relation off its stack, and then
					// call `queryFunc`. This explores whether the `foundObject` satisfies the next
					// relationship in the original query chain.
					newReq := r.clone()
					_ = newReq.stack.Pop()
					wg.Add(1)
					go queryFunc(qCtx, newReq, foundObject)
				}
			} else {
				// For non-recursive relations (majority of cases), if there are more items on the stack, we continue
				// the evaluation one level higher up the tree with the `foundObject`.
				wg.Add(1)
				go queryFunc(qCtx, r.clone(), foundObject)
			}
		}
	}

	// Now kick off the recursive function defined above.
	wg.Add(1)
	go queryFunc(ctx, req, "")

	wg.Wait()
	//fmt.Printf("JUstin ran %d jobs\n", numJobs.Load())
	close(errChan)
	var errs error
	for err := range errChan {
		errs = errors.Join(errs, err)
	}
	return errs
}

// TODO: explain this better if it actually works out
func (c *ReverseExpandQuery) queryCycle(
	ctx context.Context,
	req *ReverseExpandRequest,
	startObject string,
	relationCycle relationStack,
	resultChan chan<- *ReverseExpandResult,
	errChan chan<- error,
) {
	fmt.Printf("JUSTIN IN THE CYCLE FUNC \n\tstartObject: %s \n\t%+v\n", startObject, relationCycle)
	var wg sync.WaitGroup
	// The Map: map[
	//	org:[
	//		{typeRel:org#org_to_team usersetRelation: isRecursive:true}
	//		{typeRel:team#team_to_company usersetRelation: isRecursive:true}
	//		{typeRel:company#company_to_org usersetRelation: isRecursive:true} // start here and walk back
	//	]]
	//entry := r.stack.Peek()

	// We're reading from the stack in a loop, but we don't want to actually .Pop() because we could do this
	// loop many times and we can't throw away elements

	jobDedupeMap := new(sync.Map)
	var queryFunc func(context.Context, *ReverseExpandRequest, string, relationStack, int)
	//numJobs := atomic.Int32{} // TODO: remove, this is for debugging
	queryFunc = func(qCtx context.Context, r *ReverseExpandRequest, object string, relationCycle relationStack, position int) {
		defer wg.Done()

		// If we have already run through the full cycle of tuples in the stack we need to reset to
		// The beginning of the cycle again
		if position < 0 {
			position = len(relationCycle) - 1
		}
		stackEntry := relationCycle[position]
		typeRel := stackEntry.typeRel

		filter := &openfgav1.ObjectRelation{Object: object}
		if stackEntry.usersetRelation != "" {
			filter.Relation = stackEntry.usersetRelation
		}
		userFilter := []*openfgav1.ObjectRelation{filter}
		objectType, relation := tuple.SplitObjectRelation(typeRel)

		// TODO: this is direct copy pasted from the other query func in this file
		// Create a unique key for the current query to avoid duplicate work.
		key := utils.Reduce(userFilter, "", func(accumulator string, current *openfgav1.ObjectRelation) string {
			return current.String() + accumulator
		})
		key += relation + objectType
		if _, loaded := jobDedupeMap.LoadOrStore(key, struct{}{}); loaded {
			// If this exact query has been run before in this path, abort.
			return
		}

		filteredIter, err := c.buildFilteredIterator(ctx, req.StoreID, objectType, relation, userFilter, req.Consistency)
		if err != nil {
			// idk do seomhting
			panic(err)
		}
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

			foundObject := tk.GetObject() // This will be a "type:id" e.g. "document:roadmap"
			fmt.Printf("JUSTIN CYCLE LOGIC FOUND AN OBJECT: %s\n", foundObject)
			if tuple.GetType(foundObject) == req.ObjectType {
				fmt.Printf("JUSTIN CYCLE LOGIC FOUND CANDIDATE: %s\n", foundObject)
				_ = c.trySendCandidate(ctx, false, foundObject, resultChan)
			}

			wg.Add(1)
			go queryFunc(ctx, req, foundObject, relationCycle, position-1)
		}

	}
	// First query for this object with the last relation in thes tack
	wg.Add(1)
	go queryFunc(ctx, req, startObject, relationCycle, len(relationCycle)-1)
	wg.Wait()
}

// TODO: this MUST have stop called on it externally
func (c *ReverseExpandQuery) buildFilteredIterator(
	ctx context.Context,
	storeID string,
	objectType string,
	relation string,
	userFilter []*openfgav1.ObjectRelation,
	consistency openfgav1.ConsistencyPreference,
) (storage.TupleKeyIterator, error) {
	iter, err := c.datastore.ReadStartingWithUser(ctx, storeID, storage.ReadStartingWithUserFilter{
		ObjectType: objectType,
		Relation:   relation,
		UserFilter: userFilter,
	}, storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: consistency,
		},
	})
	if err != nil {
		return nil, err
	}
	//
	//fmt.Printf("Justin the filters run:"+
	//	"\n\tobjectType: %s,"+
	//	"\n\trelation: %s,"+
	//	"\n\tuserFilter: %s\n",
	//	objectType,
	//	relation,
	//	userFilter,
	//)

	// filter out invalid tuples yielded by the database iterator
	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(c.typesystem),
	)

	return filteredIter, nil
}
