package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/pkg/telemetry"
	"go.opentelemetry.io/otel/trace"
	"sync"

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
}

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
	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

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

		// Going to a userset presents risk of infinite loop. Using from + to ensures
		// we don't traverse the exact same edge more than once.
		goingToUserset := toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation
		if goingToUserset {
			key := edge.GetFrom().GetUniqueLabel() + toNode.GetUniqueLabel()
			if _, loaded := c.visitedUsersetsMap.LoadOrStore(key, struct{}{}); loaded {
				// we've already visited this userset through this edge, exit to avoid an infinite cycle
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

				r.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})

				// Now continue traversing
				err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
				if err != nil {
					errs = errors.Join(errs, err)
					return errs
				}
				continue
			}

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

			// Push tupleset relation (`document#parent`)
			tuplesetRel := typeRelEntry{typeRel: edge.GetTuplesetRelation()}
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
	span := trace.SpanFromContext(ctx)

	// This map is used for memoization within this query path. It prevents re-running the exact
	// same database query for a given object type, relation, and user filter.
	jobDedupeMap := new(sync.Map)
	var queryFunc func(context.Context, *ReverseExpandRequest, string) error
	queryFunc = func(qCtx context.Context, r *ReverseExpandRequest, foundObject string) error {
		if qCtx.Err() != nil {
			return qCtx.Err()
		}

		// Ensure we're always working with a copy
		currentReq := r.clone()

		userFilter, typeRel, ok := c.buildFiltersAndDedupe(jobDedupeMap, currentReq, foundObject)
		if !ok {
			// this means we've run this exact query in this tree's path already
			return nil
		}

		objectType, relation := tuple.SplitObjectRelation(typeRel)

		filteredIter, err := c.buildFilteredIterator(ctx, req, objectType, relation, userFilter)
		if err != nil {
			return err
		}
		defer filteredIter.Stop()

		// TODO: this means EACH query could kick off resolveNodeBreadthLimit goroutines which defeats the purpose
		// Could manage the limit with select over a channel
		pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

		var errs error

	LoopOnIterator:
		for {
			tk, err := filteredIter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				errs = errors.Join(errs, err)
				break LoopOnIterator
			}

			condEvalResult, err := eval.EvaluateTupleCondition(ctx, tk, c.typesystem, req.Context)
			if err != nil {
				errs = errors.Join(errs, err)
				continue
			}

			if !condEvalResult.ConditionMet {
				if len(condEvalResult.MissingParameters) > 0 {
					errs = errors.Join(errs, condition.NewEvaluationError(
						tk.GetCondition().GetName(),
						fmt.Errorf("tuple '%s' is missing context parameters '%v'",
							tuple.TupleKeyToString(tk),
							condEvalResult.MissingParameters),
					))
				}

				continue
			}

			// This will be a "type:id" e.g. "document:roadmap"
			foundObject = tk.GetObject()

			// If there are no more type#rel to look for in the stack that means we have hit the base case
			// and this object is a candidate for return to the user.
			if len(currentReq.stack) == 0 {
				_ = c.trySendCandidate(ctx, needsCheck, foundObject, resultChan)
				continue
			}

			// For non-recursive relations (majority of cases), if there are more items on the stack, we continue
			// the evaluation one level higher up the tree with the `foundObject`.
			pool.Go(func(qCtx context.Context) error {
				return queryFunc(qCtx, currentReq, foundObject)
			})
		}

		errs = errors.Join(errs, pool.Wait())

		return errs
	}

	// Now kick off the recursive function defined above.
	err := queryFunc(ctx, req, "")

	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}
	return nil
}

func (c *ReverseExpandQuery) buildFiltersAndDedupe(
	dedupeMap *sync.Map,
	req *ReverseExpandRequest,
	object string,
) ([]*openfgav1.ObjectRelation, string, bool) {

	var typeRel string
	var userFilter []*openfgav1.ObjectRelation

	// This is true on every call except the first
	if object != "" {
		entry := req.stack.Pop()
		typeRel = entry.typeRel
		filter := &openfgav1.ObjectRelation{Object: object}
		if entry.usersetRelation != "" {
			filter.Relation = entry.usersetRelation
		}
		userFilter = append(userFilter, filter)
	} else {
		// This else block ONLY hits on the first call to queryFunc.
		var userID string
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

	// Create a unique key for the current query to avoid duplicate work.
	key := utils.Reduce(userFilter, "", func(accumulator string, current *openfgav1.ObjectRelation) string {
		return current.String() + accumulator
	})
	key += relation + objectType
	if _, loaded := dedupeMap.LoadOrStore(key, struct{}{}); loaded {
		// If this exact query has been run before in this path, abort.
		return nil, "", false
	}

	return userFilter, typeRel, true
}

// buildFilteredIterator constructs the iterator used when reverse_expand queries for tuples.
// The returned iterator MUST have .Stop() called on it.
func (c *ReverseExpandQuery) buildFilteredIterator(
	ctx context.Context,
	req *ReverseExpandRequest,
	objectType string,
	relation string,
	userFilter []*openfgav1.ObjectRelation,
) (storage.TupleKeyIterator, error) {
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
		return nil, err
	}

	// filter out invalid tuples yielded by the database iterator
	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(c.typesystem),
	)
	return filteredIter, nil
}
