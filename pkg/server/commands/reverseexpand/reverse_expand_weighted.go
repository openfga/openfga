package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"github.com/openfga/openfga/pkg/telemetry"
	"go.opentelemetry.io/otel/trace"
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
}

// relationStack represents the path of queryable relationships encountered on the way to a terminal type.
// As reverseExpand traverses from a requested type#rel to its leaf nodes, it pushes to this stack.
// Each entry is a `typeRelEntry` struct, which contains the `type#relation`
// along with metadata:
//   - `usersetRelation`: To handle transitions through usersets (e.g. `[team#member]`).
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

// queryJob represents a single task in the reverse expansion process.
// It holds the `foundObject` from a previous step in the traversal
// and the `ReverseExpandRequest` containing the current state of the request.
type queryJob struct {
	foundObject string
	req         *ReverseExpandRequest
}

// jobQueue is a thread-safe queue for managing `queryJob` instances.
// It's used to hold jobs that need to be processed during the recursive
// `queryForTuples` operation, allowing concurrent processing of branches
// in the authorization graph.
type jobQueue struct {
	items []queryJob
	mu    sync.Mutex
}

func newJobQueue() *jobQueue {
	return &jobQueue{
		items: []queryJob{},
	}
}

func (q *jobQueue) hasItems() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) > 0
}

func (q *jobQueue) enqueue(value ...queryJob) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, value...)
}

func (q *jobQueue) dequeue() (queryJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return queryJob{}, false
	}

	item := q.items[0]
	q.items = q.items[1:]
	return item, true
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
		newReq := req.clone()
		newReq.weightedEdge = edge
		newReq.weightedEdgeTypeRel = edge.GetTo().GetUniqueLabel()

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
				newReq.stack[len(newReq.stack)-1].usersetRelation = tuple.GetRelation(toNode.GetUniqueLabel())

				newReq.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})

				// Now continue traversing
				err := c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
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
					newReq,
					needsCheck,
					resultChan,
				)
			})
		case weightedGraph.ComputedEdge:
			// A computed edge is an alias (e.g., `define viewer: editor`).
			// We replace the current relation on the stack (`viewer`) with the computed one (`editor`),
			// as tuples are only written against `editor`.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				_ = newReq.stack.Pop()
				newReq.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})
			}

			err := c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
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
			_ = newReq.stack.Pop()

			// Push tupleset relation (`document#parent`)
			tuplesetRel := typeRelEntry{typeRel: edge.GetTuplesetRelation()}
			newReq.stack.Push(tuplesetRel)

			// Push target type#rel (`folder#admin`)
			newReq.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})

			err := c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.RewriteEdge:
			// Behaves just like ComputedEdge above
			// Operator nodes (union, intersection, exclusion) are not real types, they never get added
			// to the stack.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				_ = newReq.stack.Pop()
				newReq.stack.Push(typeRelEntry{typeRel: toNode.GetUniqueLabel()})
			}
			err := c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
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

	// This map is used for memoization of database queries for this branch of the reverse expansion.
	// It prevents re-running the exact same database query for a given object type, relation, and user filter.
	jobDedupeMap := new(sync.Map)
	queryJobQueue := newJobQueue()

	var queryFunc func(ctx context.Context, job queryJob) ([]queryJob, error)
	queryFunc = func(ctx context.Context, job queryJob) ([]queryJob, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Ensure we're always working with a copy
		currentReq := job.req.clone()

		userFilter := buildUserFilter(currentReq, job.foundObject)

		// Now pop the top relation off of the stack for querying
		typeRel := currentReq.stack.Pop().typeRel

		// Ensure that we haven't already run this query
		ok := checkQueryIsUnique(jobDedupeMap, userFilter, typeRel)
		if !ok {
			// this means we've run this exact query in this branch's path already
			return nil, nil
		}

		objectType, relation := tuple.SplitObjectRelation(typeRel)

		filteredIter, err := c.buildFilteredIterator(ctx, req, objectType, relation, userFilter)
		if err != nil {
			return nil, err
		}
		defer filteredIter.Stop()

		var errs error
		var nextJobs []queryJob

	LoopOnIterator:
		for {
			tupleKey, err := filteredIter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				errs = errors.Join(errs, err)
				break LoopOnIterator
			}

			condEvalResult, err := eval.EvaluateTupleCondition(ctx, tupleKey, c.typesystem, req.Context)
			if err != nil {
				errs = errors.Join(errs, err)
				continue
			}

			if !condEvalResult.ConditionMet {
				if len(condEvalResult.MissingParameters) > 0 {
					errs = errors.Join(errs, condition.NewEvaluationError(
						tupleKey.GetCondition().GetName(),
						fmt.Errorf("tuple '%s' is missing context parameters '%v'",
							tuple.TupleKeyToString(tupleKey),
							condEvalResult.MissingParameters),
					))
				}

				continue
			}

			// This will be a "type:id" e.g. "document:roadmap"
			foundObject := tupleKey.GetObject()

			// If there are no more type#rel to look for in the stack that means we have hit the base case
			// and this object is a candidate for return to the user.
			if len(currentReq.stack) == 0 {
				_ = c.trySendCandidate(ctx, needsCheck, foundObject, resultChan)
				continue
			}

			// For non-recursive relations (majority of cases), if there are more items on the stack, we continue
			// the evaluation one level higher up the tree with the `foundObject`.
			nextJobs = append(nextJobs, queryJob{foundObject: foundObject, req: currentReq})
		}

		return nextJobs, errs
	}

	// Now kick off the recursive function defined above.
	items, err := queryFunc(ctx, queryJob{req: req, foundObject: ""})
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}

	// Populate the jobQueue with the initial jobs
	queryJobQueue.enqueue(items...)

	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))
	activeJobs := atomic.Int64{}

	initial := true // Needed to enter the first iteration of the for loop below

	// This loop processes jobs from the queue concurrently.
	// It continues as long as there are items in the queue OR there are active goroutines processing jobs.
	// The `initial` flag ensures the loop runs at least once to kick off the first jobs.
	for initial == true || activeJobs.Load() > 0 {
		initial = false // set to false and rely on our activeJobs count

		for queryJobQueue.hasItems() {
			job, ok := queryJobQueue.dequeue()
			if !ok {
				// This shouldn't be possible if hasItems() just succeeded
				break
			}
			activeJobs.Add(1)

			pool.Go(func(ctx context.Context) error {
				defer activeJobs.Add(-1)
				newItems, err := queryFunc(ctx, job)
				if err != nil {
					return err
				}

				// Each job can spawn many new jobs
				queryJobQueue.enqueue(newItems...)
				return nil
			})
		}
	}

	err = pool.Wait()
	if err != nil {
		return err
	}

	return nil
}

func buildUserFilter(
	req *ReverseExpandRequest,
	object string,
) []*openfgav1.ObjectRelation {
	var userFilter []*openfgav1.ObjectRelation

	// This is true on every call to queryFunc except the first, since we only trigger subsequent
	// calls if we successfully found an object.
	if object != "" {
		entry := req.stack.Peek()
		filter := &openfgav1.ObjectRelation{Object: object}
		if entry.usersetRelation != "" {
			filter.Relation = entry.usersetRelation
		}
		userFilter = append(userFilter, filter)
	} else {
		// This else block ONLY hits on the first call to queryFunc.
		toNode := req.weightedEdge.GetTo()

		switch toNode.GetNodeType() {
		case weightedGraph.SpecificType: // Direct User Reference. To() -> "user"
			// We will always have a UserRefObject here. Queries that come in for pure usersets do not take this code path.
			// e.g. ListObjects(team:fga#member, document, viewer) will not make it here.
			var userID string
			if val, ok := req.User.(*UserRefObject); ok {
				userID = val.Object.GetId()
			}
			userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: tuple.BuildObject(toNode.GetUniqueLabel(), userID)})

		case weightedGraph.SpecificTypeWildcard: // Wildcard Referece To() -> "user:*"
			userFilter = append(userFilter, &openfgav1.ObjectRelation{Object: toNode.GetUniqueLabel()})
		}
	}

	return userFilter
}

func checkQueryIsUnique(
	dedupeMap *sync.Map,
	userFilter []*openfgav1.ObjectRelation,
	typeRel string,
) bool {
	objectType, relation := tuple.SplitObjectRelation(typeRel)

	// Create a unique key for the current query to avoid duplicate work.
	key := utils.Reduce(userFilter, "", func(accumulator string, current *openfgav1.ObjectRelation) string {
		return current.String() + accumulator
	})

	key += relation + objectType
	if _, loaded := dedupeMap.LoadOrStore(key, struct{}{}); loaded {
		// This means this query has been run on this branch of the graph already, don't do it again.
		return false
	}

	return true
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
