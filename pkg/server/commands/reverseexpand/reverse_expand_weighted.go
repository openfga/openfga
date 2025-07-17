package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"sync"

	aq "github.com/emirpasic/gods/queues/arrayqueue"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/stack"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const (
	listObjectsResultChannelLength = 100
)

var ErrEmptyStack = errors.New("unexpected empty stack")

// typeRelEntry represents a step in the path taken to reach a leaf node.
// As reverseExpand traverses from a requested type#rel to its leaf nodes, it stack.Pushes typeRelEntry structs to a stack.
// After reaching a leaf, this stack is consumed by the `queryForTuples` function to build the precise chain of
// database queries needed to find the resulting objects.
type typeRelEntry struct {
	typeRel string // e.g. "organization#admin"

	// Only present for userset relations. Will be the userset relation string itself.
	// For `rel admin: [team#member]`, usersetRelation is "member"
	usersetRelation string
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
	queue aq.Queue
	mu    sync.Mutex
}

func newJobQueue() *jobQueue {
	return &jobQueue{queue: *aq.New()}
}

func (q *jobQueue) Empty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Empty()
}

func (q *jobQueue) enqueue(value ...queryJob) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, item := range value {
		q.queue.Enqueue(item)
	}
}

func (q *jobQueue) dequeue() (queryJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	val, ok := q.queue.Dequeue()
	if !ok {
		return queryJob{}, false
	}
	job, ok := val.(queryJob)
	if !ok {
		return queryJob{}, false
	}

	return job, true
}

// loopOverEdges iterates over a set of weightedGraphEdges and acts as a dispatcher,
// processing each edge according to its type to continue the reverse expansion process.
//
// While traversing, loopOverEdges appends relation entries to a stack for use in querying after traversal is complete.
// It will continue to dispatch and traverse the graph until it reaches a DirectEdge, which
// leads to a leaf node in the authorization graph. Once a DirectEdge is found, loopOverEdges invokes
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
func (c *ReverseExpandQuery) loopOverEdges(
	ctx context.Context,
	req *ReverseExpandRequest,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	needsCheck bool,
	resolutionMetadata *ResolutionMetadata,
	resultChan chan<- *ReverseExpandResult,
	sourceUserType string,
) error {
	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	for _, edge := range edges {
		newReq := req.clone()
		newReq.weightedEdge = edge

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
				if newReq.relationStack == nil {
					return ErrEmptyStack
				}
				entry, newStack := stack.Pop(newReq.relationStack)
				entry.usersetRelation = tuple.GetRelation(toNode.GetUniqueLabel())

				newStack = stack.Push(newStack, entry)
				newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				newReq.relationStack = newStack

				// Now continue traversing
				pool.Go(func(ctx context.Context) error {
					return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
				})
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
					"",
				)
			})
		case weightedGraph.ComputedEdge:
			// A computed edge is an alias (e.g., `define viewer: editor`).
			// We replace the current relation on the stack (`viewer`) with the computed one (`editor`),
			// as tuples are only written against `editor`.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				if newReq.relationStack == nil {
					return ErrEmptyStack
				}
				_, newStack := stack.Pop(newReq.relationStack)
				newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				newReq.relationStack = newStack
			}

			pool.Go(func(ctx context.Context) error {
				return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
			})
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
			if newReq.relationStack == nil {
				return ErrEmptyStack
			}
			_, newStack := stack.Pop(newReq.relationStack)

			// stack.Push tupleset relation (`document#parent`)
			tuplesetRel := typeRelEntry{typeRel: edge.GetTuplesetRelation()}
			newStack = stack.Push(newStack, tuplesetRel)

			// stack.Push target type#rel (`folder#admin`)
			newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
			newReq.relationStack = newStack

			pool.Go(func(ctx context.Context) error {
				return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.RewriteEdge:
			// Behaves just like ComputedEdge above
			// Operator nodes (union, intersection, exclusion) are not real types, they never get added
			// to the stack.
			if toNode.GetNodeType() != weightedGraph.OperatorNode {
				if newReq.relationStack == nil {
					return ErrEmptyStack
				}
				_, newStack := stack.Pop(newReq.relationStack)
				newStack = stack.Push(newStack, typeRelEntry{typeRel: toNode.GetUniqueLabel()})
				newReq.relationStack = newStack

				pool.Go(func(ctx context.Context) error {
					return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
				})
				// continue to the next edge
				break
			}

			// If the edge is an operator node, we need to handle it differently.
			switch toNode.GetLabel() {
			case weightedGraph.IntersectionOperator:
				intersectionEdges, err := c.typesystem.GetEdgesFromNode(toNode, sourceUserType)
				if err != nil {
					return err
				}
				err = c.intersectionHandler(pool, newReq, resultChan, intersectionEdges, sourceUserType, resolutionMetadata)
				if err != nil {
					return err
				}
			case weightedGraph.ExclusionOperator:
				exclusionEdges, err := c.typesystem.GetEdgesFromNode(toNode, sourceUserType)
				if err != nil {
					return err
				}
				err = c.exclusionHandler(ctx, pool, newReq, resultChan, exclusionEdges, sourceUserType, resolutionMetadata)
				if err != nil {
					return err
				}
			case weightedGraph.UnionOperator:
				pool.Go(func(ctx context.Context) error {
					return c.dispatch(ctx, newReq, resultChan, needsCheck, resolutionMetadata)
				})
			default:
				return fmt.Errorf("unsupported operator node: %s", toNode.GetLabel())
			}
		default:
			return fmt.Errorf("unsupported edge type: %v", edge.GetEdgeType())
		}
	}

	return pool.Wait()
}

// queryForTuples performs all datastore-related reverse expansion logic. After a leaf node has been found in loopOverEdges,
// this function works backwards from a specified user (using the stack created in loopOverEdges)
// and an initial relationship edge to find all the objects that the given user has the given relationship with.
//
// This function orchestrates the concurrent execution of individual query jobs. It initializes a memoization
// map (`jobDedupeMap`) to prevent redundant database queries and a job queue to manage pending tasks.
// It kicks off the initial query and then continuously processes jobs from the queue using a concurrency pool
// until all branches leading up from the leaf have been explored.
func (c *ReverseExpandQuery) queryForTuples(
	ctx context.Context,
	req *ReverseExpandRequest,
	needsCheck bool,
	resultChan chan<- *ReverseExpandResult,
	foundObject string,
) error {
	span := trace.SpanFromContext(ctx)

	// This map is used for memoization of database queries for this branch of the reverse expansion.
	// It prevents re-running the exact same database query for a given object type, relation, and user filter.
	jobDedupeMap := new(sync.Map)
	queryJobQueue := newJobQueue()

	// Now kick off the chain of queries
	items, err := c.executeQueryJob(ctx, queryJob{req: req, foundObject: foundObject}, resultChan, needsCheck, jobDedupeMap)
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}

	// stack.Populate the jobQueue with the initial jobs
	queryJobQueue.enqueue(items...)

	// We could potentially have c.resolveNodeBreadthLimit active routines reaching this point.
	// Limit querying routines to 2 to avoid explosion of routines.
	pool := concurrency.NewPool(ctx, 2)

	for !queryJobQueue.Empty() {
		job, ok := queryJobQueue.dequeue()
		if !ok {
			// this shouldn't be possible
			return nil
		}

		// Each goroutine will take its first job from the original queue above
		// and then continue generating and processing jobs until there are no more.
		pool.Go(func(ctx context.Context) error {
			localQueue := newJobQueue()
			localQueue.enqueue(job)

			// While this goroutine's queue has items, keep looking for more
			for !localQueue.Empty() {
				nextJob, ok := localQueue.dequeue()
				if !ok {
					break
				}
				newItems, err := c.executeQueryJob(ctx, nextJob, resultChan, needsCheck, jobDedupeMap)
				if err != nil {
					return err
				}
				localQueue.enqueue(newItems...)
			}

			return nil
		})
	}

	err = pool.Wait()
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}

	return nil
}

// executeQueryJob represents a single recursive step in the reverse expansion query process.
// It takes a `queryJob`, which encapsulates the current state of the traversal (found object,
// and the reverse expand request with its relation stack).
// The method constructs a database query based on the current relation at the top of the stack
// and the `foundObject` from the previous step. It queries the datastore, and for each result:
//   - If the relation stack is empty, it means a candidate object has been found, which is then sent to `resultChan`.
//   - If matching tuples are found, it prepares new `queryJob` instances to continue the traversal further up the graph,
//     using the newly found object as the `foundObject` for the next step.
//   - If no matching objects are found in the datastore, this branch of reverse expand is a dead end, and no more jobs are needed.
func (c *ReverseExpandQuery) executeQueryJob(
	ctx context.Context,
	job queryJob,
	resultChan chan<- *ReverseExpandResult,
	needsCheck bool,
	jobDedupeMap *sync.Map,
) ([]queryJob, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Ensure we're always working with a copy
	currentReq := job.req.clone()

	userFilter, err := buildUserFilter(currentReq, job.foundObject)
	if err != nil {
		return nil, err
	}

	if currentReq.relationStack == nil {
		return nil, ErrEmptyStack
	}

	// Now pop the top relation off of the stack for querying
	entry, newStack := stack.Pop(currentReq.relationStack)
	typeRel := entry.typeRel

	currentReq.relationStack = newStack

	// Ensure that we haven't already run this query
	if isDuplicateQuery(jobDedupeMap, userFilter, typeRel) {
		return nil, nil
	}

	objectType, relation := tuple.SplitObjectRelation(typeRel)

	filteredIter, err := c.buildFilteredIterator(ctx, currentReq, objectType, relation, userFilter)
	if err != nil {
		return nil, err
	}
	defer filteredIter.Stop()

	var nextJobs []queryJob

	for {
		tupleKey, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			return nil, err
		}

		// This will be a "type:id" e.g. "document:roadmap"
		foundObject := tupleKey.GetObject()

		// If there are no more type#rel to look for in the stack that means we have hit the base case
		// and this object is a candidate for return to the user.
		if currentReq.relationStack == nil {
			c.trySendCandidate(ctx, needsCheck, foundObject, resultChan)
			continue
		}

		// For non-recursive relations (majority of cases), if there are more items on the stack, we continue
		// the evaluation one level higher up the tree with the `foundObject`.
		nextJobs = append(nextJobs, queryJob{foundObject: foundObject, req: currentReq})
	}

	return nextJobs, err
}

func buildUserFilter(
	req *ReverseExpandRequest,
	object string,
) ([]*openfgav1.ObjectRelation, error) {
	var filter *openfgav1.ObjectRelation
	// This is true on every call to queryFunc except the first, since we only trigger subsequent
	// calls if we successfully found an object.
	if object != "" {
		if req.relationStack == nil {
			return nil, ErrEmptyStack
		}

		entry := stack.Peek(req.relationStack)
		filter = &openfgav1.ObjectRelation{Object: object}
		if entry.usersetRelation != "" {
			filter.Relation = entry.usersetRelation
		}
	} else {
		// This else block ONLY hits on the first call to queryFunc.
		toNode := req.weightedEdge.GetTo()

		switch toNode.GetNodeType() {
		case weightedGraph.SpecificType: // Direct User Reference. To() -> "user"
			// req.User will always be either a UserRefObject or UserRefTypedWildcard here. Queries that come in for
			// pure usersets do not take this code path. e.g. ListObjects(team:fga#member, document, viewer) will not make it here.
			var userID string
			val, ok := req.User.(*UserRefObject)
			if ok {
				userID = val.Object.GetId()
			} else {
				// It might be a wildcard user, which is ok
				_, ok = req.User.(*UserRefTypedWildcard)
				if !ok {
					return nil, fmt.Errorf("unexpected user type when building User filter: %T", val)
				}
				return []*openfgav1.ObjectRelation{}, nil
			}

			filter = &openfgav1.ObjectRelation{Object: tuple.BuildObject(toNode.GetUniqueLabel(), userID)}

		case weightedGraph.SpecificTypeWildcard: // Wildcard Referece To() -> "user:*"
			filter = &openfgav1.ObjectRelation{Object: toNode.GetUniqueLabel()}
		}
	}

	return []*openfgav1.ObjectRelation{filter}, nil
}

func isDuplicateQuery(
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
	_, loaded := dedupeMap.LoadOrStore(key, struct{}{})

	return loaded
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
	return storage.NewConditionsFilteredTupleKeyIterator(
		storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(iter),
			validation.FilterInvalidTuples(c.typesystem),
		),
		checkutil.BuildTupleKeyConditionFilter(ctx, req.Context, c.typesystem),
	), nil
}

// findCandidatesForLowestWeightEdge finds the candidate objects for the lowest weight edge for intersection or exclusion.
func (c *ReverseExpandQuery) findCandidatesForLowestWeightEdge(
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	tmpResultChan chan<- *ReverseExpandResult,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) {
	// We need to create a new stack with the top item from the original request's stack
	// and use it to get the candidates for the lowest weight edge.
	// If the edge is a tuple to userset edge, we need to later check the candidates against the
	// original relationStack with the top item removed.
	var topItemStack stack.Stack[typeRelEntry]
	if req.relationStack != nil {
		topItem, newStack := stack.Pop(req.relationStack)
		req.relationStack = newStack
		topItemStack = stack.Push(nil, topItem)
	}

	// getting list object candidates from the lowest weight edge and have its result
	// pass through tmpResultChan.
	pool.Go(func(ctx context.Context) error {
		defer close(tmpResultChan)
		// stack with only the top item in it
		newReq := req.clone()
		newReq.relationStack = topItemStack
		err := c.shallowClone().loopOverEdges(
			ctx,
			newReq,
			edges,
			false,
			resolutionMetadata,
			tmpResultChan,
			sourceUserType,
		)
		return err
	})
}

// callCheckForCandidates calls check on the list objects candidates against non lowest weight edges.
func (c *ReverseExpandQuery) callCheckForCandidates(
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	tmpResultChan <-chan *ReverseExpandResult,
	resultChan chan<- *ReverseExpandResult,
	userset *openfgav1.Userset,
	isAllowed bool,
) {
	pool.Go(func(ctx context.Context) error {
		for tmpResult := range tmpResultChan {
			handlerFunc := c.localCheckResolver.CheckRewrite(ctx,
				&graph.ResolveCheckRequest{
					StoreID:              req.StoreID,
					AuthorizationModelID: c.typesystem.GetAuthorizationModelID(),
					TupleKey:             tuple.NewTupleKey(tmpResult.Object, req.Relation, req.User.String()),
					ContextualTuples:     req.ContextualTuples,
					Context:              req.Context,
					Consistency:          req.Consistency,
					RequestMetadata:      graph.NewCheckRequestMetadata(),
				}, userset)
			tmpCheckResult, err := handlerFunc(ctx)
			if err != nil {
				functionName := "intersectionHandler"
				if !isAllowed {
					functionName = "exclusionHandler"
				}
				c.logger.Error("Failed to execute", zap.Error(err),
					zap.String("function", functionName),
					zap.String("object", tmpResult.Object),
					zap.String("relation", req.Relation),
					zap.String("user", req.User.String()))
				return err
			}

			// If the allowed value does not match what we expect, we skip this candidate.
			// eg, for intersection we expect the check result to be true
			// and for exclusion we expect the check result to be false.
			if tmpCheckResult.GetAllowed() != isAllowed {
				continue
			}

			// If the original stack only had 1 value, we can trySendCandidate right away (nothing more to check)
			if stack.Len(req.relationStack) == 0 {
				c.trySendCandidate(ctx, false, tmpResult.Object, resultChan)
				continue
			}

			// If the original stack had more than 1 value, we need to query the parent values
			// new stack with top item in stack
			err = c.queryForTuples(ctx, req, false, resultChan, tmpResult.Object)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// invoke loopOverWeightedEdges to get list objects candidate. Check
// will then be invoked on the non-lowest weight edges against these
// list objects candidates. If check returns true, then the list
// object candidates are true candidates and will be returned via
// resultChan. If check returns false, then these list object candidates
// are invalid because it does not satisfy all paths for intersection.
func (c *ReverseExpandQuery) intersectionHandler(
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) error {
	intersectionEdgeComparison, err := typesystem.GetEdgesForIntersection(edges, sourceUserType)
	if err != nil {
		c.logger.Error("Failed to get lowest weight edge",
			zap.String("function", "intersectionHandler"),
			zap.Error(err),
			zap.Any("edges", edges),
			zap.String("sourceUserType", sourceUserType))
		return err
	}

	if !intersectionEdgeComparison.DirectEdgesAreLeastWeight && intersectionEdgeComparison.LowestEdge == nil {
		// no need to go further because list objects must return empty
		return nil
	}

	lowestWeightEdges := []*weightedGraph.WeightedAuthorizationModelEdge{intersectionEdgeComparison.LowestEdge}

	if intersectionEdgeComparison.DirectEdgesAreLeastWeight {
		lowestWeightEdges = intersectionEdgeComparison.DirectEdges
	}

	tmpResultChan := make(chan *ReverseExpandResult, listObjectsResultChannelLength)

	siblings := intersectionEdgeComparison.Siblings
	usersets := make([]*openfgav1.Userset, 0, len(siblings)+1)

	if !intersectionEdgeComparison.DirectEdgesAreLeastWeight && len(intersectionEdgeComparison.DirectEdges) > 0 {
		// direct weight is not the lowest edge. Therefore, need to call check against directly assigned types.
		usersets = append(usersets, typesystem.This())
	}

	for _, sibling := range siblings {
		userset, err := c.typesystem.ConstructUserset(sibling)
		if err != nil {
			// This should never happen.
			c.logger.Error("Failed to construct userset",
				zap.String("function", "intersectionHandler"),
				zap.Any("edge", sibling),
				zap.Error(err))
			return err
		}
		usersets = append(usersets, userset)
	}
	userset := &openfgav1.Userset{
		Userset: &openfgav1.Userset_Intersection{
			Intersection: &openfgav1.Usersets{
				Child: usersets,
			}}}

	// Concurrently find candidates and call check on them as they are found
	c.findCandidatesForLowestWeightEdge(pool, req, tmpResultChan, lowestWeightEdges, sourceUserType, resolutionMetadata)
	c.callCheckForCandidates(pool, req, tmpResultChan, resultChan, userset, true)

	return nil
}

// invoke loopOverWeightedEdges to get list objects candidate. Check
// will then be invoked on the excluded edge against these
// list objects candidates. If check returns false, then the list
// object candidates are true candidates and will be returned via
// resultChan. If check returns true, then these list object candidates
// are invalid because it does not satisfy all paths for exclusion.
func (c *ReverseExpandQuery) exclusionHandler(
	ctx context.Context,
	pool *concurrency.Pool,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	sourceUserType string,
	resolutionMetadata *ResolutionMetadata,
) error {
	baseEdges, excludedEdge, err := typesystem.GetEdgesForExclusion(edges, sourceUserType)
	if err != nil {
		c.logger.Error("Failed to get lowest weight edge",
			zap.String("function", "exclusionHandler"),
			zap.Error(err),
			zap.Any("edges", edges),
			zap.String("sourceUserType", sourceUserType))
		return err
	}

	// This means the exclusion edge does not have a path to the terminal type.
	// e.g. `B` in `A but not B` is not relevant to this query.
	if excludedEdge == nil {
		newReq := req.clone()

		return c.shallowClone().loopOverEdges(
			ctx,
			newReq,
			baseEdges,
			false,
			resolutionMetadata,
			resultChan,
			sourceUserType,
		)
	}

	tmpResultChan := make(chan *ReverseExpandResult, listObjectsResultChannelLength)

	userset, err := c.typesystem.ConstructUserset(excludedEdge)
	if err != nil {
		// This should never happen.
		c.logger.Error("Failed to construct userset",
			zap.String("function", "exclusionHandler"),
			zap.Any("edge", excludedEdge),
			zap.Error(err))
		return err
	}

	// Concurrently find candidates and call check on them as they are found
	c.findCandidatesForLowestWeightEdge(pool, req, tmpResultChan, baseEdges, sourceUserType, resolutionMetadata)
	c.callCheckForCandidates(pool, req, tmpResultChan, resultChan, userset, false)

	return nil
}
