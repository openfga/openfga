package check

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	modelUtils "github.com/openfga/openfga/pkg/typesystem"
)

var tracer = otel.Tracer("internal/check")

type Config struct {
	Model            *AuthorizationModelGraph
	Datastore        storage.RelationshipTupleReader
	Planner          *planner.Planner
	ConcurrencyLimit int
	UpstreamTimeout  time.Duration
	Logger           logger.Logger
	Strategies       map[string]Strategy
}
type Resolver struct {
	model            *AuthorizationModelGraph
	datastore        storage.RelationshipTupleReader
	planner          *planner.Planner
	concurrencyLimit int
	upstreamTimeout  time.Duration
	logger           logger.Logger

	strategies map[string]Strategy
}

type Request = graph.ResolveCheckRequest // TODO: Once we finish migrating, move into this package and drop useless fields (VisitedPaths, RequestMetadata)
type Response = graph.ResolveCheckResponse

type RequestParams = graph.ResolveCheckRequestParams

func NewRequest(p RequestParams) (*Request, error) {
	return graph.NewResolveCheckRequest(p)
}

var ErrPanicRequest = errors.New("invalid check request") // == panic in ResolveCheck so should be handled accordingly (should be seen as a 500 to client)

type ResponseMsg struct {
	Res *Response
	Err error
}

func New(cfg Config) *Resolver {
	r := &Resolver{
		model:            cfg.Model,
		datastore:        cfg.Datastore,
		planner:          cfg.Planner,
		concurrencyLimit: cfg.ConcurrencyLimit,
		upstreamTimeout:  cfg.UpstreamTimeout,
		logger:           cfg.Logger,
		strategies:       cfg.Strategies,
	}

	return r
}

func (r *Resolver) ResolveCheck(ctx context.Context, req *Request) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveCheck", trace.WithAttributes(
		attribute.String("store_id", req.GetStoreID()),
		attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(req.GetTupleKey())),
	))
	defer span.End()

	// TODO: Handle where User is a userset (model would dynamically compute weight in order to prune branches, needs work in language)

	// TODO: While we are doing the rollout, ok should never be false due it being caught by the validation in the command layer via `validateCheckRequest`.
	// Once the rollout is done, we should swap the existing implementation with this which is much more efficient.
	node, _ := r.model.GetNodeByID(tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation()))

	_, ok := node.GetWeight(req.GetUserType())
	if !ok {
		// If the user type is not reachable from the object type and relation, we can immediately return false.
		return &Response{Allowed: false}, nil
	}

	return r.ResolveUnion(ctx, req, node, nil)
}

func (r *Resolver) GetModel() *AuthorizationModelGraph {
	return r.model
}

func (r *Resolver) ResolveUnionEdges(ctx context.Context, req *Request, edges []*authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make(chan ResponseMsg, len(edges))
	var pool errgroup.Group
	pool.SetLimit(r.concurrencyLimit)
	defer func() {
		_ = pool.Wait()
		close(out)
	}()

	for _, edge := range edges {
		pool.Go(func() error {
			res, err := r.ResolveEdge(ctx, req, edge, visited)
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, out)
			return nil
		})
	}

	var err error
	for range edges {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-out:
			if msg.Err != nil {
				err = msg.Err
				continue
			}

			if msg.Res.Allowed {
				// Short-circuit: In a union, if any branch returns true, we can immediately return.
				return msg.Res, nil
			}
		}
	}
	// we only return error in a union when all edges are exhausted and there is at least one edge with error
	return &Response{Allowed: false}, err
}

// reduce as a logical union operation (exit the moment we have a single true).
func (r *Resolver) ResolveUnion(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode, visited *sync.Map) (*Response, error) {
	if visited == nil && node.GetNodeType() == authzGraph.SpecificTypeAndRelation && (node.GetRecursiveRelation() == node.GetUniqueLabel() || node.IsPartOfTupleCycle()) {
		// initialize visited map for first time,
		visited = &sync.Map{}
		// add the first object#relation that is being evaluated
		visited.Store(tuple.ToObjectRelationString(req.GetTupleKey().GetObject(), req.GetTupleKey().GetRelation()), struct{}{})
		// if it is not first time we don't need to resolve any recursive relation because we are already iterating over it
		if node.GetRecursiveRelation() == node.GetUniqueLabel() && !node.IsPartOfTupleCycle() {
			if edge, ok := r.model.CanApplyRecursiveOptimization(node, node.GetRecursiveRelation(), req.GetUserType()); ok {
				return r.ResolveRecursive(ctx, req, edge, visited)
			}
		}
	}

	// flatten the node to get all terminal edges to avoid unnecessary goroutines
	terminalEdges, err := r.model.FlattenNode(node, req.GetUserType())
	if err != nil {
		return nil, ErrPanicRequest
	}

	return r.ResolveUnionEdges(ctx, req, terminalEdges, visited)
}

func (r *Resolver) executeStrategy(keyPlan *planner.KeyPlan, strategy *planner.KeyPlanStrategy,
	fn func() (*Response, error)) (*Response, error) {
	start := time.Now()
	res, err := fn()
	if err != nil {
		// penalize plans that timeout from the upstream context
		if errors.Is(err, context.DeadlineExceeded) {
			keyPlan.UpdateStats(strategy, r.upstreamTimeout)
		}
		return nil, err
	}
	keyPlan.UpdateStats(strategy, time.Since(start))
	return res, nil
}

func (r *Resolver) resolveRecursiveUserset(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	conditionEdge, err := r.model.GetDirectEdgeFromNodeForUserType(tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation()), edge.GetTo().GetUniqueLabel())
	if err != nil {
		return nil, ErrPanicRequest
	}
	iter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
		Object:   req.GetTupleKey().GetObject(),
		Relation: req.GetTupleKey().GetRelation(),
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
			Type:               objectType,
			RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: relation},
		}},
		Conditions: conditionEdge.GetConditions(),
	}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{Preference: req.GetConsistency()}})
	if err != nil {
		return nil, err
	}
	defer iter.Stop()

	i := storage.NewTupleKeyIteratorFromTupleIterator(iter)
	if len(conditionEdge.GetConditions()) > 1 || conditionEdge.GetConditions()[0] != authzGraph.NoCond {
		i = storage.NewConditionsFilteredTupleKeyIterator(i,
			BuildTupleKeyConditionFilter(ctx, r.model, conditionEdge, req.GetContext()),
		)
	}
	i = storage.NewDeduplicatedTupleKeyIterator(i, visited, func(key *openfgav1.TupleKey) string {
		return key.GetUser() // this is a userset (object#relation)
	})

	var b strings.Builder
	b.WriteString("v2|")
	b.WriteString("userset|")
	b.WriteString(req.GetAuthorizationModelID())
	b.WriteString("|")
	b.WriteString(edge.GetTo().GetUniqueLabel())
	b.WriteString("|")
	b.WriteString(req.GetUserType())
	b.WriteString("|")
	b.WriteString("infinite")

	possibleStrategies := map[string]*planner.KeyPlanStrategy{
		DefaultStrategyName:   DefaultRecursivePlan,
		RecursiveStrategyName: RecursivePlan,
	}

	keyPlan := r.planner.GetKeyPlan(b.String())
	strategy := keyPlan.SelectStrategy(possibleStrategies)

	return r.executeStrategy(keyPlan, strategy, func() (*Response, error) {
		return r.strategies[strategy.Type].Userset(ctx, req, edge, i, visited)
	})
}

func (r *Resolver) resolveRecursiveTTU(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	_, tuplesetRelation := tuple.SplitObjectRelation(edge.GetTuplesetRelation())
	subjectType, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	conditionEdge, err := r.model.GetDirectEdgeFromNodeForUserType(tuplesetRelation, subjectType)
	if err != nil {
		return nil, ErrPanicRequest
	}

	iter, err := r.datastore.Read(
		ctx,
		req.GetStoreID(),
		storage.ReadFilter{
			Object:     req.GetTupleKey().GetObject(),
			Relation:   tuplesetRelation,
			User:       subjectType + ":",
			Conditions: conditionEdge.GetConditions(),
		},
		storage.ReadOptions{Consistency: storage.ConsistencyOptions{Preference: req.GetConsistency()}},
	)
	if err != nil {
		return nil, err
	}

	defer iter.Stop()

	i := storage.NewTupleKeyIteratorFromTupleIterator(iter)
	if len(conditionEdge.GetConditions()) > 1 || conditionEdge.GetConditions()[0] != authzGraph.NoCond {
		i = storage.NewConditionsFilteredTupleKeyIterator(i,
			BuildTupleKeyConditionFilter(ctx, r.model, conditionEdge, req.GetContext()),
		)
	}
	i = storage.NewDeduplicatedTupleKeyIterator(i, visited, func(key *openfgav1.TupleKey) string {
		return tuple.ToObjectRelationString(key.GetUser(), computedRelation)
	})

	var b strings.Builder
	b.WriteString("v2|")
	b.WriteString("ttu|")
	b.WriteString(req.GetAuthorizationModelID())
	b.WriteString("|")
	b.WriteString(edge.GetTo().GetUniqueLabel())
	b.WriteString("|")
	b.WriteString(req.GetUserType())
	b.WriteString("|")
	b.WriteString(edge.GetTuplesetRelation())
	b.WriteString("|")
	b.WriteString("infinite")

	possibleStrategies := map[string]*planner.KeyPlanStrategy{
		DefaultStrategyName:   DefaultRecursivePlan,
		RecursiveStrategyName: RecursivePlan,
	}

	keyPlan := r.planner.GetKeyPlan(b.String())
	strategy := keyPlan.SelectStrategy(possibleStrategies)

	return r.executeStrategy(keyPlan, strategy, func() (*Response, error) {
		return r.strategies[strategy.Type].TTU(ctx, req, edge, i, visited)
	})
}

func (r *Resolver) ResolveRecursive(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	nonRecursiveEdges, err := r.model.FlattenRecursiveNode(edge.GetTo(), req.GetUserType())
	if err != nil {
		return nil, ErrPanicRequest
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	out := make(chan ResponseMsg, 2)
	go func() {
		res, err := r.ResolveUnionEdges(ctx, req, nonRecursiveEdges, visited)
		concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, out)
	}()

	go func() {
		var err error
		var res *Response
		switch edge.GetEdgeType() {
		case authzGraph.DirectEdge:
			res, err = r.resolveRecursiveUserset(ctx, req, edge, visited)
		case authzGraph.TTUEdge:
			res, err = r.resolveRecursiveTTU(ctx, req, edge, visited)
		default:
			res, err = nil, ErrPanicRequest
		}

		concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, out)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-out:
			if msg.Err != nil {
				err = msg.Err
				continue
			}

			if msg.Res.Allowed {
				return msg.Res, nil
			}
		}
	}
	return &Response{Allowed: false}, err
}

// reduce as a logical intersection operation (exit the moment we have a single false)
// should panic if a single handler returns nil.
func (r *Resolver) ResolveIntersection(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode) (*Response, error) {
	edges, ok := r.model.GetEdgesFromNode(node)
	if !ok {
		return nil, ErrPanicRequest
	}

	ctx, cancel := context.WithCancel(ctx)
	out := make(chan ResponseMsg, len(edges))

	var pool errgroup.Group
	pool.SetLimit(r.concurrencyLimit)
	defer func() {
		cancel()
		_ = pool.Wait()
		close(out)
	}()

	scheduledHandlers := 0

	for _, edge := range edges {
		_, ok := edge.GetWeight(req.GetUserType())
		if !ok {
			return nil, ErrPanicRequest
		}
		scheduledHandlers++
		pool.Go(func() error {
			// intersection is never part of a cycle or recursion
			res, err := r.ResolveEdge(ctx, req, edge, nil)
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, out)
			return nil
		})
	}

	var err error
	for i := 0; i < scheduledHandlers; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-out:
			if msg.Err != nil || !msg.Res.Allowed {
				// NOTE: This is one of the breaking changes from the current check implementation. Delete this after this rollout.
				// In intersection _every_ branch must return true.
				return msg.Res, msg.Err
			}
		}
	}
	return &Response{Allowed: true}, err
}

// reduce as a logical exclusion operation
// if base is false, short circuit.
func (r *Resolver) ResolveExclusion(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode) (*Response, error) {
	edges, ok := r.model.GetEdgesFromNode(node)
	if !ok {
		return nil, ErrPanicRequest
	}
	// base edge validation
	if _, ok := edges[0].GetWeight(req.GetUserType()); !ok {
		return nil, ErrPanicRequest
	}
	ctx, cancel := context.WithCancel(ctx)
	base := make(chan ResponseMsg, 1)
	wg := &sync.WaitGroup{}

	scheduledHandlers := 1
	wg.Add(1)
	go func() {
		defer wg.Done()
		// exclusion is never part of a cycle or recursion
		res, err := r.ResolveEdge(ctx, req, edges[0], nil)
		concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, base)
		close(base)
	}()

	defer func() {
		cancel()
		wg.Wait()
	}()

	var subtract chan ResponseMsg
	// excluded edge
	if _, ok := edges[1].GetWeight(req.GetUserType()); ok {
		scheduledHandlers++
		subtract = make(chan ResponseMsg, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// exclusion is never part of a cycle or recursion
			res, err := r.ResolveEdge(ctx, req, edges[1], nil)
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, subtract)
			close(subtract)
		}()
	}

	// Loop until we have received the necessary results to determine the outcome.
	resultsReceived := 0
	for resultsReceived < scheduledHandlers {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case msg, ok := <-base:
			if !ok {
				base = nil // Stop selecting this case.
				continue
			}
			resultsReceived++

			if msg.Err != nil {
				// NOTE: This is one of the breaking changes from the current check implementation. Delete this after this rollout.
				// If base returns an error, we return it immediately since the result of the exclusion cannot be determined.
				return nil, msg.Err
			}

			// Short-circuit: If base is false, the whole expression is false.
			if !msg.Res.GetAllowed() {
				return &Response{Allowed: false}, nil
			}

			// Short-circuit: If base is true and there's no subtract, the whole expression is true.
			if msg.Res.Allowed && subtract == nil {
				return msg.Res, nil
			}

		case msg, ok := <-subtract: // subtract can be nil
			if !ok {
				subtract = nil // Stop selecting this case.
				continue
			}
			resultsReceived++

			if msg.Err != nil {
				// NOTE: This is one of the breaking changes from the current check implementation. Delete this after this rollout.
				// If subtract returns an error, we return it immediately since the result of the exclusion cannot be determined. There will never be a case in which it returns false if it returns an error given there is only one branch.
				return nil, msg.Err
			}

			// Short-circuit: If subtract is true, the whole expression is false.
			if msg.Res.GetAllowed() {
				return &Response{Allowed: false}, nil
			}
		}
	}

	// The only way to get here is if base was (Allowed: true) and subtract was (Allowed: false).
	return &Response{Allowed: true}, nil
}

func (r *Resolver) ResolveEdge(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	var visitedObjects *sync.Map
	if edge.IsPartOfTupleCycle() || edge.GetRecursiveRelation() != "" {
		visitedObjects = visited
	}
	// computed edges are solved by the relation node caller
	switch edge.GetEdgeType() {
	case authzGraph.DirectEdge:
		switch edge.GetTo().GetNodeType() {
		case authzGraph.SpecificType:
			// terminal types are never part of a cycle
			return r.specificType(ctx, req, edge)
		case authzGraph.SpecificTypeWildcard:
			// terminal types are never part of a cycle
			return r.specificTypeWildcard(ctx, req, edge)
		case authzGraph.SpecificTypeAndRelation:
			// check for recursiveRelation
			return r.specificTypeAndRelation(ctx, req, edge, visitedObjects)
		default:
			return nil, ErrPanicRequest
		}
	case authzGraph.DirectLogicalEdge:
		return r.ResolveUnion(ctx, req, edge.GetTo(), visitedObjects)
	case authzGraph.TTUEdge:
		return r.ttu(ctx, req, edge, visitedObjects)
	case authzGraph.TTULogicalEdge:
		return r.ResolveUnion(ctx, req, edge.GetTo(), visitedObjects)
	case authzGraph.RewriteEdge:
		return r.ResolveRewrite(ctx, req, edge.GetTo(), visitedObjects)
	default:
		return nil, ErrPanicRequest
	}
}

func (r *Resolver) ResolveRewrite(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode, visited *sync.Map) (*Response, error) {
	// relation and union have save behavior
	switch node.GetNodeType() {
	case authzGraph.SpecificTypeAndRelation:
		return r.ResolveUnion(ctx, req, node, visited)
	case authzGraph.OperatorNode:
		switch node.GetLabel() {
		case authzGraph.UnionOperator:
			return r.ResolveUnion(ctx, req, node, visited)
		case authzGraph.IntersectionOperator:
			// intersection is never part of a graph cycle
			return r.ResolveIntersection(ctx, req, node)
		case authzGraph.ExclusionOperator:
			// exclusion is never part of a graph cycle
			return r.ResolveExclusion(ctx, req, node)
		default:
			return nil, ErrPanicRequest
		}
	default:
		return nil, ErrPanicRequest
	}
}

func (r *Resolver) specificType(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) (*Response, error) {
	ctx, span := tracer.Start(ctx, "specificType",
		trace.WithAttributes(
			attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(req.GetTupleKey())),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	_, relation := tuple.SplitObjectRelation(edge.GetFrom().GetUniqueLabel())
	tk := tuple.NewTupleKey(req.GetTupleKey().GetObject(), relation, req.GetTupleKey().GetUser())
	t, err := r.datastore.ReadUserTuple(ctx, req.GetStoreID(), tk, storage.ReadUserTupleOptions{Consistency: storage.ConsistencyOptions{Preference: req.GetConsistency()}})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &Response{Allowed: false}, nil
		}
		return nil, err
	}

	allowed, err := evaluateCondition(ctx, r.model, edge, t.GetKey(), req.GetContext())
	if err != nil {
		return nil, err
	}
	if allowed {
		span.SetAttributes(attribute.Bool("allowed", true))
	}
	return &Response{Allowed: allowed}, nil
}

func (r *Resolver) specificTypeWildcard(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) (*Response, error) {
	ctx, span := tracer.Start(ctx, "specificTypeWildcard",
		trace.WithAttributes(
			attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(req.GetTupleKey())),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	_, relation := tuple.SplitObjectRelation(edge.GetFrom().GetUniqueLabel())
	// Query via ReadUsersetTuples instead of ReadUserTuple tuples to take iterator cache.
	iter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
		Object:                      req.GetTupleKey().GetObject(),
		Relation:                    relation,
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{modelUtils.WildcardRelationReference(req.GetTupleKey().GetUser())},
		Conditions:                  edge.GetConditions(),
	}, storage.ReadUsersetTuplesOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	})
	if err != nil {
		return nil, err
	}

	defer iter.Stop()

	// NOTE: Iterator should only really have one entry which should be a specific wildcard tuple.
	t, err := iter.Head(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrIteratorDone) {
			return &Response{Allowed: false}, nil
		}
		return nil, err
	}

	allowed, err := evaluateCondition(ctx, r.model, edge, t.GetKey(), req.GetContext())
	if err != nil {
		return nil, err
	}
	if allowed {
		span.SetAttributes(attribute.Bool("allowed", true))
	}
	return &Response{Allowed: allowed}, nil
}

// TODO: At the current time we will only handle non recursive/tuple cycle which have to be validated at an earlier stage.
func (r *Resolver) specificTypeAndRelation(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "specificTypeAndRelation",
		trace.WithAttributes(
			attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(req.GetTupleKey())),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	userObjectType, userRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	_, relation := tuple.SplitObjectRelation(edge.GetFrom().GetUniqueLabel())

	// userset that is represented by the same edge, conditions on the iterator will be defined in this edge
	iter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
		Object:   req.GetTupleKey().GetObject(),
		Relation: relation,
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
			Type:               userObjectType,
			RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: userRelation},
		}},
		Conditions: edge.GetConditions(),
	}, storage.ReadUsersetTuplesOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	})
	if err != nil {
		return nil, err
	}
	defer iter.Stop()

	// TODO change pattern of chaining iterators and have one iterator multiple filters
	i := storage.NewTupleKeyIteratorFromTupleIterator(iter)
	if len(edge.GetConditions()) > 1 || edge.GetConditions()[0] != authzGraph.NoCond {
		i = storage.NewConditionsFilteredTupleKeyIterator(i,
			BuildTupleKeyConditionFilter(ctx, r.model, edge, req.GetContext()),
		)
	}
	// only when we are in the presence of visited (meaning tuple cycle or recursion) we need to deduplicate to avoid infinite loop
	if visited != nil {
		i = storage.NewDeduplicatedTupleKeyIterator(i, visited, func(key *openfgav1.TupleKey) string {
			return key.GetUser()
		})
	}

	// TODO: Need optimization to solve userset as principal
	if tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
		return r.strategies[DefaultStrategyName].Userset(ctx, req, edge, i, visited)
	}

	possibleStrategies := map[string]*planner.KeyPlanStrategy{
		DefaultStrategyName: DefaultPlan,
	}

	if w, _ := edge.GetWeight(req.GetUserType()); w == 2 {
		possibleStrategies[WeightTwoStrategyName] = weight2Plan
	}

	usersetKey := createUsersetPlanKey(req, edge.GetTo().GetUniqueLabel())
	keyPlan := r.planner.GetKeyPlan(usersetKey)
	strategy := keyPlan.SelectStrategy(possibleStrategies)

	return r.executeStrategy(keyPlan, strategy, func() (*Response, error) {
		return r.strategies[strategy.Type].Userset(ctx, req, edge, i, visited)
	})
}

func (r *Resolver) ttu(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	_, tuplesetRelation := tuple.SplitObjectRelation(edge.GetTuplesetRelation())
	subjectType, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	ctx, span := tracer.Start(ctx, "ttu",
		trace.WithAttributes(
			attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(req.GetTupleKey())),
			attribute.String("tupleset_relation", tuple.ToObjectRelationString(req.GetObjectType(), tuplesetRelation)),
			attribute.String("computed_relation", computedRelation),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	// selecting the edge for the tupleset that points directly to the specific subjecttype
	// the graph is already deduplicated by the conditions and combined in one unique edge for the same tupleset
	tuplesetEdge, err := r.model.GetDirectEdgeFromNodeForUserType(edge.GetTuplesetRelation(), subjectType)
	if err != nil {
		return nil, ErrPanicRequest
	}

	iter, err := r.datastore.Read(
		ctx,
		req.GetStoreID(),
		storage.ReadFilter{
			Object:     req.GetTupleKey().GetObject(),
			Relation:   tuplesetRelation,
			User:       subjectType + ":",
			Conditions: tuplesetEdge.GetConditions(),
		},
		storage.ReadOptions{Consistency: storage.ConsistencyOptions{Preference: req.GetConsistency()}},
	)
	if err != nil {
		return nil, err
	}

	defer iter.Stop()
	// TODO change pattern of chaining iterators and have one iterator multiple filters
	i := storage.NewTupleKeyIteratorFromTupleIterator(iter)
	if len(tuplesetEdge.GetConditions()) > 1 || tuplesetEdge.GetConditions()[0] != authzGraph.NoCond {
		i = storage.NewConditionsFilteredTupleKeyIterator(i,
			BuildTupleKeyConditionFilter(ctx, r.model, tuplesetEdge, req.GetContext()),
		)
	}
	// only when we are in the presence of visited (meaning tuple cycle or recursion) we need to deduplicate to avoid infinite loop
	if visited != nil {
		i = storage.NewDeduplicatedTupleKeyIterator(i, visited, func(key *openfgav1.TupleKey) string {
			return tuple.ToObjectRelationString(key.GetUser(), computedRelation)
		})
	}

	// TODO: Need optimization to solve userset as principal
	if tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
		return r.strategies[DefaultStrategyName].TTU(ctx, req, edge, i, visited)
	}

	possibleStrategies := map[string]*planner.KeyPlanStrategy{
		DefaultStrategyName: DefaultPlan,
	}

	if w, _ := edge.GetWeight(req.GetUserType()); w == 2 {
		possibleStrategies[WeightTwoStrategyName] = weight2Plan
	}

	planKey := createTTUPlanKey(req, tuplesetRelation, computedRelation)
	keyPlan := r.planner.GetKeyPlan(planKey)
	strategy := keyPlan.SelectStrategy(possibleStrategies)

	return r.executeStrategy(keyPlan, strategy, func() (*Response, error) {
		return r.strategies[strategy.Type].TTU(ctx, req, edge, i, visited)
	})
}

func createTTUPlanKey(req *Request, tuplesetRelation string, computedRelation string) string {
	var b strings.Builder
	b.WriteString("v2|")
	b.WriteString("ttu|")
	b.WriteString(req.GetAuthorizationModelID())
	b.WriteString("|")
	b.WriteString(req.GetObjectType())
	b.WriteString("|")
	b.WriteString(req.GetTupleKey().GetRelation())
	b.WriteString("|")
	b.WriteString(req.GetUserType())
	b.WriteString("|")
	b.WriteString(tuplesetRelation)
	b.WriteString("|")
	b.WriteString(computedRelation)
	return b.String()
}

func createUsersetPlanKey(req *Request, userset string) string {
	var b strings.Builder
	b.WriteString("v2|")
	b.WriteString("userset|")
	b.WriteString(req.GetAuthorizationModelID())
	b.WriteString("|")
	b.WriteString(req.GetObjectType())
	b.WriteString("|")
	b.WriteString(req.GetTupleKey().GetRelation())
	b.WriteString("|")
	b.WriteString(req.GetUserType())
	b.WriteString("|")
	b.WriteString(userset)
	return b.String()
}
