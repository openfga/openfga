package check

import (
	"context"
	"errors"
	"slices"
	"sort"
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
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
)

const cacheKeyDelimiter = "|"
const cacheKeyPrefix = "c."

var tracer = otel.Tracer("internal/check")

var ErrValidation = errors.New("object relation does not exist")
var ErrUsersetInvalidRequest = errors.New("userset request cannot be resolved when exclusion operation is involved")
var ErrPanicRequest = errors.New("invalid check request")
var ErrWildcardInvalidRequest = errors.New("wildcard request cannot be resolved when intersection or exclusion is involved")

type Config struct {
	Model                     *modelgraph.AuthorizationModelGraph
	Datastore                 storage.RelationshipTupleReader
	Cache                     storage.InMemoryCache[any]
	CacheTTL                  time.Duration
	LastCacheInvalidationTime time.Time
	Planner                   planner.Manager
	ConcurrencyLimit          int
	UpstreamTimeout           time.Duration
	Logger                    logger.Logger
	Strategies                map[string]Strategy
}
type Resolver struct {
	model                     *modelgraph.AuthorizationModelGraph
	datastore                 storage.RelationshipTupleReader
	cache                     storage.InMemoryCache[any]
	cacheTTL                  time.Duration
	lastCacheInvalidationTime time.Time
	planner                   planner.Manager
	concurrencyLimit          int
	upstreamTimeout           time.Duration
	logger                    logger.Logger

	strategies map[string]Strategy
}

func New(cfg Config) *Resolver {
	r := &Resolver{
		model:                     cfg.Model,
		datastore:                 cfg.Datastore,
		cache:                     cfg.Cache,
		cacheTTL:                  cfg.CacheTTL,
		lastCacheInvalidationTime: cfg.LastCacheInvalidationTime,
		planner:                   cfg.Planner,
		concurrencyLimit:          cfg.ConcurrencyLimit,
		upstreamTimeout:           cfg.UpstreamTimeout,
		logger:                    cfg.Logger,
		strategies:                cfg.Strategies,
	}

	if r.strategies == nil {
		r.strategies = map[string]Strategy{
			DefaultStrategyName:   NewDefault(cfg.Model, r, cfg.ConcurrencyLimit),
			WeightTwoStrategyName: NewWeight2(cfg.Model, cfg.Datastore),
			RecursiveStrategyName: NewRecursive(cfg.Model, cfg.Datastore, cfg.ConcurrencyLimit),
		}
	}

	return r
}

func (r *Resolver) ResolveCheck(ctx context.Context, req *Request) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveCheck", trace.WithAttributes(
		attribute.String("store_id", req.GetStoreID()),
		attribute.String("tuple_key", req.GetTupleString()),
		attribute.Bool("allowed", false),
	))
	defer span.End()

	node, ok := r.model.GetNodeByID(tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation()))
	if !ok {
		// this should never happen as the request is already validated before
		return nil, ErrPanicRequest
	}

	// GetUserType returns the user type if the request in not an object relation otherwise the usertyperelation
	_, ok = r.model.GetNodeWeight(node, req.GetUserType())
	if !ok {
		// If the user type is not reachable from the object type and relation, we can immediately return false.
		return &Response{Allowed: false}, nil
	}

	// Check if the user is a wildcard and if the node does not have a path to the user type wildcard then return false
	if req.IsTypedWildcard() && !slices.Contains(node.GetWildcards(), req.GetUserType()) {
		return &Response{Allowed: false}, nil
	}

	res, err := r.ResolveUnion(ctx, req, node, nil)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}
	if res.GetAllowed() {
		span.SetAttributes(attribute.Bool("allowed", true))
	}

	return res, nil
}

func (r *Resolver) isCached(consistency openfgav1.ConsistencyPreference, key string) (*Response, bool) {
	if consistency == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY || r.lastCacheInvalidationTime.IsZero() {
		return nil, false
	}
	v := r.cache.Get(key)
	if v == nil {
		return nil, false
	}
	res, ok := v.(*ResponseCacheEntry)
	if !ok {
		return nil, false
	}
	if !res.LastModified.After(r.lastCacheInvalidationTime) {
		return nil, false
	}
	return res.Res, true
}

func buildEdgeCacheKey(modelID string, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) string {
	keyBuilder := &strings.Builder{}
	keyBuilder.WriteString(cacheKeyPrefix)
	keyBuilder.WriteString(modelID)
	keyBuilder.WriteString(cacheKeyDelimiter)
	keyBuilder.WriteString(req.GetTupleKey().GetObject())
	keyBuilder.WriteString(cacheKeyDelimiter)
	keyBuilder.WriteString(req.GetTupleKey().GetUser())
	keyBuilder.WriteString(cacheKeyDelimiter)
	keyBuilder.WriteString(edge.GetRelationDefinition())
	keyBuilder.WriteString(req.GetInvariantCacheKey())
	return keyBuilder.String()
}

func (r *Resolver) ResolveUnionEdges(ctx context.Context, req *Request, edges []*authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if res, ok := r.isCached(req.GetConsistency(), req.GetCacheKey()); ok {
		return res, nil
	}

	out := make(chan ResponseMsg, len(edges))
	var pool errgroup.Group
	pool.SetLimit(r.concurrencyLimit)
	defer func() {
		_ = pool.Wait()
		close(out)
	}()

	relation := req.GetTupleKey().GetRelation()
	objectType := tuple.GetType(req.GetTupleKey().GetObject())
	objectRelation := tuple.ToObjectRelationString(objectType, relation)
	ids := make([]string, 0, len(edges))
	for _, edge := range edges {
		id := buildEdgeCacheKey(r.model.GetModelID(), req, edge)
		ids = append(ids, id)
		if res, ok := r.isCached(req.GetConsistency(), id); ok {
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{ID: id, Res: res}, out)
			continue
		}
		pool.Go(func() error {
			res, err := r.ResolveEdge(ctx, req, edge, visited)
			// we only need to cache the response for the edge if the edge does not belong to the request relation
			// otherwise the subproblem should be sufficient
			if err != nil && edge.GetRelationDefinition() != objectRelation {
				entry := &ResponseCacheEntry{Res: res, LastModified: time.Now()}
				r.cache.Set(id, entry, r.cacheTTL)
			}
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{ID: id, Res: res, Err: err}, out)
			return nil
		})
	}

	var err error
	for range ids {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-out:
			if msg.Err != nil {
				err = msg.Err
				continue
			}

			if msg.Res.GetAllowed() {
				// Short-circuit: In a union, if any branch returns true, we can immediately return.
				entry := &ResponseCacheEntry{Res: msg.Res, LastModified: time.Now()}
				r.cache.Set(req.GetCacheKey(), entry, r.cacheTTL)
				return msg.Res, nil
			}
		}
	}
	if err != nil {
		// we only return error in a union when all edges are exhausted and there is at least one edge with error
		return nil, err
	}
	res := &Response{Allowed: false}
	entry := &ResponseCacheEntry{Res: res, LastModified: time.Now()}
	r.cache.Set(req.GetCacheKey(), entry, r.cacheTTL)
	return res, nil
}

// reduce as a logical union operation (exit the moment we have a single true).
func (r *Resolver) ResolveUnion(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode, visited *sync.Map) (*Response, error) {
	emptyCycle := visited == nil
	if emptyCycle && node.GetNodeType() == authzGraph.SpecificTypeAndRelation && (node.GetRecursiveRelation() == node.GetUniqueLabel() || node.IsPartOfTupleCycle()) {
		// initialize visited map for first time,
		visited = &sync.Map{}
		// add the first object#relation that is being evaluated
		visited.Store(tuple.ToObjectRelationString(req.GetTupleKey().GetObject(), req.GetTupleKey().GetRelation()), struct{}{})
	}

	edge, withOptimization := r.model.CanApplyRecursion(node, req.GetUserType(), emptyCycle)
	if edge != nil {
		return r.ResolveRecursive(ctx, req, edge, visited, withOptimization)
	}

	// flatten the node to get all terminal edges to avoid unnecessary goroutines
	terminalEdges, err := r.model.FlattenNode(node, req.GetUserType(), req.IsTypedWildcard(), false)
	if err != nil {
		return nil, errors.Join(ErrPanicRequest, err)
	}

	return r.ResolveUnionEdges(ctx, req, terminalEdges, visited)
}

func (r *Resolver) executeStrategy(ctx context.Context, selector planner.Selector, strategy *planner.PlanConfig, fn func() (*Response, error)) (*Response, error) {
	span := trace.SpanFromContext(ctx)
	start := time.Now()
	res, err := fn()
	if err != nil {
		// penalize plans that timeout from the upstream context
		if errors.Is(err, context.DeadlineExceeded) {
			selector.UpdateStats(strategy, r.upstreamTimeout)
		}
		telemetry.TraceError(span, err)
		return nil, err
	}
	selector.UpdateStats(strategy, time.Since(start))
	if res.GetAllowed() {
		span.SetAttributes(attribute.Bool("allowed", true))
	}
	return res, nil
}

func (r *Resolver) resolveRecursiveUserset(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map, canApplyOptimization bool) (*Response, error) {
	userObjectType, userRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	ctx, span := tracer.Start(ctx, "resolveRecursiveUserset",
		trace.WithAttributes(
			attribute.String("tuple_key", req.GetTupleString()),
			attribute.String("strategy", "default"),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	tIter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
		Object:   req.GetTupleKey().GetObject(),
		Relation: userRelation, // a user relation in a recursive userset is the same than the defined relation
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
			Type:               userObjectType,
			RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: userRelation},
		}},
		Conditions: edge.GetConditions(),
	}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{Preference: req.GetConsistency()}})
	if err != nil {
		return nil, err
	}
	defer tIter.Stop()

	iter := r.buildIterator(ctx, req, tIter, edge.GetConditions(), userRelation, edge.GetTo().GetUniqueLabel(), visited)
	if !canApplyOptimization {
		res, err := r.strategies[DefaultStrategyName].Userset(ctx, req, edge, iter, visited)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}
		if res.GetAllowed() {
			span.SetAttributes(attribute.Bool("allowed", true))
		}
		return res, nil
	}
	possibleStrategies := map[string]*planner.PlanConfig{
		DefaultStrategyName:   DefaultRecursivePlan,
		RecursiveStrategyName: RecursivePlan,
	}

	keyPlan := r.planner.GetPlanSelector(createRecursiveUsersetPlanKey(req, edge.GetTo().GetUniqueLabel()))
	strategy := keyPlan.Select(possibleStrategies)
	span.SetAttributes(attribute.Bool("allowed", true))
	return r.executeStrategy(ctx, keyPlan, strategy, func() (*Response, error) {
		return r.strategies[strategy.Name].Userset(ctx, req, edge, iter, visited)
	})
}

func (r *Resolver) resolveRecursiveTTU(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map, canApplyOptimization bool) (*Response, error) {
	_, tuplesetRelation := tuple.SplitObjectRelation(edge.GetTuplesetRelation())
	subjectType, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	ctx, span := tracer.Start(ctx, "resolveRecursiveTTU",
		trace.WithAttributes(
			attribute.String("tuple_key", req.GetTupleString()),
			attribute.String("tupleset_relation", tuple.ToObjectRelationString(req.GetObjectType(), tuplesetRelation)),
			attribute.String("computed_relation", computedRelation),
			attribute.String("strategy", "default"),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	conditionEdge, err := r.model.GetDirectEdgeFromNodeForUserType(edge.GetTuplesetRelation(), subjectType)
	if err != nil {
		return nil, errors.Join(ErrPanicRequest, err)
	}

	tIter, err := r.datastore.Read(
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
		telemetry.TraceError(span, err)
		return nil, err
	}

	defer tIter.Stop()
	iter := r.buildIterator(ctx, req, tIter, conditionEdge.GetConditions(), tuplesetRelation, subjectType, visited)

	if !canApplyOptimization {
		res, err := r.strategies[DefaultStrategyName].TTU(ctx, req, edge, iter, visited)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}
		if res.GetAllowed() {
			span.SetAttributes(attribute.Bool("allowed", true))
		}
		return res, nil
	}
	possibleStrategies := map[string]*planner.PlanConfig{
		DefaultStrategyName:   DefaultRecursivePlan,
		RecursiveStrategyName: RecursivePlan,
	}

	keyPlan := r.planner.GetPlanSelector(createRecursiveTTUPlanKey(req, edge.GetRecursiveRelation()))
	strategy := keyPlan.Select(possibleStrategies)
	span.SetAttributes(attribute.String("strategy", strategy.Name))
	return r.executeStrategy(ctx, keyPlan, strategy, func() (*Response, error) {
		return r.strategies[strategy.Name].TTU(ctx, req, edge, iter, visited)
	})
}

func (r *Resolver) ResolveRecursive(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map, canApplyOptimization bool) (*Response, error) {
	nonRecursiveEdges, err := r.model.FlattenNode(edge.GetTo(), req.GetUserType(), req.IsTypedWildcard(), true)
	if err != nil {
		return nil, errors.Join(ErrPanicRequest, err)
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
			res, err = r.resolveRecursiveUserset(ctx, req, edge, visited, canApplyOptimization)
		case authzGraph.TTUEdge:
			res, err = r.resolveRecursiveTTU(ctx, req, edge, visited, canApplyOptimization)
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

			if msg.Res.GetAllowed() {
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

	// errors will always be sent to the out channel
	var pool errgroup.Group
	pool.SetLimit(r.concurrencyLimit)
	defer func() {
		cancel()
		_ = pool.Wait()
		close(out)
	}()

	// in the case wildcard is requested if not all edges have wildcard path for the user type then return FALSE
	if req.IsTypedWildcard() {
		for _, edge := range edges {
			if !slices.Contains(edge.GetWildcards(), req.GetUserType()) {
				return &Response{Allowed: false}, nil
			}
		}
	}

	scheduledHandlers := 0
	for _, edge := range edges {
		_, ok := r.model.GetEdgeWeight(edge, req.GetUserType())
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
			if msg.Err != nil || !msg.Res.GetAllowed() {
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
	if _, ok := r.model.GetEdgeWeight(edges[0], req.GetUserType()); !ok {
		return nil, ErrPanicRequest
	}
	ctx, cancel := context.WithCancel(ctx)
	base := make(chan ResponseMsg, 1)
	var wg sync.WaitGroup

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
	_, ok = r.model.GetEdgeWeight(edges[1], req.GetUserType())
	if tuple.IsObjectRelation(req.GetTupleKey().GetUser()) && !ok {
		// If the user is an object relation and there is no way to the userset in the exclusion part we cannot have an answer,
		return nil, ErrUsersetInvalidRequest
	}

	if ok {
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
			if msg.Res.GetAllowed() && subtract == nil {
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
	case authzGraph.DirectLogicalEdge, authzGraph.TTULogicalEdge, authzGraph.ComputedEdge:
		return r.ResolveUnion(ctx, req, edge.GetTo(), visitedObjects)
	case authzGraph.TTUEdge:
		return r.ttu(ctx, req, edge, visitedObjects)
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
			// the request cannot have a wildcard if exclusion is involved
			if req.IsTypedWildcard() {
				return nil, ErrWildcardInvalidRequest
			}
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
			attribute.String("tuple_key", req.GetTupleString()),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	_, relation := tuple.SplitObjectRelation(edge.GetRelationDefinition())

	var t *openfgav1.TupleKey

	if ctxTuples, ok := req.GetContextualTuplesByUserID(req.GetTupleKey().GetUser(), relation, tuple.GetType(req.GetTupleKey().GetObject())); ok {
		i := sort.Search(len(ctxTuples), func(i int) bool {
			return ctxTuples[i].GetObject() >= req.GetTupleKey().GetObject()
		})
		if i < len(ctxTuples) && ctxTuples[i].GetObject() == req.GetTupleKey().GetObject() {
			t = ctxTuples[i]
		}
	}
	if t == nil {
		ut, err := r.datastore.ReadUserTuple(ctx, req.GetStoreID(), storage.ReadUserTupleFilter{
			Object:   req.GetTupleKey().GetObject(),
			Relation: relation,
			User:     req.GetTupleKey().GetUser(),
		}, storage.ReadUserTupleOptions{Consistency: storage.ConsistencyOptions{Preference: req.GetConsistency()}})
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return &Response{Allowed: false}, nil
			}
			telemetry.TraceError(span, err)
			return nil, err
		}
		t = ut.GetKey()
	}

	allowed, err := evaluateCondition(ctx, r.model, edge.GetConditions(), t, req.GetContext())
	if err != nil {
		telemetry.TraceError(span, err)
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
			attribute.String("tuple_key", req.GetTupleString()),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	_, relation := tuple.SplitObjectRelation(edge.GetRelationDefinition())
	var iter storage.TupleKeyIterator

	if ctxTuples, ok := req.GetContextualTuplesByObjectID(req.GetTupleKey().GetObject(), relation, req.GetUserType()); ok {
		for _, ct := range ctxTuples {
			if tuple.IsTypedWildcard(ct.GetUser()) {
				iter = storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{ct})
				break
			}
		}
	}

	if iter == nil {
		// Query via ReadUsersetTuples instead of ReadUserTuple tuples to take iterator cache.
		tIter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
			Object:                      req.GetTupleKey().GetObject(),
			Relation:                    relation,
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{modelgraph.WildcardRelationReference(req.GetUserType())},
			Conditions:                  edge.GetConditions(),
		}, storage.ReadUsersetTuplesOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: req.GetConsistency(),
			},
		})
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}

		defer tIter.Stop()

		iter = storage.NewTupleKeyIteratorFromTupleIterator(tIter)
	}
	t, err := iter.Next(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrIteratorDone) {
			return &Response{Allowed: false}, nil
		}
		telemetry.TraceError(span, err)
		return nil, err
	}

	allowed, err := evaluateCondition(ctx, r.model, edge.GetConditions(), t, req.GetContext())
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}
	if allowed {
		span.SetAttributes(attribute.Bool("allowed", true))
	}
	return &Response{Allowed: allowed}, nil
}

func (r *Resolver) specificTypeAndRelation(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "specificTypeAndRelation",
		trace.WithAttributes(
			attribute.String("tuple_key", req.GetTupleString()),
			attribute.String("strategy", "default"),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	// if the request is a userset then we need to execute the specific type
	if edge.GetTo().GetUniqueLabel() == req.GetUserType() {
		// we break here if there is an error, or the response is allowed, or if the edge is not recursive and it is not part of a tuple cycle
		// in case it is recursive or it is part of the tuple cycle it needs to continue expanding the graph
		res, err := r.specificType(ctx, req, edge)
		if err != nil || res.GetAllowed() || (edge.GetRecursiveRelation() == "" && !edge.IsPartOfTupleCycle()) {
			return res, err
		}
	}

	userObjectType, userRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
	_, relation := tuple.SplitObjectRelation(edge.GetRelationDefinition())

	// userset that is represented by the same edge, conditions on the iterator will be defined in this edge
	tIter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
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
		telemetry.TraceError(span, err)
		return nil, err
	}
	defer tIter.Stop()

	iter := r.buildIterator(ctx, req, tIter, edge.GetConditions(), relation, edge.GetTo().GetUniqueLabel(), visited)
	// when the request usertype is a userset, then only available strategy at the moment is default strategy
	if tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
		res, err := r.strategies[DefaultStrategyName].Userset(ctx, req, edge, iter, visited)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}
		if res.GetAllowed() {
			span.SetAttributes(attribute.Bool("allowed", true))
		}
		return res, nil
	}

	possibleStrategies := map[string]*planner.PlanConfig{
		DefaultStrategyName: DefaultPlan,
	}

	if w, _ := edge.GetWeight(req.GetUserType()); w == 2 {
		possibleStrategies[WeightTwoStrategyName] = weight2Plan
	}

	usersetKey := createUsersetPlanKey(req, edge.GetTo().GetUniqueLabel())
	keyPlan := r.planner.GetPlanSelector(usersetKey)
	strategy := keyPlan.Select(possibleStrategies)
	span.SetAttributes(attribute.String("strategy", strategy.Name))
	return r.executeStrategy(ctx, keyPlan, strategy, func() (*Response, error) {
		return r.strategies[strategy.Name].Userset(ctx, req, edge, iter, visited)
	})
}

func (r *Resolver) ttu(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	_, tuplesetRelation := tuple.SplitObjectRelation(edge.GetTuplesetRelation())
	subjectType, computedRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	ctx, span := tracer.Start(ctx, "ttu",
		trace.WithAttributes(
			attribute.String("tuple_key", req.GetTupleString()),
			attribute.String("tupleset_relation", tuple.ToObjectRelationString(req.GetObjectType(), tuplesetRelation)),
			attribute.String("computed_relation", computedRelation),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	// selecting the edge for the tupleset that points directly to the specific subjectType
	// the graph is already deduplicated by the conditions and combined in one unique edge for the same tupleset
	tuplesetEdge, err := r.model.GetDirectEdgeFromNodeForUserType(edge.GetTuplesetRelation(), subjectType)
	if err != nil {
		return nil, errors.Join(ErrPanicRequest, err)
	}

	tIter, err := r.datastore.Read(
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

	defer tIter.Stop()

	iter := r.buildIterator(ctx, req, tIter, tuplesetEdge.GetConditions(), tuplesetRelation, subjectType, visited)

	if tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
		res, err := r.strategies[DefaultStrategyName].TTU(ctx, req, edge, iter, visited)
		if err != nil {
			telemetry.TraceError(span, err)
			return nil, err
		}
		if res.GetAllowed() {
			span.SetAttributes(attribute.Bool("allowed", true))
		}
		return res, nil
	}

	possibleStrategies := map[string]*planner.PlanConfig{
		DefaultStrategyName: DefaultPlan,
	}

	if w, _ := edge.GetWeight(req.GetUserType()); w == 2 {
		possibleStrategies[WeightTwoStrategyName] = weight2Plan
	}

	planKey := createTTUPlanKey(req, tuplesetRelation, computedRelation)
	keyPlan := r.planner.GetPlanSelector(planKey)
	strategy := keyPlan.Select(possibleStrategies)
	span.SetAttributes(attribute.String("strategy", strategy.Name))
	return r.executeStrategy(ctx, keyPlan, strategy, func() (*Response, error) {
		return r.strategies[strategy.Name].TTU(ctx, req, edge, iter, visited)
	})
}

func (r *Resolver) buildIterator(ctx context.Context, req *Request, iter storage.TupleIterator, conditions []string, relation string, userType string, visited *sync.Map) storage.TupleKeyIterator {
	tupleKeyIter := storage.NewTupleKeyIteratorFromTupleIterator(iter)
	if ctxTuples, ok := req.GetContextualTuplesByObjectID(req.GetTupleKey().GetObject(), relation, userType); ok {
		tupleKeyIter = iterator.Concat(storage.NewStaticTupleKeyIterator(ctxTuples), tupleKeyIter)
	}

	iterFilters := make([]iterator.FilterFunc[*openfgav1.TupleKey], 0, 2)
	if visited != nil {
		iterFilters = append(iterFilters, BuildUniqueTupleKeyFilter(visited, func(key *openfgav1.TupleKey) string {
			return key.GetUser() // this is a userset (object#relation)
		}))
	}

	if len(conditions) > 1 || conditions[0] != authzGraph.NoCond {
		iterFilters = append(iterFilters, BuildConditionTupleKeyFilter(ctx, r.model, conditions, req.GetContext()))
	}

	if len(iterFilters) > 0 {
		return iterator.NewFilteredIterator(tupleKeyIter, iterFilters...)
	}

	return tupleKeyIter
}
