package check

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/check/metrics"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/internal/telemetry"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/tuple"
)

var tracer = otel.Tracer("internal/check")

var ErrValidation = errors.New("object relation does not exist")
var ErrUsersetInvalidRequest = errors.New("userset request cannot be resolved when exclusion operation is involved")
var ErrPanicRequest = errors.New("invalid check request")
var ErrWildcardInvalidRequest = errors.New("wildcard request cannot be resolved when intersection or exclusion is involved")

type LogicalEdge interface {
	Key(*Request) keys.Key
}

type GroupEdge struct {
	node  *graph.WeightedAuthorizationModelNode
	edges []*graph.WeightedAuthorizationModelEdge
}

func (e *GroupEdge) Explode() []LogicalEdge {
	logicalEdges := make([]LogicalEdge, 0, len(e.edges))

	for _, edge := range e.edges {
		logicalEdges = append(logicalEdges, (*SingleEdge)(edge))
	}
	return logicalEdges
}

func (e *GroupEdge) Key(req *Request) keys.Key {
	return NodeKey(req, e.node)
}

type SingleEdge graph.WeightedAuthorizationModelEdge

func (e *SingleEdge) Key(req *Request) keys.Key {
	return EdgeCacheKey(req, (*graph.WeightedAuthorizationModelEdge)(e))
}

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
	GroupStrategies           map[string]GroupStrategy
	EdgeStrategies            map[string]EdgeStrategy
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

	groupStrategies map[string]GroupStrategy
	edgeStrategies  map[string]EdgeStrategy
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
		groupStrategies:           cfg.GroupStrategies,
		edgeStrategies:            cfg.EdgeStrategies,
	}

	if r.cache == nil {
		r.cache = storage.NewNoopCache()
	}

	defaultStrategy := NewDefault(cfg.Model, r, cfg.ConcurrencyLimit)

	if r.edgeStrategies == nil {
		r.edgeStrategies = map[string]EdgeStrategy{
			DefaultStrategyName:   defaultStrategy,
			WeightTwoStrategyName: NewWeight2(cfg.Model, cfg.Datastore),
			RecursiveStrategyName: NewRecursive(cfg.Model, cfg.Datastore, cfg.ConcurrencyLimit),
		}
	}

	if r.groupStrategies == nil {
		r.groupStrategies = map[string]GroupStrategy{
			DefaultStrategyName: defaultStrategy,
			SQLStrategyName:     NewSQL(cfg.Model, cfg.Datastore),
		}
	}
	return r
}

func NodeKey(req *Request, node *graph.WeightedAuthorizationModelNode) keys.Key {
	const prefix = "NODE"

	builder := keys.GetBuilder()
	builder.EncodeString(prefix)
	builder.EncodeString(req.GetStoreID())
	builder.EncodeString(req.GetAuthorizationModelID())
	builder.EncodeString(node.GetUniqueLabel())
	builder.EncodeString(req.GetUserType())
	planKey := builder.Key()
	builder.Close()
	return planKey
}

func (r *Resolver) ResolveCheck(ctx context.Context, req *Request) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveCheck", trace.WithAttributes(
		attribute.String("consistency", req.GetConsistency().String()),
		attribute.String("store_id", req.GetStoreID()),
		attribute.String("model_id", req.GetAuthorizationModelID()),
		attribute.String("object", req.GetTupleKey().GetObject()),
		attribute.String("relation", req.GetTupleKey().GetRelation()),
		attribute.String("user", req.GetTupleKey().GetUser()),
		attribute.String("tuple_key", req.GetTupleString()),
		attribute.Bool("allowed", false),
	))
	defer span.End()

	defer func(ctx context.Context) {
		if err := ctx.Err(); err != nil {
			span.RecordError(err)
		}
	}(ctx)

	node, ok := r.model.GetNodeByID(tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation()))
	if !ok {
		// this should never happen as the request is already validated before
		span.AddEvent("invalid_node", trace.WithAttributes(
			attribute.String("node", tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation())),
		))
		span.SetStatus(codes.Error, "invalid node")
		return nil, ErrPanicRequest
	}

	// GetUserType returns the user type if the request in not an object relation otherwise the usertyperelation
	_, ok = r.model.GetNodeWeight(node, req.GetUserType())
	if !ok {
		// If the user type is not reachable from the object type and relation, we can immediately return false.
		span.AddEvent("user_type_not_reachable", trace.WithAttributes(
			attribute.String("node", node.GetUniqueLabel()),
			attribute.String("user_type", req.GetUserType()),
		))
		return &Response{Allowed: false}, nil
	}

	// Check if the user is a wildcard and if the node does not have a path to the user type wildcard then return false
	if req.IsTypedWildcard() && !slices.Contains(node.GetWildcards(), req.GetUserType()) {
		span.AddEvent("user_wildcard_not_reachable", trace.WithAttributes(
			attribute.String("node", node.GetUniqueLabel()),
			attribute.String("user_type", req.GetUserType()),
		))
		return &Response{Allowed: false}, nil
	}

	res, err := r.ResolveUnion(ctx, req, node, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	if res.GetAllowed() {
		span.SetAttributes(attribute.Bool("allowed", true))
	}

	return res, nil
}

func (r *Resolver) isCached(ctx context.Context, consistency openfgav1.ConsistencyPreference, key keys.Key) (res *Response, hit bool) {
	defer func() {
		if !hit {
			return
		}
		metrics.CacheHitCounter.Inc()

		span := trace.SpanFromContext(ctx)

		span.AddEvent("response_cache_hit", trace.WithAttributes(
			attribute.String("key", key.String()),
			attribute.Bool("allowed", res.GetAllowed()),
		))
	}()

	if consistency == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return
	}

	metrics.CacheLookupCounter.Inc()
	v := r.cache.Get(key)
	if v == nil {
		return
	}

	entry, ok := v.(*ResponseCacheEntry)
	if !ok {
		return
	}

	if !entry.LastModified.After(r.lastCacheInvalidationTime) {
		metrics.CacheInvalidHitCounter.Inc()
		return
	}
	res, hit = entry.Res, true
	return
}

const PrefixEdgeCacheKey = "EDGE"

// EdgeCacheKey builds a cache key that uniquely identifies a single edge
// evaluation within a check resolution. It incorporates the invariant hash
// so that requests with different contexts or contextual tuples never share
// cached edge results.
func EdgeCacheKey(req *Request, edge *graph.WeightedAuthorizationModelEdge) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(PrefixEdgeCacheKey)
	builder.EncodeString(req.GetStoreID())
	builder.EncodeString(req.GetAuthorizationModelID())
	builder.EncodeString(req.GetTupleKey().GetObject())
	builder.EncodeString(req.GetTupleKey().GetUser())
	builder.EncodeString(edge.GetRelationDefinition())
	builder.EncodeUint64(uint64(edge.GetEdgeType()))
	builder.EncodeString(edge.GetTo().GetUniqueLabel())
	builder.EncodeString(edge.GetTuplesetRelation())
	builder.EncodeUint64(req.GetInvariantCacheKey())
	return builder.Key()
}

func (r *Resolver) ResolveUnionEdges(ctx context.Context, req *Request, edges []LogicalEdge, visited *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveUnionEdges", trace.WithAttributes(
		attribute.String("tuple_key", req.GetTupleString()),
		attribute.Bool("allowed", false),
		attribute.Int("edge_count", len(edges)),
	))
	defer span.End()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	cacheKeys := make([]keys.Key, 0, len(edges))

	var i int

	// Check the cache for logical edge entries. Any existing entry with an "allowed" value of `true`
	// will result in an resolution short-circuit. When the "allowed" value is `false`, the logical edge
	// is removed from the logicalEdges slice. Any elements not having a cache entry are kept within
	// the logicalEdges slice for further processing.
	//
	// The function slices.DeleteFunc could have been used here similarly, but we need the ability to
	// short-circuit the entire function when a logical edge's cache entry has an "allowed" value of `true`.
	for j := 0; j < len(edges); j++ {
		edge := edges[j]

		key := edge.Key(req)

		if res, ok := r.isCached(ctx, req.GetConsistency(), key); ok {
			if res.GetAllowed() {
				return &Response{Allowed: true}, nil
			}
			continue
		}
		edges[i] = edge
		i++
		cacheKeys = append(cacheKeys, key)
	}
	clear(edges[i:])  // remove extraneous elements
	edges = edges[:i] // right-size the slice

	defer func(ctx context.Context) {
		if err := ctx.Err(); err != nil {
			span.RecordError(err)
		}
	}(ctx)

	ctx, cancel := context.WithCancel(ctx)

	out := make(chan ResponseMsg, len(edges))

	var pool errgroup.Group
	pool.SetLimit(r.concurrencyLimit)

	defer func() {
		cancel()
		pool.Wait()
	}()

	var expectedMessages int

	for i, edge := range edges {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		pool.Go(func() error {
			res, err := r.ResolveLogicalEdge(ctx, req, edge, visited)

			if err == nil {
				err = ctx.Err()
			}

			if err == nil {
				r.cache.Set(cacheKeys[i], &ResponseCacheEntry{Res: res, LastModified: time.Now()}, r.cacheTTL)
			}

			concurrency.TrySendThroughChannel(
				ctx,
				ResponseMsg{
					Res: res,
					Err: err,
				},
				out,
			)
			return nil
		})
		expectedMessages++
	}

	var err error
	for expectedMessages > 0 {
		// Force determinism when the context is canceled.
		// Without this check, on each iteration, the select
		// could continue to process messages buffered in out,
		// instead of taking the ctx.Done() branch even when
		// the context has already expired/canceled.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-out:
			expectedMessages--

			if msg.Err != nil {
				err = msg.Err
				span.RecordError(err)
				continue
			}

			if msg.Res.GetAllowed() {
				// Short-circuit: In a union, if any branch returns true, we can immediately return.
				span.SetAttributes(attribute.Bool("allowed", true))
				return msg.Res, nil
			}
		}
	}
	return nil, err
}

func (r *Resolver) SplitWeightOne(terminal string, edges ...*graph.WeightedAuthorizationModelEdge) ([]*graph.WeightedAuthorizationModelEdge, []*graph.WeightedAuthorizationModelEdge) {
	dst := make([]*graph.WeightedAuthorizationModelEdge, len(edges))
	copy(dst, edges)

	// left tracks first non-weight-1
	// right looks for the first weight 1 to swap with left

	var left int

	for right := range len(dst) {
		rightWeight, _ := r.model.GetEdgeWeight(dst[right], terminal)

		if rightWeight == 1 {
			dst[left], dst[right] = dst[right], dst[left]
			left++
		}
	}

	return dst[:left], dst[left:]
}

func (r *Resolver) GatherLogicalEdges(req *Request, node *graph.WeightedAuthorizationModelNode, edges []*graph.WeightedAuthorizationModelEdge) []LogicalEdge {
	if len(edges) == 1 {
		return []LogicalEdge{(*SingleEdge)(edges[0])}
	}

	if nodeWeight, _ := r.model.GetNodeWeight(node, req.GetUserType()); nodeWeight == 1 {
		return []LogicalEdge{&GroupEdge{node: node, edges: edges}}
	}

	weightOneEdges, weightTwoPlusEdges := r.SplitWeightOne(req.GetUserType(), edges...)

	logicalEdges := make([]LogicalEdge, 0, 1+len(weightTwoPlusEdges))

	if len(weightOneEdges) > 0 {
		logicalEdges = append(logicalEdges, &GroupEdge{node: node, edges: weightOneEdges})
	}

	for _, edge := range weightTwoPlusEdges {
		logicalEdges = append(logicalEdges, (*SingleEdge)(edge))
	}
	return logicalEdges
}

// reduce as a logical union operation (exit the moment we have a single true).
func (r *Resolver) ResolveUnion(ctx context.Context, req *Request, node *graph.WeightedAuthorizationModelNode, visited *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveUnion", trace.WithAttributes(
		attribute.String("tuple_key", req.GetTupleString()),
		attribute.String("node", node.GetUniqueLabel()),
	))
	defer span.End()

	defer func(ctx context.Context) {
		if err := ctx.Err(); err != nil {
			span.RecordError(err)
		}
	}(ctx)

	emptyCycle := visited == nil
	if emptyCycle && node.GetNodeType() == graph.SpecificTypeAndRelation && (node.GetRecursiveRelation() == node.GetUniqueLabel() || node.IsPartOfTupleCycle()) {
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

	// Group weight-one edges together and higher-weight edges separately.
	logicalEdges := r.GatherLogicalEdges(req, node, terminalEdges)

	return r.ResolveUnionEdges(ctx, req, logicalEdges, visited)
}

func (r *Resolver) executeStrategy(ctx context.Context, selector planner.Selector, strategy *planner.PlanConfig, fn func() (*Response, error)) (*Response, error) {
	span := trace.SpanFromContext(ctx)
	start := time.Now()
	res, err := fn()
	duration := time.Since(start)
	span.SetAttributes(attribute.Int64("strategy_duration_ms", duration.Milliseconds()))
	if err != nil {
		// penalize plans that timeout from the upstream context
		if errors.Is(err, context.DeadlineExceeded) {
			selector.UpdateStats(strategy, r.upstreamTimeout)
		}
		telemetry.TraceError(span, err)
		return nil, err
	}
	selector.UpdateStats(strategy, duration)
	if res.GetAllowed() {
		span.SetAttributes(attribute.Bool("allowed", true))
	}
	return res, nil
}

func (r *Resolver) resolveRecursiveUserset(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, visited *sync.Map, canApplyOptimization bool) (*Response, error) {
	userObjectType, userRelation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	ctx, span := tracer.Start(ctx, "resolveRecursiveUserset",
		trace.WithAttributes(
			attribute.String("tuple_key", req.GetTupleString()),
			attribute.String("strategy", "default"),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	allowedTypes := []*openfgav1.RelationReference{{
		Type:               userObjectType,
		RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: userRelation},
	}}
	tIter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
		Object:                      req.GetTupleKey().GetObject(),
		Relation:                    userRelation, // a user relation in a recursive userset is the same than the defined relation
		AllowedUserTypeRestrictions: allowedTypes,
		Conditions:                  edge.GetConditions(),
	}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{Preference: req.GetConsistency()}})
	if err != nil {
		return nil, err
	}
	defer tIter.Stop()

	iter := r.buildIterator(ctx, req, tIter, edge.GetConditions(), userRelation, edge.GetTo().GetUniqueLabel(), visited)
	if !canApplyOptimization {
		res, err := r.edgeStrategies[DefaultStrategyName].Userset(ctx, req, edge, iter, visited)
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

	planKey := createRecursiveUsersetPlanKey(req, edge.GetTo().GetUniqueLabel())
	keyPlan := r.planner.GetPlanSelector(planKey)
	strategy := keyPlan.Select(possibleStrategies)

	span.SetAttributes(
		attribute.Int64("edge.type", int64(edge.GetEdgeType())),
		attribute.String("edge.to", edge.GetTo().GetUniqueLabel()),
		attribute.String("edge.from", edge.GetFrom().GetUniqueLabel()),
		attribute.String("strategy", strategy.Name),
		attribute.Int("candidate_strategies", len(possibleStrategies)),
	)

	return r.executeStrategy(ctx, keyPlan, strategy, func() (*Response, error) {
		return r.edgeStrategies[strategy.Name].Userset(ctx, req, edge, iter, visited)
	})
}

func (r *Resolver) resolveRecursiveTTU(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, visited *sync.Map, canApplyOptimization bool) (*Response, error) {
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

	userFilter := subjectType + ":"
	tIter, err := r.datastore.Read(
		ctx,
		req.GetStoreID(),
		storage.ReadFilter{
			Object:     req.GetTupleKey().GetObject(),
			Relation:   tuplesetRelation,
			User:       userFilter,
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
		res, err := r.edgeStrategies[DefaultStrategyName].TTU(ctx, req, edge, iter, visited)
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

	planKey := createRecursiveTTUPlanKey(req, edge.GetRecursiveRelation())
	keyPlan := r.planner.GetPlanSelector(planKey)
	strategy := keyPlan.Select(possibleStrategies)

	span.SetAttributes(
		attribute.Int64("edge.type", int64(edge.GetEdgeType())),
		attribute.String("edge.to", edge.GetTo().GetUniqueLabel()),
		attribute.String("edge.from", edge.GetFrom().GetUniqueLabel()),
		attribute.String("edge.recursive_relation", edge.GetRecursiveRelation()),
		attribute.String("strategy", strategy.Name),
		attribute.Int("candidate_strategies", len(possibleStrategies)),
	)

	return r.executeStrategy(ctx, keyPlan, strategy, func() (*Response, error) {
		return r.edgeStrategies[strategy.Name].TTU(ctx, req, edge, iter, visited)
	})
}

func (r *Resolver) ResolveRecursive(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, visited *sync.Map, canApplyOptimization bool) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveRecursive", trace.WithAttributes(
		attribute.Int64("edge.type", int64(edge.GetEdgeType())),
		attribute.String("edge.to", edge.GetTo().GetUniqueLabel()),
		attribute.String("edge.from", edge.GetFrom().GetUniqueLabel()),
		attribute.String("tuple_key", req.GetTupleString()),
		attribute.Bool("allowed", false),
		attribute.Bool("can_apply_optimization", canApplyOptimization),
	))
	defer span.End()

	defer func(ctx context.Context) {
		if err := ctx.Err(); err != nil {
			span.RecordError(err)
		}
	}(ctx)

	node := edge.GetTo()

	nonRecursiveEdges, err := r.model.FlattenNode(node, req.GetUserType(), req.IsTypedWildcard(), true)
	if err != nil {
		return nil, errors.Join(ErrPanicRequest, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make(chan ResponseMsg, 2)

	go func() {
		logicalEdges := r.GatherLogicalEdges(req, node, nonRecursiveEdges)
		res, err := r.ResolveUnionEdges(ctx, req, logicalEdges, visited)
		concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, out)
	}()

	go func() {
		cacheKey := EdgeCacheKey(req, edge)

		if res, ok := r.isCached(ctx, req.GetConsistency(), cacheKey); ok {
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res}, out)
			return
		}

		var err error
		var res *Response

		switch edge.GetEdgeType() {
		case graph.DirectEdge:
			res, err = r.resolveRecursiveUserset(ctx, req, edge, visited, canApplyOptimization)
		case graph.TTUEdge:
			res, err = r.resolveRecursiveTTU(ctx, req, edge, visited, canApplyOptimization)
		default:
			res, err = nil, ErrPanicRequest
		}

		if err == nil && ctx.Err() == nil {
			entry := &ResponseCacheEntry{Res: res, LastModified: time.Now()}
			r.cache.Set(cacheKey, entry, r.cacheTTL)
		}
		concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, out)
	}()

	for range 2 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-out:
			if msg.Err != nil {
				err = msg.Err
				continue
			}

			if msg.Res.GetAllowed() {
				span.SetAttributes(attribute.Bool("allowed", true))
				return msg.Res, nil
			}
		}
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return &Response{Allowed: false}, err
}

func (r *Resolver) ResolveIntersectionEdges(ctx context.Context, req *Request, edges []LogicalEdge) (*Response, error) {
	ctx, cancel := context.WithCancel(ctx)

	out := make(chan ResponseMsg, len(edges))

	// errors will always be sent to the out channel
	var pool errgroup.Group
	pool.SetLimit(r.concurrencyLimit)

	defer func() {
		cancel()
		pool.Wait()
	}()

	var expectedMessages int

	for _, edge := range edges {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		pool.Go(func() error {
			// intersection is never part of a cycle or recursion
			res, err := r.ResolveLogicalEdge(ctx, req, edge, nil)
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, out)
			return nil
		})
		expectedMessages++
	}

	for expectedMessages > 0 {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-out:
			expectedMessages--

			if msg.Err != nil || !msg.Res.GetAllowed() {
				// NOTE: This is one of the breaking changes from the current check implementation. Delete this after this rollout.
				// In intersection _every_ branch must return true.
				return msg.Res, msg.Err
			}
		}
	}
	return &Response{Allowed: true}, nil
}

// reduce as a logical intersection operation (exit the moment we have a single false)
// should panic if a single handler returns nil.
func (r *Resolver) ResolveIntersection(ctx context.Context, req *Request, node *graph.WeightedAuthorizationModelNode) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveIntersection", trace.WithAttributes(
		attribute.String("tuple_key", req.GetTupleString()),
		attribute.Bool("allowed", false),
	))
	defer span.End()

	edges, ok := r.model.GetEdgesFromNode(node)
	if !ok {
		return nil, ErrPanicRequest
	}

	for _, edge := range edges {
		if _, ok := r.model.GetEdgeWeight(edge, req.GetUserType()); !ok {
			return nil, ErrPanicRequest
		}

		// in the case wildcard is requested if not all edges have wildcard path for the user type then return FALSE
		if req.IsTypedWildcard() && !slices.Contains(edge.GetWildcards(), req.GetUserType()) {
			return &Response{Allowed: false}, nil
		}
	}

	logicalEdges := r.GatherLogicalEdges(req, node, edges)

	return r.ResolveIntersectionEdges(ctx, req, logicalEdges)
}

func (r *Resolver) ResolveExclusionEdges(ctx context.Context, req *Request, edges []LogicalEdge) (*Response, error) {
	ctx, cancel := context.WithCancel(ctx)

	span := trace.SpanFromContext(ctx)

	var pool errgroup.Group
	pool.SetLimit(2)

	defer func() {
		cancel()
		pool.Wait()
	}()

	chBase := make(chan ResponseMsg, 1)
	baseEdge := edges[0]

	edges = edges[1:]

	pool.Go(func() error {
		res, err := r.ResolveLogicalEdge(ctx, req, baseEdge, nil)
		concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, chBase)
		close(chBase)
		return nil
	})

	var chSubtract chan ResponseMsg

	if len(edges) > 0 {
		chSubtract = make(chan ResponseMsg, 1)
		subtractEdge := edges[0]

		pool.Go(func() error {
			res, err := r.ResolveLogicalEdge(ctx, req, subtractEdge, nil)
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, chSubtract)
			close(chSubtract)
			return nil
		})
	}

	for chBase != nil || chSubtract != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-chBase:
			if !ok {
				chBase = nil // Stop selecting this case.
				continue
			}

			if msg.Err != nil {
				// NOTE: This is one of the breaking changes from the current check implementation. Delete this after this rollout.
				// If base returns an error, we return it immediately since the result of the exclusion cannot be determined.
				return nil, msg.Err
			}

			// Short-circuit: If base is false, the whole expression is false.
			if !msg.Res.GetAllowed() {
				return &Response{Allowed: false}, nil
			}

			if msg.Res.GetAllowed() && chSubtract == nil {
				span.SetAttributes(attribute.Bool("allowed", true))
				return msg.Res, nil
			}
		case msg, ok := <-chSubtract:
			if !ok {
				chSubtract = nil
				continue
			}

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
	span.SetAttributes(attribute.Bool("allowed", true))
	return &Response{Allowed: true}, nil
}

// reduce as a logical exclusion operation
// if base is false, short circuit.
func (r *Resolver) ResolveExclusion(ctx context.Context, req *Request, node *graph.WeightedAuthorizationModelNode) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveExclusion", trace.WithAttributes(
		attribute.String("tuple_key", req.GetTupleString()),
		attribute.Bool("allowed", false),
	))
	defer span.End()

	edges, ok := r.model.GetEdgesFromNode(node)
	if !ok {
		return nil, ErrPanicRequest
	}

	// base edge validation
	if _, ok := r.model.GetEdgeWeight(edges[0], req.GetUserType()); !ok {
		return nil, ErrPanicRequest
	}

	// subtract edge validation
	_, subtractHasPath := r.model.GetEdgeWeight(edges[1], req.GetUserType())
	if !subtractHasPath {
		if tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
			// If the user is an object relation and there is no way to the userset in the exclusion part we
			// cannot have an answer, since the system does not perform an exhaustive search to verify if
			// individual users of the userset have the relation to the object.
			return nil, ErrUsersetInvalidRequest
		}
		edges = edges[:1]
	}

	logicalEdges := r.GatherLogicalEdges(req, node, edges)

	return r.ResolveExclusionEdges(ctx, req, logicalEdges)
}

func (r *Resolver) ResolveEdge(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveEdge", trace.WithAttributes(
		attribute.String("tuple_key", req.GetTupleString()),
		attribute.String("edge.to", edge.GetTo().GetUniqueLabel()),
		attribute.String("edge.from", edge.GetFrom().GetUniqueLabel()),
	))
	defer span.End()

	defer func(ctx context.Context) {
		if err := ctx.Err(); err != nil {
			span.RecordError(err)
		}
	}(ctx)

	var visitedObjects *sync.Map
	if edge.IsPartOfTupleCycle() || edge.GetRecursiveRelation() != "" {
		visitedObjects = visited
	}
	// computed edges are solved by the relation node caller
	switch edge.GetEdgeType() {
	case graph.DirectEdge:
		switch edge.GetTo().GetNodeType() {
		case graph.SpecificType:
			// terminal types are never part of a cycle
			return r.specificType(ctx, req, edge)
		case graph.SpecificTypeWildcard:
			// terminal types are never part of a cycle
			return r.specificTypeWildcard(ctx, req, edge)
		case graph.SpecificTypeAndRelation:
			// check for recursiveRelation
			return r.specificTypeAndRelation(ctx, req, edge, visitedObjects)
		default:
			return nil, ErrPanicRequest
		}
	case graph.DirectLogicalEdge, graph.TTULogicalEdge, graph.ComputedEdge:
		return r.ResolveUnion(ctx, req, edge.GetTo(), visitedObjects)
	case graph.TTUEdge:
		return r.ttu(ctx, req, edge, visitedObjects)
	case graph.RewriteEdge:
		return r.ResolveRewrite(ctx, req, edge.GetTo(), visitedObjects)
	default:
		return nil, ErrPanicRequest
	}
}

func (r *Resolver) ResolveLogicalEdge(ctx context.Context, req *Request, logicalEdge LogicalEdge, visited *sync.Map) (*Response, error) {
	if singleEdge, ok := logicalEdge.(*SingleEdge); ok {
		return r.ResolveEdge(ctx, req, (*graph.WeightedAuthorizationModelEdge)(singleEdge), visited)
	}

	groupEdge := logicalEdge.(*GroupEdge)

	if r.datastore.Builder(req.Consistency) == nil {
		switch groupEdge.node.GetNodeType() {
		case graph.SpecificTypeAndRelation:
			return r.groupStrategies[DefaultPlan.Name].Union(ctx, req, groupEdge)
		case graph.OperatorNode:
			switch groupEdge.node.GetLabel() {
			case graph.UnionOperator:
				return r.groupStrategies[DefaultPlan.Name].Union(ctx, req, groupEdge)
			case graph.IntersectionOperator:
				return r.groupStrategies[DefaultPlan.Name].Intersection(ctx, req, groupEdge)
			case graph.ExclusionOperator:
				return r.groupStrategies[DefaultPlan.Name].Exclusion(ctx, req, groupEdge)
			default:
				panic("unknown operator node type")
			}
		default:
			return nil, ErrPanicRequest
		}
	}

	planKey := groupEdge.Key(req)
	selector := r.planner.GetPlanSelector(planKey)

	candidates := map[string]*planner.PlanConfig{
		DefaultStrategyName: DefaultPlan,
		SQLStrategyName:     SQLPlan,
	}

	plan := selector.Select(candidates)

	return r.executeStrategy(ctx, selector, plan, func() (*Response, error) {
		switch groupEdge.node.GetNodeType() {
		case graph.SpecificTypeAndRelation:
			return r.groupStrategies[plan.Name].Union(ctx, req, groupEdge)
		case graph.OperatorNode:
			switch groupEdge.node.GetLabel() {
			case graph.UnionOperator:
				return r.groupStrategies[plan.Name].Union(ctx, req, groupEdge)
			case graph.IntersectionOperator:
				return r.groupStrategies[plan.Name].Intersection(ctx, req, groupEdge)
			case graph.ExclusionOperator:
				return r.groupStrategies[plan.Name].Exclusion(ctx, req, groupEdge)
			default:
				panic("unknown operator node type")
			}
		default:
			return nil, ErrPanicRequest
		}
	})
}

func (r *Resolver) ResolveRewrite(ctx context.Context, req *Request, node *graph.WeightedAuthorizationModelNode, visited *sync.Map) (*Response, error) {
	// relation and union have save behavior
	switch node.GetNodeType() {
	case graph.SpecificTypeAndRelation:
		return r.ResolveUnion(ctx, req, node, visited)
	case graph.OperatorNode:
		switch node.GetLabel() {
		case graph.UnionOperator:
			return r.ResolveUnion(ctx, req, node, visited)
		case graph.IntersectionOperator:
			// intersection is never part of a graph cycle
			return r.ResolveIntersection(ctx, req, node)
		case graph.ExclusionOperator:
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

func (r *Resolver) specificType(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge) (*Response, error) {
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

func (r *Resolver) specificTypeWildcard(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge) (*Response, error) {
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
		// Query via ReadUsersetTuples - caching is now handled by CachedTupleReader wrapper
		allowedTypes := []*openfgav1.RelationReference{modelgraph.WildcardRelationReference(req.GetUserType())}
		tIter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
			Object:                      req.GetTupleKey().GetObject(),
			Relation:                    relation,
			AllowedUserTypeRestrictions: allowedTypes,
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

func (r *Resolver) specificTypeAndRelation(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
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
	allowedTypes := []*openfgav1.RelationReference{{
		Type:               userObjectType,
		RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: userRelation},
	}}
	tIter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
		Object:                      req.GetTupleKey().GetObject(),
		Relation:                    relation,
		AllowedUserTypeRestrictions: allowedTypes,
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

	iter := r.buildIterator(ctx, req, tIter, edge.GetConditions(), relation, edge.GetTo().GetUniqueLabel(), visited)
	// when the request usertype is a userset, then only available strategy at the moment is default strategy
	if tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
		res, err := r.edgeStrategies[DefaultStrategyName].Userset(ctx, req, edge, iter, visited)
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
	span.SetAttributes(
		attribute.Int64("edge.type", int64(edge.GetEdgeType())),
		attribute.String("edge.to", edge.GetTo().GetUniqueLabel()),
		attribute.String("edge.from", edge.GetFrom().GetUniqueLabel()),
		attribute.String("strategy", strategy.Name),
		attribute.Int("candidate_strategies", len(possibleStrategies)),
	)
	return r.executeStrategy(ctx, keyPlan, strategy, func() (*Response, error) {
		return r.edgeStrategies[strategy.Name].Userset(ctx, req, edge, iter, visited)
	})
}

func (r *Resolver) ttu(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, visited *sync.Map) (*Response, error) {
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

	userFilter := subjectType + ":"
	tIter, err := r.datastore.Read(
		ctx,
		req.GetStoreID(),
		storage.ReadFilter{
			Object:     req.GetTupleKey().GetObject(),
			Relation:   tuplesetRelation,
			User:       userFilter,
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
		res, err := r.edgeStrategies[DefaultStrategyName].TTU(ctx, req, edge, iter, visited)
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

	span.SetAttributes(
		attribute.Int64("edge.type", int64(edge.GetEdgeType())),
		attribute.String("edge.to", edge.GetTo().GetUniqueLabel()),
		attribute.String("edge.from", edge.GetFrom().GetUniqueLabel()),
		attribute.String("tupleset_relation", tuplesetRelation),
		attribute.String("computed_relation", computedRelation),
		attribute.String("strategy", strategy.Name),
		attribute.Int("candidate_strategies", len(possibleStrategies)),
	)

	return r.executeStrategy(ctx, keyPlan, strategy, func() (*Response, error) {
		return r.edgeStrategies[strategy.Name].TTU(ctx, req, edge, iter, visited)
	})
}

func (r *Resolver) buildIterator(ctx context.Context, req *Request, iter storage.TupleIterator, conditions []string, relation string, userType string, visited *sync.Map) storage.TupleKeyIterator {
	// Note: Iterator caching is now handled by CachedTupleReader wrapper at the storage layer.
	// This method only handles contextual tuples merge and condition filtering.

	// STEP 1: Convert TupleIterator to TupleKeyIterator
	tupleKeyIter := storage.NewTupleKeyIteratorFromTupleIterator(iter)

	// STEP 2: Merge contextual tuples (these are request-specific and not cached)
	if ctxTuples, ok := req.GetContextualTuplesByObjectID(req.GetTupleKey().GetObject(), relation, userType); ok {
		tupleKeyIter = iterator.Concat(storage.NewStaticTupleKeyIterator(ctxTuples), tupleKeyIter)
	}

	// STEP 3: Build filter chain
	iterFilters := make([]iterator.FilterFunc[*openfgav1.TupleKey], 0, 2)
	if visited != nil {
		iterFilters = append(iterFilters, BuildUniqueTupleKeyFilter(visited, func(key *openfgav1.TupleKey) string {
			return key.GetUser() // this is a userset (object#relation)
		}))
	}

	// STEP 4: Condition filter - evaluates conditions at retrieval time
	// This uses the cached tuple's condition context + request context
	if len(conditions) > 1 || conditions[0] != graph.NoCond {
		iterFilters = append(iterFilters, BuildConditionTupleKeyFilter(ctx, r.model, conditions, req.GetContext()))
	}

	if len(iterFilters) > 0 {
		return iterator.NewFilteredIterator(tupleKeyIter, iterFilters...)
	}

	return tupleKeyIter
}
