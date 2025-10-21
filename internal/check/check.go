package check

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
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
	Model              *AuthorizationModelGraph
	Datastore          storage.RelationshipTupleReader
	Planner            *planner.Planner
	MaxResolutionDepth int32
	ConcurrencyLimit   int
	UpstreamTimeout    time.Duration
	Logger             logger.Logger
	Strategies         map[string]Strategy
}
type Resolver struct {
	model              *AuthorizationModelGraph
	datastore          storage.RelationshipTupleReader
	planner            *planner.Planner
	maxResolutionDepth int32
	concurrencyLimit   int
	upstreamTimeout    time.Duration
	logger             logger.Logger

	depthCount atomic.Int32
	strategies map[string]Strategy
}

type Request = graph.ResolveCheckRequest // TODO: Once we finish migrating, move into this package and drop useless fields (VisitedPaths, RequestMetadata)
type Response = graph.ResolveCheckResponse

type RequestParams = graph.ResolveCheckRequestParams

func NewRequest(p RequestParams) (*Request, error) {
	return graph.NewResolveCheckRequest(p)
}

var ErrResolutionDepthExceeded = graph.ErrResolutionDepthExceeded
var ErrPanicRequest = errors.New("invalid check request") // == panic in ResolveCheck so should be handled accordingly (should be seen as a 500 to client)

type ResponseMsg struct {
	Res *Response
	Err error
}

func New(cfg Config) *Resolver {
	r := &Resolver{
		model:              cfg.Model,
		datastore:          cfg.Datastore,
		planner:            cfg.Planner,
		maxResolutionDepth: cfg.MaxResolutionDepth,
		concurrencyLimit:   cfg.ConcurrencyLimit,
		upstreamTimeout:    cfg.UpstreamTimeout,
		logger:             cfg.Logger,
		strategies:         cfg.Strategies,
	}

	return r
}

func (r *Resolver) ResolveCheck(ctx context.Context, req *Request) (*Response, error) {
	ctx, span := tracer.Start(ctx, "ResolveCheck", trace.WithAttributes(
		attribute.String("store_id", req.GetStoreID()),
		attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(req.GetTupleKey())),
	))
	defer span.End()

	if r.depthCount.Load() >= r.maxResolutionDepth {
		return nil, ErrResolutionDepthExceeded
	}

	// TODO: Handle where User is a userset (model would dynamically compute weight in order to prune branches, needs work in language)

	// TODO: While we are doing the rollout, ok should never be false due it being caught by the validation in the command layer via `validateCheckRequest`.
	// Once the rollout is done, we should swap the existing implementation with this which is much more efficient.
	node, _ := r.model.GetNodeByID(tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation()))

	_, ok := node.GetWeight(req.GetObjectType())
	if !ok {
		// If the user type is not reachable from the object type and relation, we can immediately return false.
		return &Response{Allowed: false}, nil
	}

	return r.ResolveUnion(ctx, req, node)
}

// reduce as a logical union operation (exit the moment we have a single true).
func (r *Resolver) ResolveUnion(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode) (*Response, error) {
	edges, ok := r.model.GetEdgesFromNode(node)
	if !ok {
		return nil, ErrPanicRequest
	}

	ctx, cancel := context.WithCancel(ctx)

	if len(edges) == 1 && (edges[0].GetEdgeType() == authzGraph.ComputedEdge) {
		edges, _ = r.model.GetEdgesFromNode(edges[0].GetTo())
	}

	if node.GetNodeType() == authzGraph.SpecificTypeAndRelation && node.GetRecursiveRelation() == node.GetUniqueLabel() {
		// if we are at a recursive relation, need to apply strategy to apply (via planner if available)
	}

	out := make(chan ResponseMsg, len(edges))
	var pool errgroup.Group
	pool.SetLimit(r.concurrencyLimit)
	defer func() {
		cancel()
		_ = pool.Wait()
		close(out)
	}()

	terminalEdges, err := r.model.FlattenNode(node, req.GetUserType())
	if err != nil {
		return nil, ErrPanicRequest
	}

	scheduledHandlers := 0
	for _, edge := range terminalEdges {
		scheduledHandlers++
		pool.Go(func() error {
			res, err := r.ResolveEdge(ctx, req, edge)
			concurrency.TrySendThroughChannel(ctx, ResponseMsg{Res: res, Err: err}, out)
			return nil
		})
	}

	for i := 0; i < scheduledHandlers; i++ {
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
			res, err := r.ResolveEdge(ctx, req, edge)
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
		res, err := r.ResolveEdge(ctx, req, edges[0])
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
			res, err := r.ResolveEdge(ctx, req, edges[1])
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

func (r *Resolver) ResolveEdge(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) (*Response, error) {
	// computed edges are solved by the relation node caller
	switch edge.GetEdgeType() {
	case authzGraph.DirectEdge:
		switch edge.GetTo().GetNodeType() {
		case authzGraph.SpecificType:
			return r.specificType(ctx, req, edge)
		case authzGraph.SpecificTypeWildcard:
			return r.specificTypeWildcard(ctx, req, edge)
		case authzGraph.SpecificTypeAndRelation:
			// check for recursiveRelation
			return r.specificTypeAndRelation(ctx, req, edge)
		default:
			return nil, ErrPanicRequest
		}
	case authzGraph.DirectLogicalEdge:
		return r.ResolveUnion(ctx, req, edge.GetTo())
	case authzGraph.TTUEdge:
		return r.ttu(ctx, req, edge)
	case authzGraph.TTULogicalEdge:
		return r.ResolveUnion(ctx, req, edge.GetTo())
	case authzGraph.RewriteEdge:
		return r.ResolveRewrite(ctx, req, edge.GetTo())
	default:
		return nil, ErrPanicRequest
	}
}

func (r *Resolver) ResolveRewrite(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode) (*Response, error) {
	// relation and union have save behavior
	switch node.GetNodeType() {
	case authzGraph.SpecificTypeAndRelation, authzGraph.LogicalDirectGrouping, authzGraph.LogicalTTUGrouping:
		return r.ResolveUnion(ctx, req, node)
	case authzGraph.OperatorNode:
		switch node.GetLabel() {
		case authzGraph.UnionOperator:
			return r.ResolveUnion(ctx, req, node)
		case authzGraph.IntersectionOperator:
			return r.ResolveIntersection(ctx, req, node)
		case authzGraph.ExclusionOperator:
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

	opts := storage.ReadUserTupleOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}

	t, err := r.datastore.ReadUserTuple(ctx, req.GetStoreID(), req.GetTupleKey(), opts)
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

	opts := storage.ReadUsersetTuplesOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}

	// Query via ReadUsersetTuples instead of ReadUserTuple tuples to take iterator cache.
	iter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
		Object:                      req.GetTupleKey().GetObject(),
		Relation:                    req.GetTupleKey().GetRelation(),
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{modelUtils.WildcardRelationReference(req.GetTupleKey().GetUser())},
	}, opts)
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
func (r *Resolver) specificTypeAndRelation(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) (*Response, error) {
	ctx, span := tracer.Start(ctx, "specificTypeAndRelation",
		trace.WithAttributes(
			attribute.String("tuple_key", tuple.TupleKeyWithConditionToString(req.GetTupleKey())),
			attribute.Bool("allowed", false),
		),
	)
	defer span.End()

	opts := storage.ReadUsersetTuplesOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}

	objectType, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())

	iter, err := r.datastore.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
		Object:   req.GetTupleKey().GetObject(),
		Relation: req.GetTupleKey().GetRelation(),
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
			Type:               objectType,
			RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: relation},
		}},
	}, opts)
	if err != nil {
		return nil, err
	}
	defer iter.Stop()

	conditionEdge, err := r.model.GetConditionsEdgeForUserType(tuple.ToObjectRelationString(req.GetObjectType(), req.GetTupleKey().GetRelation()), edge.GetTo().GetUniqueLabel())
	if err != nil {
		return nil, ErrPanicRequest
	}

	i := storage.NewTupleKeyIteratorFromTupleIterator(iter)
	if len(conditionEdge.GetConditions()) > 1 || conditionEdge.GetConditions()[0] != "" {
		i = storage.NewConditionsFilteredTupleKeyIterator(i,
			BuildTupleKeyConditionFilter(ctx, r.model, conditionEdge, req.GetContext()),
		)
	}

	// TODO: Need optimization to solve userset as principal
	if tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
		return r.strategies[DefaultStrategyName].Userset(ctx, req, edge, i)
	}

	var b strings.Builder
	b.WriteString("v2|") // TODO: drop versioning when we are sure of no breaking changes
	b.WriteString("userset|")
	b.WriteString(req.GetAuthorizationModelID())
	b.WriteString("|")
	b.WriteString(req.GetObjectType())
	b.WriteString("|")
	b.WriteString(req.GetTupleKey().GetRelation())
	b.WriteString("|")
	b.WriteString(req.GetUserType())
	b.WriteString("|userset|")
	b.WriteString(edge.GetTo().GetUniqueLabel())

	possibleStrategies := map[string]*planner.KeyPlanStrategy{
		DefaultStrategyName: DefaultPlan,
	}

	if w, _ := edge.GetWeight(req.GetUserType()); w == 2 {
		possibleStrategies[WeightTwoStrategyName] = weight2Plan
	}

	keyPlan := r.planner.GetKeyPlan(b.String())
	strategy := keyPlan.SelectStrategy(possibleStrategies)

	start := time.Now()
	res, err := r.strategies[strategy.Type].Userset(ctx, req, edge, i)
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

func (r *Resolver) ttu(ctx context.Context, req *Request, edge *authzGraph.WeightedAuthorizationModelEdge) (*Response, error) {
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

	opts := storage.ReadOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}

	iter, err := r.datastore.Read(
		ctx,
		req.GetStoreID(),
		tuple.NewTupleKey(req.GetTupleKey().GetObject(), tuplesetRelation, subjectType+":"), // TODO: Read doesn't support passing subjectType with objectID and relation
		opts,
	)
	if err != nil {
		return nil, err
	}

	defer iter.Stop()
	conditionEdge, err := r.model.GetConditionsEdgeForUserType(tuplesetRelation, subjectType)
	if err != nil {
		return nil, ErrPanicRequest
	}

	i := storage.NewTupleKeyIteratorFromTupleIterator(iter)
	if len(conditionEdge.GetConditions()) > 1 || conditionEdge.GetConditions()[0] != "" {
		i = storage.NewConditionsFilteredTupleKeyIterator(i,
			BuildTupleKeyConditionFilter(ctx, r.model, conditionEdge, req.GetContext()),
		)
	}

	// TODO: Need optimization to solve userset as principal
	if tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
		return r.strategies[DefaultStrategyName].TTU(ctx, req, edge, i)
	}

	possibleStrategies := map[string]*planner.KeyPlanStrategy{
		DefaultStrategyName: DefaultPlan,
	}

	if w, _ := edge.GetWeight(req.GetUserType()); w == 2 {
		possibleStrategies[WeightTwoStrategyName] = weight2Plan
	}

	var b strings.Builder
	b.WriteString("v2|") // TODO: drop versioning when we are sure of no breaking changes
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
	planKey := b.String()

	keyPlan := r.planner.GetKeyPlan(planKey)
	strategy := keyPlan.SelectStrategy(possibleStrategies)

	start := time.Now()
	res, err := r.strategies[strategy.Type].TTU(ctx, req, edge, i)
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
