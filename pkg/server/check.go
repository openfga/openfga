package server

import (
	"context"
	"errors"
	"strconv"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sourcegraph/conc/panics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/telemetry"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/middleware/requestid"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func (s *Server) Check(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	const methodName = "check"

	startTime := time.Now()

	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, apimethod.Check.String(), trace.WithAttributes(
		attribute.KeyValue{Key: "store_id", Value: attribute.StringValue(req.GetStoreId())},
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
		attribute.KeyValue{Key: "consistency", Value: attribute.StringValue(req.GetConsistency().String())},
	))
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  apimethod.Check.String(),
	})

	err := s.checkAuthz(ctx, req.GetStoreId(), apimethod.Check)
	if err != nil {
		return nil, err
	}

	storeID := req.GetStoreId()

	if s.featureFlagClient.Boolean(serverconfig.ExperimentalWeightedGraphCheck, storeID) {
		res, err := s.v2Check(ctx, req, s.sharedDatastoreResources.CheckCache, s.sharedDatastoreResources.CacheController, s.authzModelGraphResolver)

		// v2Check can return errors that v1 Check wouldn't (e.g. ErrInvalidModel when the weighted graph
		// can't represent the model). Fallback to v1 on non-timeout errors for backward compatibility.
		if err == nil || commands.IsV2CheckTerminalError(err) {
			tookMs := time.Since(startTime).Milliseconds()

			if res != nil {
				queryCount := float64(res.DatastoreQueryCount)
				itemCount := float64(res.DatastoreItemCount)

				grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, queryCount)
				span.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, queryCount))
				datastoreQueryCountHistogram.WithLabelValues(s.serviceName, methodName).Observe(queryCount)

				grpc_ctxtags.Extract(ctx).Set(datastoreItemCountHistogramName, itemCount)
				span.SetAttributes(attribute.Float64(datastoreItemCountHistogramName, itemCount))
				datastoreItemCountHistogram.WithLabelValues(s.serviceName, methodName).Observe(itemCount)

				requestDurationHistogram.WithLabelValues(
					s.serviceName,
					methodName,
					utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
					utils.Bucketize(0, s.requestDurationByDispatchCountHistogramBuckets),
					req.GetConsistency().String(),
				).Observe(float64(tookMs))

				if s.authorizer.AccessControlStoreID() == req.GetStoreId() {
					accessControlStoreCheckDurationHistogram.WithLabelValues(
						utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
						utils.Bucketize(0, s.requestDurationByDispatchCountHistogramBuckets),
						req.GetConsistency().String(),
					).Observe(float64(tookMs))
				}

				if res.WasThrottled {
					throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDatastore).Inc()
				}
				grpc_ctxtags.Extract(ctx).Set("request.datastore_throttled", res.WasThrottled)
			}

			if err != nil {
				telemetry.TraceError(span, err)
				if _, ok := status.FromError(err); !ok {
					err = commands.CheckCommandErrorToServerError(err)
				}
				return nil, err
			}

			checkResultCounter.With(prometheus.Labels{allowedLabel: strconv.FormatBool(res.Allowed)}).Inc()
			span.SetAttributes(attribute.Bool("allowed", res.Allowed))

			// Flag potential v2Check resolution breaking changes for userset requests.
			// See breakingChangeReason for the scenarios we detect.
			if !res.Allowed && tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
				if typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId()); err == nil {
					tk := req.GetTupleKey()
					if reason := breakingChangeReason(typesys, tk); reason != "" {
						requestID := requestid.GetRequestIDFromContext(ctx)
						s.logger.WarnWithContext(ctx, "potential v2Check resolution breaking change: userset request returned false",
							zap.String("store_id", storeID),
							zap.String("model_id", req.GetAuthorizationModelId()),
							zap.String("request_id", requestID),
							zap.String("reason", reason),
						)
					}
				}
			}

			return &openfgav1.CheckResponse{Allowed: res.Allowed}, nil
		}

		requestID := requestid.GetRequestIDFromContext(ctx)
		s.logger.WarnWithContext(ctx, "Weighted graph check failed, falling back",
			zap.Error(err),
			zap.String("store_id", storeID),
			zap.String("model_id", req.GetAuthorizationModelId()),
			zap.String("request_id", requestID),
		)

		startTime = time.Now() // reset startTime to avoid counting v2Check duration in case of fallback when it's enabled
	}

	// v1 Check

	checkResolver, checkResolverCloser, err := s.getCheckResolverBuilder(storeID).Build()
	if err != nil {
		return nil, err
	}
	defer checkResolverCloser()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}
	req.AuthorizationModelId = typesys.GetAuthorizationModelID() // the resolved model id

	checkQuery := commands.NewCheckCommand(
		s.datastore,
		checkResolver,
		typesys,
		commands.WithCheckCommandLogger(s.logger),
		commands.WithCheckCommandMaxConcurrentReads(s.maxConcurrentReadsForCheck),
		commands.WithCheckCommandCache(s.sharedDatastoreResources, s.cacheSettings),
		commands.WithCheckDatastoreThrottler(
			s.featureFlagClient.Boolean(serverconfig.ExperimentalDatastoreThrottling, storeID),
			s.checkDatastoreThrottleThreshold,
			s.checkDatastoreThrottleDuration,
		),
	)

	resp, err := checkQuery.Execute(ctx, &commands.CheckCommandParams{
		StoreID:          storeID,
		TupleKey:         req.GetTupleKey(),
		ContextualTuples: req.GetContextualTuples(),
		Context:          req.GetContext(),
		Consistency:      req.GetConsistency(),
	})

	tookMs := time.Since(startTime).Milliseconds()

	if resp != nil {
		dispatchCount := float64(resp.DispatchCount)

		grpc_ctxtags.Extract(ctx).Set(dispatchCountHistogramName, dispatchCount)
		span.SetAttributes(attribute.Float64(dispatchCountHistogramName, dispatchCount))
		dispatchCountHistogram.WithLabelValues(
			s.serviceName,
			methodName,
		).Observe(dispatchCount)

		queryCount := float64(resp.DatastoreQueryCount)

		grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, queryCount)
		span.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, queryCount))
		datastoreQueryCountHistogram.WithLabelValues(s.serviceName, methodName).Observe(queryCount)

		datastoreItemCount := float64(resp.DatastoreItemCount)

		grpc_ctxtags.Extract(ctx).Set(datastoreItemCountHistogramName, datastoreItemCount)
		span.SetAttributes(attribute.Float64(datastoreItemCountHistogramName, datastoreItemCount))
		datastoreItemCountHistogram.WithLabelValues(s.serviceName, methodName).Observe(datastoreItemCount)

		requestDurationHistogram.WithLabelValues(
			s.serviceName,
			methodName,
			utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
			utils.Bucketize(uint(resp.DispatchCount), s.requestDurationByDispatchCountHistogramBuckets),
			req.GetConsistency().String(),
		).Observe(float64(tookMs))

		if s.authorizer.AccessControlStoreID() == req.GetStoreId() {
			accessControlStoreCheckDurationHistogram.WithLabelValues(
				utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
				utils.Bucketize(uint(resp.DispatchCount), s.requestDurationByDispatchCountHistogramBuckets),
				req.GetConsistency().String(),
			).Observe(float64(tookMs))
		}

		if resp.DispatchThrottled {
			throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDispatch).Inc()
		}
		grpc_ctxtags.Extract(ctx).Set("request.dispatch_throttled", resp.DispatchThrottled)

		if resp.WasThrottled {
			throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDatastore).Inc()
		}
		grpc_ctxtags.Extract(ctx).Set("request.datastore_throttled", resp.WasThrottled)
	}

	if err != nil {
		telemetry.TraceError(span, err)
		finalErr := commands.CheckCommandErrorToServerError(err)
		if errors.Is(finalErr, serverErrors.ErrThrottledTimeout) && resp != nil {
			if resp.DispatchThrottled {
				throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDispatch).Inc()
			}
			if resp.WasThrottled {
				throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDatastore).Inc()
			}
		}
		// should we define all metrics in one place that is accessible from everywhere (including LocalChecker!)
		// and add a wrapper helper that automatically injects the service name tag?
		return nil, finalErr
	}

	checkResultCounter.With(prometheus.Labels{allowedLabel: strconv.FormatBool(resp.Allowed)}).Inc()

	span.SetAttributes(
		attribute.Bool("cycle_detected", resp.CycleDetected),
		attribute.Bool("allowed", resp.Allowed))

	res := &openfgav1.CheckResponse{
		Allowed: resp.Allowed,
	}

	if s.featureFlagClient.Boolean(serverconfig.ExperimentalShadowWeightedGraphCheck, req.GetStoreId()) {
		go s.shadowV2Check(ctx, req, res, tookMs,
			resp.DatastoreQueryCount,
			resp.DatastoreItemCount)
	}

	return res, nil
}

func (s *Server) shadowV2Check(ctx context.Context, req *openfgav1.CheckRequest, mainRes *openfgav1.CheckResponse, mainTook int64, mainDatastoreQueryCount uint32, mainDatastoreItemCount uint64) {
	start := time.Now()
	var res *commands.CheckResult
	var err error
	originalSpanCtx := trace.SpanContextFromContext(ctx)
	recoveredErr := panics.Try(func() {
		newCtx, cancel := context.WithTimeout(context.Background(), s.shadowCheckResolverTimeout)
		defer cancel()
		// Inject the original span context so shadow spans share the same trace ID.
		newCtx = trace.ContextWithSpanContext(newCtx, originalSpanCtx)
		newCtx, shadowSpan := tracer.Start(newCtx, "shadowV2Check", trace.WithAttributes(
			attribute.String("store_id", req.GetStoreId()),
		))
		defer shadowSpan.End()
		res, err = s.v2Check(newCtx, req, s.sharedDatastoreResources.ShadowCheckCache, s.sharedDatastoreResources.ShadowCacheController, s.shadowAuthzModelGraphResolver)

		if res != nil {
			shadowQueryCount := float64(res.DatastoreQueryCount)
			shadowItemCount := float64(res.DatastoreItemCount)

			grpc_ctxtags.Extract(newCtx).Set(datastoreQueryCountHistogramName, shadowQueryCount)
			shadowSpan.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, shadowQueryCount))
			datastoreQueryCountHistogram.WithLabelValues(s.serviceName, commands.V2CheckMethodName).Observe(shadowQueryCount)

			grpc_ctxtags.Extract(newCtx).Set(datastoreItemCountHistogramName, shadowItemCount)
			shadowSpan.SetAttributes(attribute.Float64(datastoreItemCountHistogramName, shadowItemCount))
			datastoreItemCountHistogram.WithLabelValues(s.serviceName, commands.V2CheckMethodName).Observe(shadowItemCount)

			if res.WasThrottled {
				throttledRequestCounter.WithLabelValues(s.serviceName, commands.V2CheckMethodName, throttleTypeDatastore).Inc()
			}
			grpc_ctxtags.Extract(newCtx).Set("request.datastore_throttled", res.WasThrottled)
			shadowSpan.SetAttributes(attribute.Bool("matches", mainRes.GetAllowed() == res.Allowed))
		}
	})
	if recoveredErr != nil {
		err = recoveredErr.AsError()
	}
	if err != nil {
		if errors.Is(err, modelgraph.ErrInvalidModel) {
			s.logger.InfoWithContext(ctx, "invalid model graph check request")
			return
		}
		s.logger.ErrorWithContext(ctx, "shadow v2 check failed", zap.Error(err))
		return
	}
	s.logger.InfoWithContext(ctx, "shadow check",
		zap.Bool("matches", mainRes.GetAllowed() == res.Allowed),
		zap.Int64("main_took", mainTook),
		zap.Int64("shadow_took", time.Since(start).Milliseconds()),
		zap.Bool("main_result", mainRes.GetAllowed()),
		zap.Bool("shadow_result", res.Allowed),
		zap.String("store_id", req.GetStoreId()),
		zap.String("object", req.GetTupleKey().GetObject()),
		zap.String("relation", req.GetTupleKey().GetRelation()),
		zap.String("user", req.GetTupleKey().GetUser()),
		zap.Any("context", req.GetContext()),
		zap.Any("contextual_tuples", req.GetContextualTuples()),
		zap.Uint32("main_datastore_query_count", mainDatastoreQueryCount),
		zap.Uint32("shadow_datastore_query_count", res.DatastoreQueryCount),
		zap.Uint64("main_datastore_item_count", mainDatastoreItemCount),
		zap.Uint64("shadow_datastore_item_count", res.DatastoreItemCount),
	)
}

func (s *Server) v2Check(
	ctx context.Context,
	req *openfgav1.CheckRequest,
	cache storage.InMemoryCache[any],
	cacheController cachecontroller.CacheController,
	modelGraphResolver *modelgraph.AuthorizationModelGraphResolver,
) (*commands.CheckResult, error) {
	storeID := req.GetStoreId()
	tk := req.GetTupleKey()

	ctx, span := tracer.Start(ctx, commands.V2CheckMethodName, trace.WithAttributes(
		attribute.String("store_id", storeID),
		attribute.String("object", tk.GetObject()),
		attribute.String("relation", tk.GetRelation()),
		attribute.String("user", tk.GetUser()),
		attribute.String("consistency", req.GetConsistency().String()),
	))
	defer span.End()

	cacheInvalidationTime := time.Time{}
	if req.GetConsistency() != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		cacheInvalidationTime = cacheController.DetermineInvalidationTime(ctx, storeID)
	}
	span.SetAttributes(
		attribute.Bool("cache_invalidation_active", !cacheInvalidationTime.IsZero()),
	)

	mg, err := modelGraphResolver.Resolve(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	q := commands.NewCheckQuery(
		commands.WithCheckQueryV2Logger(s.logger),
		commands.WithCheckQueryV2Datastore(s.datastore),
		commands.WithCheckQueryV2MaxConcurrentReads(s.maxConcurrentReadsForCheck),
		commands.WithCheckQueryV2DatastoreThrottling(
			s.featureFlagClient.Boolean(serverconfig.ExperimentalDatastoreThrottling, storeID),
			s.checkDatastoreThrottleThreshold,
			s.checkDatastoreThrottleDuration,
		),
		commands.WithCheckQueryV2Model(mg),
		commands.WithCheckQueryV2Cache(cache),
		commands.WithCheckQueryV2QueryCacheEnabled(s.cacheSettings.ShouldCacheCheckQueries()),
		commands.WithCheckQueryV2QueryCacheTTL(s.cacheSettings.CheckQueryCacheTTL),
		commands.WithCheckQueryV2Planner(s.planner),
		commands.WithCheckQueryV2LastCacheInvalidationTime(cacheInvalidationTime),
		commands.WithCheckQueryV2ConcurrencyLimit(int(s.resolveNodeBreadthLimit)),
		commands.WithCheckQueryV2UpstreamTimeout(s.requestTimeout),
		commands.WithCheckQueryV2SharedResources(s.sharedDatastoreResources),
	)

	res, err := q.Execute(ctx, &commands.CheckCommandParams{
		StoreID:          req.GetStoreId(),
		TupleKey:         req.GetTupleKey(),
		ContextualTuples: req.GetContextualTuples(),
		Context:          req.GetContext(),
		Consistency:      req.GetConsistency(),
	})

	if err != nil {
		telemetry.TraceError(span, err)
		return res, commands.CheckCommandErrorToServerError(err)
	}

	span.SetAttributes(attribute.Bool("allowed", res.Allowed))
	return res, nil
}

const (
	reasonSelfReferentialUserset = "self_referential_userset"
	reasonAliasUserset           = "alias_userset"
	reasonComputedUsersetSelfObj = "computed_userset_self_object"
	reasonTTUUserset             = "ttu_userset"
)

// breakingChangeReason returns a non-empty reason string when the request shape matches a
// known v1→v2 Check divergence for userset users. Caller has already verified that the
// user is a userset (object#relation) and that v2Check returned FALSE.
//
// Reasons:
//
//   - "alias_userset": the target relation directly accepts T#R' where R' resolves via
//     computed_userset to the user's relation R, and R is not itself directly assignable
//     on the target. e.g.
//     model
//     type user
//     type document
//     relations
//     define reader: [user]
//     define allowed: reader
//     define viewer: [user, document#allowed]
//     With the query `document:d1, viewer, document3#reader`; v1 follows the allowed→reader
//     alias from a stored `allowed` tuple, v2 does not.
//
//   - "self_referential_userset": v1 unconditionally returned TRUE for this shape
//     regardless of whether any data existed; v2 evaluates against the schema and
//     storage and correctly returns FALSE. e.g.
//     model
//     type user
//     type document
//     relations
//     define viewer: [user]
//     `document:d1, viewer, document:d1#viewer` v1 returned TRUE, v2 returns FALSE.
//
//   - "computed_userset_self_object": user's object equals the target object, and the user's
//     relation appears as a ComputedUserset leaf in the target relation's rewrite tree.
//     e.g.
//     model
//     type user
//     type document
//     relations
//     define viewer: editor or writer
//     define editor: [user]
//     define writer: [user]
//     `document:d1, viewer, document:d1#writer` v1 returned TRUE, v2 returns FALSE.`
//
//   - "ttu_userset": target relation's rewrite contains a TupleToUserset whose computed
//     relation equals the user's relation, AND the user's object type is directly-related
//     to the tupleset relation. e.g. document.viewer = viewer from parent; query
//     viewer@folder:f2#viewer on document:d1. v1 returned TRUE from schema reachability
//     plus a parent tuple; v2 requires the userset to be explicit. e.g.
//     model
//     type user
//     type folder
//     relations
//     define viewer: [user]
//     type document
//     relations
//     define parent: [folder]
//     define viewer: viewer from parent
//     With the query `document:d1, viewer, folder:f2#viewer` and the tuple `document:d1, parent, folder:f2`;
//     v1 returned TRUE, v2 returns FALSE.
//
// All schema-shape filters are necessary conditions only — they may over-report when no
// matching tuple is actually stored, but never miss a real divergence.
func breakingChangeReason(typesys *typesystem.TypeSystem, tk *openfgav1.CheckRequestTupleKey) string {
	if tk.GetUser() == tk.GetObject()+"#"+tk.GetRelation() {
		return reasonSelfReferentialUserset
	}
	userObject, userRelation := tuple.SplitObjectRelation(tk.GetUser())
	userObjectType := tuple.GetType(userObject)
	targetObjectType := tuple.GetType(tk.GetObject())
	targetRelation := tk.GetRelation()

	if usersetAliasesTargetRelation(typesys, targetObjectType, targetRelation, userObjectType, userRelation) {
		return reasonAliasUserset
	}
	rel, err := typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		return ""
	}
	rewrite := rel.GetRewrite()
	if userObject == tk.GetObject() && rewriteContainsComputedUserset(rewrite, userRelation) {
		return reasonComputedUsersetSelfObj
	}
	if rewriteContainsTTUForUser(typesys, targetObjectType, rewrite, userObjectType, userRelation) {
		return reasonTTUUserset
	}
	return ""
}

// rewriteContainsComputedUserset reports whether any ComputedUserset leaf in the rewrite
// tree references the given relation name.
func rewriteContainsComputedUserset(rewrite *openfgav1.Userset, relation string) bool {
	result, _ := typesystem.WalkUsersetRewrite(rewrite, func(r *openfgav1.Userset) interface{} {
		if cu, ok := r.GetUserset().(*openfgav1.Userset_ComputedUserset); ok && cu.ComputedUserset.GetRelation() == relation {
			return true
		}
		return nil
	})
	return result != nil && result.(bool)
}

// rewriteContainsTTUForUser reports whether the target's rewrite contains a TupleToUserset
// whose computed relation equals the user's relation, where the tupleset relation on the
// target object type is directly related to the user's object type.
func rewriteContainsTTUForUser(ts *typesystem.TypeSystem, targetObjectType string, rewrite *openfgav1.Userset, userObjectType, userRelation string) bool {
	result, _ := typesystem.WalkUsersetRewrite(rewrite, func(r *openfgav1.Userset) interface{} {
		if ttu, ok := r.GetUserset().(*openfgav1.Userset_TupleToUserset); ok && ttu.TupleToUserset.GetComputedUserset().GetRelation() == userRelation {
			tuplesetRel := ttu.TupleToUserset.GetTupleset().GetRelation()
			if directlyRelated, err := ts.GetDirectlyRelatedUserTypes(targetObjectType, tuplesetRel); err == nil {
				for _, dr := range directlyRelated {
					if dr.GetType() == userObjectType {
						return true
					}
				}
			}
		}
		return nil
	})
	return result != nil && result.(bool)
}

// usersetAliasesTargetRelation reports whether the target's directly-related usersets
// include some T#R' (where T = user's object type) that is a computed_userset alias for
// the user's relation R. Excludes the trivial case where T#R is itself directly assignable
// (since v1 and v2 agree on direct matches).
func usersetAliasesTargetRelation(ts *typesystem.TypeSystem, targetObjectType, targetRelation, userObjectType, userRelation string) bool {
	usersets, err := ts.DirectlyRelatedUsersets(targetObjectType, targetRelation)
	if err != nil {
		return false
	}
	foundAlias := false
	for _, ref := range usersets {
		if ref.GetType() != userObjectType {
			continue
		}
		if ref.GetRelation() == userRelation {
			return false
		}
		if resolved, err := ts.ResolveComputedRelation(ref.GetType(), ref.GetRelation()); err == nil && resolved == userRelation {
			foundAlias = true
		}
	}
	return foundAlias
}

func (s *Server) getCheckResolverBuilder(storeID string) *graph.CheckResolverOrderedBuilder {
	checkCacheOptions, checkDispatchThrottlingOptions := s.getCheckResolverOptions()

	return graph.NewOrderedCheckResolvers([]graph.CheckResolverOrderedBuilderOpt{
		graph.WithLocalCheckerOpts([]graph.LocalCheckerOption{
			graph.WithResolveNodeBreadthLimit(s.resolveNodeBreadthLimit),
			graph.WithOptimizations(s.featureFlagClient.Boolean(serverconfig.ExperimentalCheckOptimizations, storeID)),
			graph.WithMaxResolutionDepth(s.resolveNodeLimit),
			graph.WithPlanner(s.planner),
			graph.WithUpstreamTimeout(s.requestTimeout),
			graph.WithLocalCheckerLogger(s.logger),
		}...),
		graph.WithLocalShadowCheckerOpts([]graph.LocalCheckerOption{
			graph.WithResolveNodeBreadthLimit(s.resolveNodeBreadthLimit),
			graph.WithOptimizations(true), // shadow checker always uses optimizations
			graph.WithMaxResolutionDepth(s.resolveNodeLimit),
			graph.WithPlanner(s.planner),
		}...),
		graph.WithShadowResolverEnabled(s.featureFlagClient.Boolean(serverconfig.ExperimentalShadowCheck, storeID)),
		graph.WithShadowResolverOpts([]graph.ShadowResolverOpt{
			graph.ShadowResolverWithLogger(s.logger),
			graph.ShadowResolverWithTimeout(s.shadowCheckResolverTimeout),
		}...),
		graph.WithCachedCheckResolverOpts(s.cacheSettings.ShouldCacheCheckQueries(), checkCacheOptions...),
		graph.WithDispatchThrottlingCheckResolverOpts(s.checkDispatchThrottlingEnabled, checkDispatchThrottlingOptions...),
	}...)
}
