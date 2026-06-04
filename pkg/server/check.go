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
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
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
		res, metadata, err := s.v2Check(ctx, req, s.sharedDatastoreResources.CheckCache, s.sharedDatastoreResources.CacheController, s.authzModelGraphResolver)

		// v2Check can return errors that v1 Check wouldn't (e.g. ErrInvalidModel when the weighted graph
		// can't represent the model). Fallback to v1 on non-timeout errors for backward compatibility.
		if err == nil || isV2TerminalError(err) {
			tookMs := time.Since(startTime).Milliseconds()
			queryCount := float64(metadata.DatastoreQueryCount)
			itemCount := float64(metadata.DatastoreItemCount)

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

			if metadata.WasThrottled {
				throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDatastore).Inc()
			}
			grpc_ctxtags.Extract(ctx).Set("request.datastore_throttled", metadata.WasThrottled)

			if err != nil {
				telemetry.TraceError(span, err)
				if _, ok := status.FromError(err); !ok {
					err = commands.CheckCommandErrorToServerError(err)
				}
				return nil, err
			}

			checkResultCounter.With(prometheus.Labels{allowedLabel: strconv.FormatBool(res.GetAllowed())}).Inc()
			span.SetAttributes(attribute.Bool("allowed", res.GetAllowed()))

			// Flag potential v2Check resolution breaking changes for userset requests.
			// See breakingChangeReason for the scenarios we detect.
			if !res.GetAllowed() && tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
				if reason := breakingChangeReason(ctx, s, req, storeID); reason != "" {
					requestID, _ := grpc_ctxtags.Extract(ctx).Values()["request_id"].(string)
					if requestID == "" {
						requestID = requestid.InitRequestID(ctx)
					}
					s.logger.InfoWithContext(ctx, "potential v2Check resolution breaking change: userset request returned false",
						zap.String("store_id", storeID),
						zap.String("model_id", req.GetAuthorizationModelId()),
						zap.String("request_id", requestID),
						zap.String("reason", reason),
					)
				}
			}

			return res, nil
		}

		requestID, _ := grpc_ctxtags.Extract(ctx).Values()["request_id"].(string)
		if requestID == "" {
			requestID = requestid.InitRequestID(ctx)
		}
		s.logger.WarnWithContext(ctx, "Weighted graph check failed, falling back to main Check",
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

	resp, checkRequestMetadata, err := checkQuery.Execute(ctx, &commands.CheckCommandParams{
		StoreID:          storeID,
		TupleKey:         req.GetTupleKey(),
		ContextualTuples: req.GetContextualTuples(),
		Context:          req.GetContext(),
		Consistency:      req.GetConsistency(),
	})

	tookMs := time.Since(startTime).Milliseconds()

	var (
		dispatchThrottled  bool
		datastoreThrottled bool
		rawDispatchCount   uint32
	)

	if checkRequestMetadata != nil {
		dispatchThrottled = checkRequestMetadata.DispatchThrottled.Load()
		datastoreThrottled = checkRequestMetadata.DatastoreThrottled.Load()
		rawDispatchCount = checkRequestMetadata.DispatchCounter.Load()
		dispatchCount := float64(rawDispatchCount)

		grpc_ctxtags.Extract(ctx).Set(dispatchCountHistogramName, dispatchCount)
		span.SetAttributes(attribute.Float64(dispatchCountHistogramName, dispatchCount))
		dispatchCountHistogram.WithLabelValues(
			s.serviceName,
			methodName,
		).Observe(dispatchCount)
	}

	if resp != nil {
		queryCount := float64(resp.GetResolutionMetadata().DatastoreQueryCount)

		grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, queryCount)
		span.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, queryCount))
		datastoreQueryCountHistogram.WithLabelValues(s.serviceName, methodName).Observe(queryCount)

		datastoreItemCount := float64(resp.GetResolutionMetadata().DatastoreItemCount)

		grpc_ctxtags.Extract(ctx).Set(datastoreItemCountHistogramName, datastoreItemCount)
		span.SetAttributes(attribute.Float64(datastoreItemCountHistogramName, datastoreItemCount))
		datastoreItemCountHistogram.WithLabelValues(s.serviceName, methodName).Observe(datastoreItemCount)

		requestDurationHistogram.WithLabelValues(
			s.serviceName,
			methodName,
			utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
			utils.Bucketize(uint(rawDispatchCount), s.requestDurationByDispatchCountHistogramBuckets),
			req.GetConsistency().String(),
		).Observe(float64(tookMs))

		if s.authorizer.AccessControlStoreID() == req.GetStoreId() {
			accessControlStoreCheckDurationHistogram.WithLabelValues(
				utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
				utils.Bucketize(uint(rawDispatchCount), s.requestDurationByDispatchCountHistogramBuckets),
				req.GetConsistency().String(),
			).Observe(float64(tookMs))
		}

		if dispatchThrottled {
			throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDispatch).Inc()
		}
		grpc_ctxtags.Extract(ctx).Set("request.dispatch_throttled", dispatchThrottled)

		if datastoreThrottled {
			throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDatastore).Inc()
		}
		grpc_ctxtags.Extract(ctx).Set("request.datastore_throttled", datastoreThrottled)
	}

	if err != nil {
		telemetry.TraceError(span, err)
		finalErr := commands.CheckCommandErrorToServerError(err)
		if errors.Is(finalErr, serverErrors.ErrThrottledTimeout) {
			if dispatchThrottled {
				throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDispatch).Inc()
			}
			if datastoreThrottled {
				throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDatastore).Inc()
			}
		}
		// should we define all metrics in one place that is accessible from everywhere (including LocalChecker!)
		// and add a wrapper helper that automatically injects the service name tag?
		return nil, finalErr
	}

	checkResultCounter.With(prometheus.Labels{allowedLabel: strconv.FormatBool(resp.GetAllowed())}).Inc()

	span.SetAttributes(
		attribute.Bool("cycle_detected", resp.GetCycleDetected()),
		attribute.Bool("allowed", resp.GetAllowed()))

	res := &openfgav1.CheckResponse{
		Allowed: resp.Allowed,
	}

	if s.featureFlagClient.Boolean(serverconfig.ExperimentalShadowWeightedGraphCheck, req.GetStoreId()) {
		go s.shadowV2Check(ctx, req, res, tookMs,
			resp.GetResolutionMetadata().DatastoreQueryCount,
			resp.GetResolutionMetadata().DatastoreItemCount)
	}

	return res, nil
}

func (s *Server) shadowV2Check(ctx context.Context, req *openfgav1.CheckRequest, mainRes *openfgav1.CheckResponse, mainTook int64, mainDatastoreQueryCount uint32, mainDatastoreItemCount uint64) {
	start := time.Now()
	var res *openfgav1.CheckResponse
	var shadowMetadata storagewrappers.Metadata
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
		res, shadowMetadata, err = s.v2Check(newCtx, req, s.sharedDatastoreResources.ShadowCheckCache, s.sharedDatastoreResources.ShadowCacheController, s.shadowAuthzModelGraphResolver)

		shadowQueryCount := float64(shadowMetadata.DatastoreQueryCount)
		shadowItemCount := float64(shadowMetadata.DatastoreItemCount)

		grpc_ctxtags.Extract(newCtx).Set(datastoreQueryCountHistogramName, shadowQueryCount)
		shadowSpan.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, shadowQueryCount))
		datastoreQueryCountHistogram.WithLabelValues(s.serviceName, commands.V2CheckMethodName).Observe(shadowQueryCount)

		grpc_ctxtags.Extract(newCtx).Set(datastoreItemCountHistogramName, shadowItemCount)
		shadowSpan.SetAttributes(attribute.Float64(datastoreItemCountHistogramName, shadowItemCount))
		datastoreItemCountHistogram.WithLabelValues(s.serviceName, commands.V2CheckMethodName).Observe(shadowItemCount)

		if shadowMetadata.WasThrottled {
			throttledRequestCounter.WithLabelValues(s.serviceName, commands.V2CheckMethodName, throttleTypeDatastore).Inc()
		}
		grpc_ctxtags.Extract(newCtx).Set("request.datastore_throttled", shadowMetadata.WasThrottled)

		if err == nil {
			shadowSpan.SetAttributes(attribute.Bool("matches", mainRes.GetAllowed() == res.GetAllowed()))
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
		zap.Bool("matches", mainRes.GetAllowed() == res.GetAllowed()),
		zap.Int64("main_took", mainTook),
		zap.Int64("shadow_took", time.Since(start).Milliseconds()),
		zap.Bool("main_result", mainRes.GetAllowed()),
		zap.Bool("shadow_result", res.GetAllowed()),
		zap.String("store_id", req.GetStoreId()),
		zap.String("object", req.GetTupleKey().GetObject()),
		zap.String("relation", req.GetTupleKey().GetRelation()),
		zap.String("user", req.GetTupleKey().GetUser()),
		zap.Any("context", req.GetContext()),
		zap.Any("contextual_tuples", req.GetContextualTuples()),
		zap.Uint32("main_datastore_query_count", mainDatastoreQueryCount),
		zap.Uint32("shadow_datastore_query_count", shadowMetadata.DatastoreQueryCount),
		zap.Uint64("main_datastore_item_count", mainDatastoreItemCount),
		zap.Uint64("shadow_datastore_item_count", shadowMetadata.DatastoreItemCount),
	)
}

func (s *Server) v2Check(
	ctx context.Context,
	req *openfgav1.CheckRequest,
	cache storage.InMemoryCache[any],
	cacheController cachecontroller.CacheController,
	modelGraphResolver *modelgraph.AuthorizationModelGraphResolver,
) (*openfgav1.CheckResponse, storagewrappers.Metadata, error) {
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
		return nil, storagewrappers.Metadata{}, err
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

	res, metadata, err := q.Execute(ctx, req)

	span.SetAttributes(attribute.Bool("allowed", res.GetAllowed()))

	if err != nil {
		telemetry.TraceError(span, err)
		return nil, metadata, commands.CheckCommandErrorToServerError(err)
	}

	return res, metadata, nil
}

// isV2TerminalError reports whether err should be returned directly from the v2Check path
// rather than falling back to v1. Two categories qualify:
//
//   - Context/timeout/throttle errors, where a v1 retry is pointless or harmful. Context
//     errors appear in two forms: raw (context.Canceled/DeadlineExceeded, from the model
//     graph resolver) or server-mapped (ErrRequestCancelled/ErrRequestDeadlineExceeded, from
//     the execute path). ErrThrottledTimeout and ErrTransactionThrottled are included since
//     they are not v2-specific, and retrying on v1 would only add load to an already-throttled
//     datastore.
//   - Deterministic request-validation failures (ErrValidation, ErrInvalidUser, and
//     contextual-tuple validation failures), which v1 rejects identically — the fallback is
//     wasted work and log noise.
//
// v2Check wraps most non-context errors via commands.CheckCommandErrorToServerError, which
// produces gRPC status.Error values. Errors.Is/As against the original sentinels/types no
// longer matches after that conversion, so request-validation cases must be classified by
// the resulting gRPC code instead.
func isV2TerminalError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, serverErrors.ErrRequestDeadlineExceeded) || errors.Is(err, serverErrors.ErrRequestCancelled) ||
		errors.Is(err, serverErrors.ErrThrottledTimeout) || errors.Is(err, serverErrors.ErrTransactionThrottled) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		switch openfgav1.ErrorCode(st.Code()) {
		case openfgav1.ErrorCode_validation_error, openfgav1.ErrorCode_invalid_tuple:
			return true
		}
	}
	return false
}

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
func breakingChangeReason(ctx context.Context, s *Server, req *openfgav1.CheckRequest, storeID string) string {
	tk := req.GetTupleKey()
	if tk.GetUser() == tk.GetObject()+"#"+tk.GetRelation() {
		return "self_referential_userset"
	}
	userObject, userRelation := tuple.SplitObjectRelation(tk.GetUser())
	userObjectType := tuple.GetType(userObject)
	targetObjectType := tuple.GetType(tk.GetObject())
	targetRelation := tk.GetRelation()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return ""
	}
	if usersetAliasesTargetRelation(typesys, targetObjectType, targetRelation, userObjectType, userRelation) {
		return "alias_userset"
	}
	rel, err := typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		return ""
	}
	rewrite := rel.GetRewrite()
	if userObject == tk.GetObject() && rewriteContainsComputedUserset(rewrite, userRelation) {
		return "computed_userset_self_object"
	}
	if rewriteContainsTTUForUser(typesys, targetObjectType, rewrite, userObjectType, userRelation) {
		return "ttu_userset"
	}
	return ""
}

// rewriteContainsComputedUserset reports whether any ComputedUserset leaf in the rewrite
// tree references the given relation name.
func rewriteContainsComputedUserset(rewrite *openfgav1.Userset, relation string) bool {
	result, _ := typesystem.WalkUsersetRewrite(rewrite, func(r *openfgav1.Userset) any {
		if cu, ok := r.GetUserset().(*openfgav1.Userset_ComputedUserset); ok {
			if cu.ComputedUserset.GetRelation() == relation {
				return true
			}
		}
		return nil
	})
	v, _ := result.(bool)
	return v
}

// rewriteContainsTTUForUser reports whether the target's rewrite contains a TupleToUserset
// whose computed relation equals the user's relation, where the tupleset relation on the
// target object type is directly related to the user's object type.
func rewriteContainsTTUForUser(ts *typesystem.TypeSystem, targetObjectType string, rewrite *openfgav1.Userset, userObjectType, userRelation string) bool {
	result, _ := typesystem.WalkUsersetRewrite(rewrite, func(r *openfgav1.Userset) any {
		ttu, ok := r.GetUserset().(*openfgav1.Userset_TupleToUserset)
		if !ok {
			return nil
		}
		if ttu.TupleToUserset.GetComputedUserset().GetRelation() != userRelation {
			return nil
		}
		tuplesetRel := ttu.TupleToUserset.GetTupleset().GetRelation()
		directlyRelated, err := ts.GetDirectlyRelatedUserTypes(targetObjectType, tuplesetRel)
		if err != nil {
			return nil
		}
		for _, dr := range directlyRelated {
			if dr.GetType() == userObjectType {
				return true
			}
		}
		return nil
	})
	v, _ := result.(bool)
	return v
}

// usersetAliasesTargetRelation reports whether the target relation has a directly-assignable
// userset T#R' where R' resolves (via computed_userset chains) to the request's user relation R,
// while R itself is NOT directly assignable on the target.
func usersetAliasesTargetRelation(ts *typesystem.TypeSystem, targetObjectType, targetRelation, userObjectType, userRelation string) bool {
	usersets, err := ts.DirectlyRelatedUsersets(targetObjectType, targetRelation)
	if err != nil {
		return false
	}
	for _, ref := range usersets {
		if ref.GetType() == userObjectType && ref.GetRelation() == userRelation {
			return false
		}
	}
	for _, ref := range usersets {
		if ref.GetType() != userObjectType {
			continue
		}
		resolved, err := ts.ResolveComputedRelation(ref.GetType(), ref.GetRelation())
		if err == nil && resolved == userRelation {
			return true
		}
	}
	return false
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
