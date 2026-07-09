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
	"github.com/openfga/openfga/pkg/server/commands/v2breaking"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
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

	// v2FellBack tracks whether v2Check ran, errored, and we fell back to v1.
	// Only in that case does the v1 path need to emit v2-breaking-change logs —
	// when the flag is off, there is no v2 comparison to signal about.
	// v2FallbackErr holds the v2Check error so the fallback path can classify
	// it (e.g. exclusion-shape rejection) without re-walking the schema.
	var v2FellBack bool
	var v2FallbackErr error

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
			// See v2breaking.CheckReason for the scenarios we detect.
			if !res.Allowed && tuple.IsObjectRelation(req.GetTupleKey().GetUser()) {
				if typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId()); err == nil {
					tk := req.GetTupleKey()
					if reason := v2breaking.CheckReason(typesys, tk); reason != "" {
						requestID := requestid.GetRequestIDFromContext(ctx)
						s.logger.WarnWithContext(ctx, "potential v2 Check resolution breaking change",
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

		v2FellBack = true
		v2FallbackErr = err
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

	// If we fell back from v2Check to v1, surface potential resolution
	// divergences: v2 might have returned a different answer (or rejected the
	// request outright) for the same input.
	//
	// Reason precedence:
	//  1. If v2Check's error was one of the exclusion-shape rejections, use it
	//     directly — no schema walk needed and no allowed-gating (v2 rejected
	//     regardless of the v1 answer).
	//  2. Otherwise v2 failed for another reason (e.g. weighted-graph build
	//     failure) and we must inspect the schema ourselves: exclusion shapes
	//     log unconditionally, per-userset shapes gate on !allowed to match
	//     the v2-success log path above.
	if v2FellBack {
		tk := req.GetTupleKey()
		requestID := requestid.GetRequestIDFromContext(ctx)
		logFields := []zap.Field{
			zap.String("store_id", storeID),
			zap.String("model_id", req.GetAuthorizationModelId()),
			zap.String("request_id", requestID),
		}
		emit := func(reason string) {
			s.logger.WarnWithContext(ctx, "potential v2 Check resolution breaking change",
				append(logFields, zap.String("reason", reason))...)
		}
		if reason := v2breaking.CheckReasonFromV2Error(v2FallbackErr); reason != "" {
			emit(reason)
		} else if reason := v2breaking.CheckExclusionReason(typesys, tk); reason != "" {
			emit(reason)
		} else if !resp.Allowed && tuple.IsObjectRelation(tk.GetUser()) {
			if reason := v2breaking.CheckReason(typesys, tk); reason != "" {
				emit(reason)
			}
		}
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
