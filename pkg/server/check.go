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

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/telemetry"
)

func (s *Server) Check(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	const methodName = "check"

	builder := s.getCheckResolverBuilder(req.GetStoreId())
	checkResolver, checkResolverCloser, err := builder.Build()
	if err != nil {
		return nil, err
	}
	defer checkResolverCloser()

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

	err = s.checkAuthz(ctx, req.GetStoreId(), apimethod.Check)
	if err != nil {
		return nil, err
	}

	storeID := req.GetStoreId()

	if s.featureFlagClient.Boolean(serverconfig.ExperimentalWeightedGraphCheck, storeID) {
		return s.v2Check(ctx, req)
	}

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

	endTime := time.Since(startTime).Milliseconds()

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
		datastoreQueryCountHistogram.WithLabelValues(
			s.serviceName,
			methodName,
		).Observe(queryCount)

		datastoreItemCount := float64(resp.GetResolutionMetadata().DatastoreItemCount)

		grpc_ctxtags.Extract(ctx).Set(datastoreItemCountHistogramName, datastoreItemCount)
		span.SetAttributes(attribute.Float64(datastoreItemCountHistogramName, datastoreItemCount))
		datastoreItemCountHistogram.WithLabelValues(
			s.serviceName,
			methodName,
		).Observe(datastoreItemCount)

		requestDurationHistogram.WithLabelValues(
			s.serviceName,
			methodName,
			utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
			utils.Bucketize(uint(rawDispatchCount), s.requestDurationByDispatchCountHistogramBuckets),
			req.GetConsistency().String(),
		).Observe(float64(endTime))

		if s.authorizer.AccessControlStoreID() == req.GetStoreId() {
			accessControlStoreCheckDurationHistogram.WithLabelValues(
				utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
				utils.Bucketize(uint(rawDispatchCount), s.requestDurationByDispatchCountHistogramBuckets),
				req.GetConsistency().String(),
			).Observe(float64(endTime))
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

	if s.featureFlagClient.Boolean(serverconfig.ExperimentalShadowWeightedGraphCheck, storeID) {
		go s.shadowV2Check(ctx, req, res, endTime)
	}

	return res, nil
}

func (s *Server) shadowV2Check(ctx context.Context, req *openfgav1.CheckRequest, mainRes *openfgav1.CheckResponse, mainTook int64) {
	start := time.Now()
	var res *openfgav1.CheckResponse
	var err error
	recoveredErr := panics.Try(func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.shadowCheckResolverTimeout)
		defer cancel()
		res, err = s.v2Check(ctx, req)
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
		zap.Bool("match", mainRes.GetAllowed() == res.GetAllowed()),
		zap.Int64("main_took", mainTook),
		zap.Duration("shadow_took", time.Since(start)),
		zap.Bool("main", mainRes.GetAllowed()),
		zap.Bool("shadow", res.GetAllowed()),
	)
}

func (s *Server) v2Check(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	cacheInvalidationTime := time.Time{}

	if req.GetConsistency() != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		cacheInvalidationTime = s.sharedDatastoreResources.CacheController.DetermineInvalidationTime(ctx, req.GetStoreId())
	}

	mg, err := s.authzModelGraphResolver.Resolve(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	q := commands.NewCheckQuery(
		commands.WithCheckQueryV2Logger(s.logger),
		commands.WithCheckQueryV2Datastore(s.datastore),
		commands.WithCheckQueryV2Model(mg),
		commands.WithCheckQueryV2Cache(s.sharedDatastoreResources.CheckCache),
		commands.WithCheckQueryV2CacheTTL(s.cacheSettings.CheckQueryCacheTTL),
		commands.WithCheckQueryV2Planner(s.planner),
		commands.WithCheckQueryV2LastCacheInvalidationTime(cacheInvalidationTime),
		commands.WithCheckQueryV2ConcurrencyLimit(int(s.resolveNodeBreadthLimit)),
		commands.WithCheckQueryV2UpstreamTimeout(s.requestTimeout),
	)

	res, err := q.Execute(ctx, req)
	if err != nil {
		return nil, commands.CheckCommandErrorToServerError(err)
	}
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
