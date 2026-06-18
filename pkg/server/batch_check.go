package server

import (
	"context"
	"errors"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/telemetry"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
)

func (s *Server) BatchCheck(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
	startTime := time.Now()

	ctx, span := tracer.Start(ctx, apimethod.BatchCheck.String(), trace.WithAttributes(
		attribute.KeyValue{Key: "store_id", Value: attribute.StringValue(req.GetStoreId())},
		attribute.KeyValue{Key: "batch_size", Value: attribute.IntValue(len(req.GetChecks()))},
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
		Method:  apimethod.BatchCheck.String(),
	})

	storeID := req.GetStoreId()
	err := s.checkAuthz(ctx, storeID, apimethod.BatchCheck)
	if err != nil {
		return nil, err
	}

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}
	req.AuthorizationModelId = typesys.GetAuthorizationModelID() // the resolved model id

	builder := s.getCheckResolverBuilder(req.GetStoreId())
	checkResolver, checkResolverCloser, err := builder.Build()
	if err != nil {
		return nil, err
	}
	defer checkResolverCloser()

	v1Checker := commands.NewCheckCommand(
		s.datastore,
		checkResolver,
		typesys,
		commands.WithCheckCommandLogger(s.logger),
		commands.WithCheckCommandMaxConcurrentReads(s.maxConcurrentReadsForCheck),
		commands.WithCheckCommandCache(s.sharedDatastoreResources, s.cacheSettings),
		commands.WithCheckDatastoreThrottler(
			s.featureFlagClient.Boolean(config.ExperimentalDatastoreThrottling, storeID),
			s.checkDatastoreThrottleThreshold,
			s.checkDatastoreThrottleDuration,
		),
	)

	var checker commands.Checker = v1Checker
	var v2GraphResolveFailed bool
	v2Enabled := s.featureFlagClient.Boolean(config.ExperimentalWeightedGraphCheck, storeID)
	if v2Enabled {
		mg, mgErr := s.authzModelGraphResolver.Resolve(ctx, storeID, req.GetAuthorizationModelId())
		if mgErr == nil {
			cacheInvalidationTime := time.Time{}
			if req.GetConsistency() != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
				cacheInvalidationTime = s.sharedDatastoreResources.CacheController.DetermineInvalidationTime(ctx, storeID)
			}

			checker = commands.NewCheckQuery(
				commands.WithCheckQueryV2Logger(s.logger),
				commands.WithCheckQueryV2Datastore(s.datastore),
				commands.WithCheckQueryV2MaxConcurrentReads(s.maxConcurrentReadsForCheck),
				commands.WithCheckQueryV2DatastoreThrottling(
					s.featureFlagClient.Boolean(config.ExperimentalDatastoreThrottling, storeID),
					s.checkDatastoreThrottleThreshold,
					s.checkDatastoreThrottleDuration,
				),
				commands.WithCheckQueryV2Model(mg),
				commands.WithCheckQueryV2Cache(s.sharedDatastoreResources.CheckCache),
				commands.WithCheckQueryV2QueryCacheEnabled(s.cacheSettings.ShouldCacheCheckQueries()),
				commands.WithCheckQueryV2QueryCacheTTL(s.cacheSettings.CheckQueryCacheTTL),
				commands.WithCheckQueryV2Planner(s.planner),
				commands.WithCheckQueryV2LastCacheInvalidationTime(cacheInvalidationTime),
				commands.WithCheckQueryV2ConcurrencyLimit(int(s.resolveNodeBreadthLimit)),
				commands.WithCheckQueryV2UpstreamTimeout(s.requestTimeout),
				commands.WithCheckQueryV2SharedResources(s.sharedDatastoreResources),
				commands.WithCheckQueryV2Fallback(v1Checker),
			)
		} else if commands.IsV2CheckTerminalError(mgErr) {
			return nil, mgErr
		} else {
			v2GraphResolveFailed = true
			s.logger.WarnWithContext(ctx, "Weighted graph model resolution failed for batch check, falling back to main Check",
				zap.Error(mgErr),
				zap.String("store_id", storeID),
				zap.String("model_id", req.GetAuthorizationModelId()),
			)
		}
	}

	cmd := commands.NewBatchCheckCommand(
		checker,
		commands.WithBatchCheckCommandLogger(s.logger),
		commands.WithBatchCheckMaxChecksPerBatch(s.maxChecksPerBatchCheck),
		commands.WithBatchCheckMaxConcurrentChecks(s.maxConcurrentChecksPerBatch),
	)

	result, metadata, err := cmd.Execute(ctx, &commands.BatchCheckCommandParams{
		AuthorizationModelID: req.GetAuthorizationModelId(),
		Checks:               req.GetChecks(),
		Consistency:          req.GetConsistency(),
		StoreID:              storeID,
	})
	if err != nil {
		telemetry.TraceError(span, err)
		var batchValidationError *commands.BatchCheckValidationError
		if errors.As(err, &batchValidationError) {
			return nil, serverErrors.ValidationError(err)
		}

		return nil, err
	}

	methodName := "batchcheck"

	dispatchCount := float64(metadata.DispatchCount)
	grpc_ctxtags.Extract(ctx).Set(dispatchCountHistogramName, dispatchCount)
	span.SetAttributes(attribute.Float64(dispatchCountHistogramName, dispatchCount))
	dispatchCountHistogram.WithLabelValues(
		s.serviceName,
		methodName,
	).Observe(dispatchCount)

	wasDispatchThrottled := metadata.DispatchThrottleCount > 0
	if wasDispatchThrottled {
		throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDispatch).Add(float64(metadata.DispatchThrottleCount))
	}
	grpc_ctxtags.Extract(ctx).Set("request.dispatch_throttled", wasDispatchThrottled)

	wasDatastoreThrottled := metadata.DatastoreThrottleCount > 0
	if wasDatastoreThrottled {
		throttledRequestCounter.WithLabelValues(s.serviceName, methodName, throttleTypeDatastore).Add(float64(metadata.DatastoreThrottleCount))
	}
	grpc_ctxtags.Extract(ctx).Set("request.datastore_throttled", wasDatastoreThrottled)

	queryCount := float64(metadata.DatastoreQueryCount)
	span.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, queryCount))
	datastoreQueryCountHistogram.WithLabelValues(
		s.serviceName,
		methodName,
	).Observe(queryCount)

	datastoreItemCount := float64(metadata.DatastoreItemCount)
	span.SetAttributes(attribute.Float64(datastoreItemCountHistogramName, datastoreItemCount))
	datastoreItemCountHistogram.WithLabelValues(
		s.serviceName,
		methodName,
	).Observe(datastoreItemCount)

	tookMs := time.Since(startTime).Milliseconds()
	requestDurationHistogram.WithLabelValues(
		s.serviceName,
		methodName,
		utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
		utils.Bucketize(uint(metadata.DispatchCount), s.requestDurationByDispatchCountHistogramBuckets),
		req.GetConsistency().String(),
	).Observe(float64(tookMs))

	duplicateChecks := "duplicate_checks"
	span.SetAttributes(attribute.Int(duplicateChecks, metadata.DuplicateCheckCount))
	grpc_ctxtags.Extract(ctx).Set(duplicateChecks, metadata.DuplicateCheckCount)

	if v2Enabled {
		if v2GraphResolveFailed {
			metadata.PrimaryCheckerCount = 0
			metadata.FallbackCount = uint32(len(req.GetChecks()))
		}
		span.SetAttributes(
			attribute.Int("v2_check_count", int(metadata.PrimaryCheckerCount)),
			attribute.Int("v2_fallback_count", int(metadata.FallbackCount)),
		)
		grpc_ctxtags.Extract(ctx).Set("v2_check_count", metadata.PrimaryCheckerCount)
		grpc_ctxtags.Extract(ctx).Set("v2_fallback_count", metadata.FallbackCount)
	}

	batchResult := map[string]*openfgav1.BatchCheckSingleResult{}
	for correlationID, outcome := range result {
		batchResult[string(correlationID)] = transformCheckResultToProto(outcome)
		s.emitCheckDurationMetric(graph.ResolveCheckResponseMetadata{DatastoreQueryCount: outcome.DatastoreQueryCount, Duration: outcome.Duration}, methodName)
	}

	grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, metadata.DatastoreQueryCount)
	grpc_ctxtags.Extract(ctx).Set(datastoreItemCountHistogramName, metadata.DatastoreItemCount)

	return &openfgav1.BatchCheckResponse{Result: batchResult}, nil
}

// transformCheckResultToProto transforms the internal BatchCheckOutcome into the external-facing
// BatchCheckSingleResult struct for transmission back via the api.
func transformCheckResultToProto(outcome *commands.BatchCheckOutcome) *openfgav1.BatchCheckSingleResult {
	singleResult := &openfgav1.BatchCheckSingleResult{}

	if outcome.Err != nil {
		singleResult.CheckResult = &openfgav1.BatchCheckSingleResult_Error{
			Error: transformCheckCommandErrorToBatchCheckError(outcome.Err),
		}
	} else {
		singleResult.CheckResult = &openfgav1.BatchCheckSingleResult_Allowed{
			Allowed: outcome.Allowed,
		}
	}

	return singleResult
}

func transformCheckCommandErrorToBatchCheckError(cmdErr error) *openfgav1.CheckError {
	var invalidRelationError *commands.InvalidRelationError
	var invalidTupleError *commands.InvalidTupleError
	var invalidContextError *commands.InvalidContextError
	var throttledError *commands.ThrottledError

	err := &openfgav1.CheckError{Message: cmdErr.Error()}

	// switch to map the possible errors to their specific GRPC codes in the proto definition
	switch {
	case errors.As(cmdErr, &invalidRelationError):
		err.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error}
	case errors.As(cmdErr, &invalidTupleError):
		err.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_invalid_tuple}
	case errors.Is(cmdErr, graph.ErrResolutionDepthExceeded):
		err.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_authorization_model_resolution_too_complex}
	case errors.Is(cmdErr, condition.ErrEvaluationFailed):
		err.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error}
	case errors.As(cmdErr, &invalidContextError):
		err.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error}
	case errors.As(cmdErr, &throttledError):
		err.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error}
	case errors.Is(cmdErr, context.DeadlineExceeded):
		err.Code = &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_deadline_exceeded}
	default:
		err.Code = &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_internal_error}
	}

	return err
}
