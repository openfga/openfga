package server

import (
	"context"
	"errors"
	"slices"
	"strconv"
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
	"github.com/openfga/openfga/pkg/middleware/requestid"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/server/commands/v2breaking"
	"github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
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
		var v2CheckCount, v2FallbackCount int
		if v2GraphResolveFailed {
			v2FallbackCount = len(req.GetChecks())
		} else if q, ok := checker.(*commands.CheckQueryV2); ok {
			v2FallbackCount = q.FallbackCount()
			v2CheckCount = len(req.GetChecks()) - metadata.DuplicateCheckCount - v2FallbackCount
		}
		span.SetAttributes(
			attribute.Int("v2_check_count", v2CheckCount),
			attribute.Int("v2_fallback_count", v2FallbackCount),
		)
		grpc_ctxtags.Extract(ctx).Set("v2_check_count", v2CheckCount)
		grpc_ctxtags.Extract(ctx).Set("v2_fallback_count", v2FallbackCount)
	}

	batchResult := map[string]*openfgav1.BatchCheckSingleResult{}
	for correlationID, outcome := range result {
		batchResult[string(correlationID)] = transformCheckResultToProto(outcome)
		s.emitCheckDurationMetric(graph.ResolveCheckResponseMetadata{DatastoreQueryCount: outcome.DatastoreQueryCount, Duration: outcome.Duration}, methodName)
	}

	if v2Enabled {
		s.logBatchCheckBreakingChanges(ctx, req, typesys, result)
	}

	grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, metadata.DatastoreQueryCount)
	grpc_ctxtags.Extract(ctx).Set(datastoreItemCountHistogramName, metadata.DatastoreItemCount)

	return &openfgav1.BatchCheckResponse{Result: batchResult}, nil
}

// logBatchCheckBreakingChanges emits a single aggregate log flagging potential
// v2 (weighted-graph) resolution breaking changes across the whole batch. It
// mirrors the single-Check detection, minus per-item v2-error attribution:
// BatchCheck injects CheckQueryV2 as the Checker and handles fallback
// internally, so the server never sees which items fell back or their v2
// errors — only an aggregate FallbackCount. We therefore rely on the
// schema-shape predicates alone, which catch the same shapes as
// CheckReasonFromV2Error would.
//
// Detection precedence per check (matching the single-Check fallback path):
//  1. Exclusion shapes (userset_with_exclusion / wildcard_with_exclusion) via
//     CheckExclusionReason — flagged unconditionally, since v2 rejects these at
//     request time regardless of the v1 answer.
//  2. Otherwise, for userset users whose result is FALSE, the per-user shapes
//     via CheckReason.
//
// Checks that errored are skipped (no meaningful v1 answer to compare). To keep
// batch logging bounded, the results are emitted as one warn line rather than
// one line per check: a `reasons` list of the distinct shapes detected, and a
// `reasons_by_correlation_id` map attributing each flagged check to its reason
// so operators can pinpoint which items in the batch are affected.
//
// The shape predicates walk the model's rewrite tree (recursively, across
// ComputedUserset/TupleToUserset edges on the wildcard path), which is the
// dominant cost. For a fixed type system, both reasons are functions of the
// request's *shape* — object type, relation, and user type[#relation] — rather
// than its concrete identifiers, so identical shapes within a batch are
// memoized and each distinct shape is walked at most once, bounding the cost by
// distinct shapes rather than batch size. Two caches are kept because the two
// reasons key differently:
//
//   - exclusionCache (CheckExclusionReason): flagged unconditionally — v2
//     rejects these shapes at request time regardless of the v1 answer — and
//     never compares object identity. Its key is pure shape, with no allowed
//     bit and no identity component, so e.g. document:1@group:eng#member and
//     document:2@group:sales#member share one entry.
//
//   - checkReasonCache (CheckReason): only consulted on the !Allowed userset
//     path, so the allowed bit is a control-flow gate rather than part of the
//     key. Its self_referential / computed_userset_self_object shapes depend on
//     the user's object equalling the target object, so the key adds a
//     sameObject bool — collapsing the concrete IDs (document:1@document:1#…,
//     document:2@document:2#…) to a single bit that still keys them together.
func (s *Server) logBatchCheckBreakingChanges(ctx context.Context, req *openfgav1.BatchCheckRequest, typesys *typesystem.TypeSystem, result map[commands.CorrelationID]*commands.BatchCheckOutcome) {
	reasonSet := map[string]struct{}{}
	reasonsByCorrelationID := map[string]string{}
	// Both caches memoize the (possibly empty) reason for a shape key so repeated
	// shapes in the batch skip the rewrite-tree walk. Empty reasons are cached
	// too, so repeated no-match checks are equally cheap.
	exclusionCache := map[string]string{}
	checkReasonCache := map[string]string{}

	for _, check := range req.GetChecks() {
		outcome := result[commands.CorrelationID(check.GetCorrelationId())]
		if outcome == nil || outcome.Err != nil {
			continue
		}

		tk := check.GetTupleKey()
		userObject, userRelation := tuple.SplitObjectRelation(tk.GetUser())
		// Pure-shape key: object type, relation, and user type[#relation]. No
		// identifiers and no allowed bit, so requests differing only in their
		// concrete IDs share an entry.
		shapeKey := tuple.GetType(tk.GetObject()) + "#" + tk.GetRelation() + "@" + tuple.GetType(userObject) + "#" + userRelation

		reason, cached := exclusionCache[shapeKey]
		if !cached {
			reason = v2breaking.CheckExclusionReason(typesys, tk)
			exclusionCache[shapeKey] = reason
		}

		if reason == "" && !outcome.Allowed && tuple.IsObjectRelation(tk.GetUser()) {
			// CheckReason's self_referential / computed_userset_self_object shapes
			// depend on the user's object equalling the target object, so add that
			// as a bool rather than keying on the identifiers themselves.
			sameObject := userObject == tk.GetObject()
			checkKey := shapeKey + "|" + strconv.FormatBool(sameObject)
			r, ok := checkReasonCache[checkKey]
			if !ok {
				r = v2breaking.CheckReason(typesys, tk)
				checkReasonCache[checkKey] = r
			}
			reason = r
		}

		if reason == "" {
			continue
		}

		reasonSet[reason] = struct{}{}
		reasonsByCorrelationID[check.GetCorrelationId()] = reason
	}

	if len(reasonSet) == 0 {
		return
	}

	reasons := make([]string, 0, len(reasonSet))
	for reason := range reasonSet {
		reasons = append(reasons, reason)
	}
	slices.Sort(reasons) // stable ordering for log readability

	s.logger.WarnWithContext(ctx, "potential v2 BatchCheck resolution breaking change",
		zap.String("store_id", req.GetStoreId()),
		zap.String("model_id", req.GetAuthorizationModelId()),
		zap.String("request_id", requestid.GetRequestIDFromContext(ctx)),
		zap.Strings("reasons", reasons),
		zap.Int("matched_checks", len(reasonsByCorrelationID)),
		zap.Any("reasons_by_correlation_id", reasonsByCorrelationID),
	)
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
