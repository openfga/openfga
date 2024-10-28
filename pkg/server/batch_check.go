package server

import (
	"context"
	"errors"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/graph"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/openfga/openfga/internal/authz"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/telemetry"
)

func (s *Server) BatchCheck(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
	ctx, span := tracer.Start(ctx, authz.Read, trace.WithAttributes(
		attribute.KeyValue{Key: "store_id", Value: attribute.StringValue(req.GetStoreId())},
		attribute.KeyValue{Key: "batch_size", Value: attribute.IntValue(len(req.GetChecks()))},
		attribute.KeyValue{Key: "consistency", Value: attribute.StringValue(req.GetConsistency().String())},
	))
	defer span.End()

	err := req.Validate()
	if err != nil {
		return nil, err
	}

	err = s.checkAuthz(ctx, req.GetStoreId(), authz.BatchCheck)
	if err != nil {
		return nil, err
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  authz.BatchCheck,
	})

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	cmd := commands.NewBatchCheckCommand(
		s.checkDatastore,
		s.checkResolver,
		typesys,
		commands.WithBatchCheckCommandCacheController(s.cacheController),
		commands.WithBatchCheckCommandLogger(s.logger),
		commands.WithBatchCheckMaxChecksPerBatch(s.maxChecksPerBatchCheck),
		commands.WithBatchCheckMaxConcurrentChecks(s.maxConcurrentChecksPerBatch),
	)

	result, err := cmd.Execute(ctx, &commands.BatchCheckCommandParams{
		AuthorizationModelID: req.GetAuthorizationModelId(),
		Checks:               req.GetChecks(),
		Consistency:          req.GetConsistency(),
		StoreID:              storeID,
	})
	if err != nil {
		return nil, err
	}

	response := &openfgav1.BatchCheckResponse{}
	response.Result = transformCheckResultToRPC(result)

	return response, nil
}

// transformCheckResultToRPC takes about 150ns per check, or 0.00015ms.
// If batch sizes ever increase into the thousands, we may want to
// refactor this to work concurrently alongside the checks themselves.
func transformCheckResultToRPC(checkResults map[string]*commands.BatchCheckOutcome) map[string]*openfgav1.BatchCheckSingleResult {
	var batchResult = map[string]*openfgav1.BatchCheckSingleResult{}
	for k, v := range checkResults {
		singleResult := &openfgav1.BatchCheckSingleResult{}
		singleResult.QueryDurationMs = wrapperspb.Int32(int32(v.Duration.Milliseconds()))

		if v.Err != nil {
			singleResult.CheckResult = &openfgav1.BatchCheckSingleResult_Error{
				Error: transformCheckErrorToBatchCheckError(v.Err),
			}
		} else {
			singleResult.CheckResult = &openfgav1.BatchCheckSingleResult_Allowed{
				Allowed: v.CheckResponse.Allowed,
			}
		}

		batchResult[k] = singleResult
	}

	return batchResult
}

func transformCheckErrorToBatchCheckError(err error) *openfgav1.CheckError {
	checkError := &openfgav1.CheckError{Message: err.Error()}

	var invalidRelationError *commands.InvalidRelationError
	if errors.As(err, &invalidRelationError) {
		checkError.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error}
		return checkError
	}

	var invalidTupleError *commands.InvalidTupleError
	if errors.As(err, &invalidTupleError) {
		checkError.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_invalid_tuple}
		return checkError
	}

	if errors.Is(err, graph.ErrResolutionDepthExceeded) {
		checkError.Code = &openfgav1.CheckError_InputError{
			InputError: openfgav1.ErrorCode_authorization_model_resolution_too_complex,
		}
		return checkError
	}

	if errors.Is(err, condition.ErrEvaluationFailed) {
		checkError.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error}
		return checkError
	}

	var throttledError *commands.ThrottledError
	if errors.As(err, &throttledError) {
		checkError.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error}
		return checkError
	}

	if errors.Is(err, context.DeadlineExceeded) {
		checkError.Code = &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_deadline_exceeded}
		return checkError
	}

	checkError.Code = &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_internal_error}
	return checkError
}
