package server

import (
	"context"
	"errors"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/middleware/validator"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/graph"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/authz"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *Server) BatchCheck(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
	ctx, span := tracer.Start(ctx, authz.BatchCheck, trace.WithAttributes(
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

	err := s.checkAuthz(ctx, req.GetStoreId(), authz.BatchCheck)
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

	result, metadata, err := cmd.Execute(ctx, &commands.BatchCheckCommandParams{
		AuthorizationModelID: req.GetAuthorizationModelId(),
		Checks:               req.GetChecks(),
		Consistency:          req.GetConsistency(),
		StoreID:              storeID,
	})

	if err != nil {
		return nil, err
	}

	grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, metadata.TotalQueries)

	return &openfgav1.BatchCheckResponse{Result: transformCheckResultToRPC(result)}, nil
}

// transformCheckResultToRPC transform the internal BatchCheckOutcome into the external-facing
// BatchCheckSingleResult struct for transmission back via the api.
func transformCheckResultToRPC(checkResults map[commands.CorrelationID]*commands.BatchCheckOutcome) map[string]*openfgav1.BatchCheckSingleResult {
	var batchResult = map[string]*openfgav1.BatchCheckSingleResult{}
	for k, v := range checkResults {
		singleResult := &openfgav1.BatchCheckSingleResult{}

		if v.Err != nil {
			singleResult.CheckResult = &openfgav1.BatchCheckSingleResult_Error{
				Error: transformCheckCommandErrorToBatchCheckError(v.Err),
			}
		} else {
			singleResult.CheckResult = &openfgav1.BatchCheckSingleResult_Allowed{
				Allowed: v.CheckResponse.Allowed,
			}
		}

		batchResult[string(k)] = singleResult
	}

	return batchResult
}

func transformCheckCommandErrorToBatchCheckError(cmdErr error) *openfgav1.CheckError {
	var invalidRelationError *commands.InvalidRelationError
	var invalidTupleError *commands.InvalidTupleError
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
	case errors.As(cmdErr, &throttledError):
		err.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error}
	case errors.Is(cmdErr, context.DeadlineExceeded):
		err.Code = &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_deadline_exceeded}
	default:
		err.Code = &openfgav1.CheckError_InternalError{InternalError: openfgav1.InternalErrorCode_internal_error}
	}

	return err
}
