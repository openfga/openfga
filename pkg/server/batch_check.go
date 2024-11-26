package server

import (
	"context"
	"errors"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/authz"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/telemetry"
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

	cmd := commands.NewBatchCheckCommand(
		s.client,
		commands.WithBatchCheckCommandLogger(s.logger),
		commands.WithBatchCheckMaxChecksPerBatch(s.maxChecksPerBatchCheck),
		commands.WithBatchCheckMaxConcurrentChecks(s.maxConcurrentChecksPerBatch),
	)

	result, metadata, err := cmd.Execute(ctx, &commands.BatchCheckCommandParams{
		AuthorizationModelID: req.GetAuthorizationModelId(),
		Checks:               req.GetChecks(),
		Consistency:          req.GetConsistency(),
		StoreID:              req.GetStoreId(),
	})

	if err != nil {
		telemetry.TraceError(span, err)
		var batchValidationError *commands.BatchCheckValidationError
		if errors.As(err, &batchValidationError) {
			return nil, serverErrors.ValidationError(err)
		}

		return nil, err
	}

	methodName := "batchCheck"
	queryCount := float64(metadata.DatastoreQueryCount)
	span.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, queryCount))
	datastoreQueryCountHistogram.WithLabelValues(
		s.serviceName,
		methodName,
	).Observe(queryCount)

	grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, metadata.DatastoreQueryCount)

	return &openfgav1.BatchCheckResponse{Result: transformCheckResultToProto(result)}, nil
}

// transformCheckResultToProto transform the internal BatchCheckOutcome into the external-facing
// BatchCheckSingleResult struct for transmission back via the api.
func transformCheckResultToProto(checkResults map[commands.CorrelationID]*commands.BatchCheckOutcome) map[string]*openfgav1.BatchCheckSingleResult {
	var batchResult = map[string]*openfgav1.BatchCheckSingleResult{}
	for k, v := range checkResults {
		singleResult := &openfgav1.BatchCheckSingleResult{}

		if v.Err != nil {
			singleResult.CheckResult = &openfgav1.BatchCheckSingleResult_Error{
				Error: transformCheckCommandErrorToBatchCheckError(v.Err),
			}
		} else {
			singleResult.CheckResult = &openfgav1.BatchCheckSingleResult_Allowed{
				Allowed: v.CheckResponse.GetAllowed(),
			}
		}

		batchResult[string(k)] = singleResult
	}

	return batchResult
}

func transformCheckCommandErrorToBatchCheckError(cmdErr error) *openfgav1.CheckError {
	err := &openfgav1.CheckError{Message: cmdErr.Error()}
	var internalError *serverErrors.InternalError

	// map the possible errors to their specific GRPC codes in the proto definition
	if statusErr := status.Convert(cmdErr); statusErr != nil {
		err.Message = statusErr.Message()
		switch {
		case statusErr.Code() == codes.Code(openfgav1.ErrorCode_validation_error):
			err.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error}
		case statusErr.Code() == codes.Code(openfgav1.ErrorCode_invalid_tuple):
			err.Code = &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_invalid_tuple}
		case errors.Is(statusErr.Err(), serverErrors.ErrThrottledTimeout):
			err.Code = &openfgav1.CheckError_InputError{
				InputError: openfgav1.ErrorCode_validation_error,
			}
		case errors.Is(statusErr.Err(), serverErrors.ErrAuthorizationModelResolutionTooComplex):
			err.Code = &openfgav1.CheckError_InputError{
				InputError: openfgav1.ErrorCode_authorization_model_resolution_too_complex,
			}
		case errors.Is(statusErr.Err(), serverErrors.ErrRequestDeadlineExceeded):
			err.Code = &openfgav1.CheckError_InternalError{
				InternalError: openfgav1.InternalErrorCode(statusErr.Code()),
			}
		default:
			err.Code = &openfgav1.CheckError_InternalError{
				InternalError: openfgav1.InternalErrorCode_internal_error,
			}
		}
	} else if ok := errors.As(cmdErr, &internalError); ok {
		err.Code = &openfgav1.CheckError_InternalError{
			InternalError: err.GetInternalError(),
		}
	}

	return err
}
