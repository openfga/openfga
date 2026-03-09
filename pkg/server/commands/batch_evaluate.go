package commands

import (
	"fmt"
	"strconv"

	"google.golang.org/protobuf/types/known/structpb"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	servererrors "github.com/openfga/openfga/pkg/server/errors"
)

// errorContext builds an arbitrary JSON context object for error responses,
// per the AuthZen spec where context is a free-form JSON object.
func errorContext(status uint32, message string) *structpb.Struct {
	ctx, _ := structpb.NewStruct(map[string]interface{}{
		"error": map[string]interface{}{
			"status":  status,
			"message": message,
		},
	})
	return ctx
}

type BatchEvaluateRequestCommand struct {
	batchCheckParams *openfgav1.BatchCheckRequest
}

func (cmd *BatchEvaluateRequestCommand) GetBatchCheckRequests() *openfgav1.BatchCheckRequest {
	return cmd.batchCheckParams
}

func NewBatchEvaluateRequestCommand(req *authzenv1.EvaluationsRequest, authorizationModelID string) (*BatchEvaluateRequestCommand, error) {
	cmd := &BatchEvaluateRequestCommand{
		batchCheckParams: &openfgav1.BatchCheckRequest{
			StoreId:              req.GetStoreId(),
			AuthorizationModelId: authorizationModelID,
			Checks:               []*openfgav1.BatchCheckItem{},
		},
	}

	// Top-level defaults
	topLevelResource := req.GetResource()
	topLevelAction := req.GetAction()
	topLevelSubject := req.GetSubject()
	topLevelContext := req.GetContext()

	for counter, evaluation := range req.GetEvaluations() {
		batchCheckItem := &openfgav1.BatchCheckItem{
			TupleKey:      &openfgav1.CheckRequestTupleKey{},
			CorrelationId: strconv.Itoa(counter),
		}

		// Resolve effective subject (evaluation overrides top-level)
		var effectiveSubject *authzenv1.Subject
		if evaluation.GetSubject() != nil {
			effectiveSubject = evaluation.GetSubject()
		} else {
			effectiveSubject = topLevelSubject
		}

		// Resolve effective resource (evaluation overrides top-level)
		var effectiveResource *authzenv1.Resource
		if evaluation.GetResource() != nil {
			effectiveResource = evaluation.GetResource()
		} else {
			effectiveResource = topLevelResource
		}

		// Resolve effective action (evaluation overrides top-level)
		var effectiveAction *authzenv1.Action
		if evaluation.GetAction() != nil {
			effectiveAction = evaluation.GetAction()
		} else {
			effectiveAction = topLevelAction
		}

		// Resolve effective context (evaluation overrides top-level)
		var effectiveContext *structpb.Struct
		if evaluation.GetContext() != nil {
			effectiveContext = evaluation.GetContext()
		} else {
			effectiveContext = topLevelContext
		}

		// Set tuple key values
		if effectiveAction != nil {
			batchCheckItem.TupleKey.Relation = effectiveAction.GetName()
		}

		if effectiveResource != nil {
			batchCheckItem.TupleKey.Object = fmt.Sprintf("%s:%s", effectiveResource.GetType(), effectiveResource.GetId())
		}

		if effectiveSubject != nil {
			batchCheckItem.TupleKey.User = fmt.Sprintf("%s:%s", effectiveSubject.GetType(), effectiveSubject.GetId())
		}

		// Merge properties from subject/resource/action into context
		mergedContext, err := MergePropertiesToContext(
			effectiveContext,
			effectiveSubject,
			effectiveResource,
			effectiveAction,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to merge properties to context for evaluation %d: %w", counter, err)
		}
		batchCheckItem.Context = mergedContext

		cmd.batchCheckParams.Checks = append(cmd.batchCheckParams.Checks, batchCheckItem)
	}

	return cmd, nil
}

func TransformResponse(bcr *openfgav1.BatchCheckResponse) (*authzenv1.EvaluationsResponse, error) {
	evaluationsResponse := &authzenv1.EvaluationsResponse{
		Evaluations: make([]*authzenv1.EvaluationResponse, len(bcr.GetResult())),
	}

	for i := range evaluationsResponse.GetEvaluations() {
		key := strconv.Itoa(i)
		result, ok := bcr.GetResult()[key]
		if !ok || result == nil {
			// Missing result in map - return error response
			evaluationsResponse.Evaluations[i] = &authzenv1.EvaluationResponse{
				Decision: false,
				Context:  errorContext(500, fmt.Sprintf("missing result for evaluation %d", i)),
			}
			continue
		}

		if errResult, ok := result.GetCheckResult().(*openfgav1.BatchCheckSingleResult_Error); ok {
			// If there's an error, we return it as part of a single
			// evaluation response, the rest of the items of the batch
			// should not include the error
			httpStatus := uint32(500)
			if errResult.Error != nil {
				// Extract error code based on type (InputError or InternalError)
				switch code := errResult.Error.GetCode().(type) {
				case *openfgav1.CheckError_InputError:
					// Input errors use OpenFGA ErrorCode (e.g., validation_error = 2000)
					encodedErr := servererrors.NewEncodedError(int32(code.InputError), errResult.Error.GetMessage())
					httpStatus = uint32(encodedErr.HTTPStatus())
				case *openfgav1.CheckError_InternalError:
					// Internal errors (e.g., deadline_exceeded) map to 500
					httpStatus = 500
				}
			}
			evaluationsResponse.Evaluations[i] = &authzenv1.EvaluationResponse{
				Decision: false,
				Context:  errorContext(httpStatus, errResult.Error.GetMessage()),
			}
		} else {
			evaluationsResponse.Evaluations[i] = &authzenv1.EvaluationResponse{
				Decision: result.GetAllowed(),
			}
		}
	}

	return evaluationsResponse, nil
}
