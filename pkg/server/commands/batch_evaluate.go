package commands

import (
	"fmt"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type BatchEvaluateRequestCommand struct {
	batchCheckParams *openfgav1.BatchCheckRequest
}

func (cmd *BatchEvaluateRequestCommand) GetBatchCheckRequests() *openfgav1.BatchCheckRequest {
	return cmd.batchCheckParams
}

func NewBatchEvaluateRequestCommand(req *authzenv1.EvaluationsRequest) (*BatchEvaluateRequestCommand, error) {
	cmd := &BatchEvaluateRequestCommand{
		batchCheckParams: &openfgav1.BatchCheckRequest{
			StoreId: req.GetStoreId(),
			Checks:  []*openfgav1.BatchCheckItem{},
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
			CorrelationId: fmt.Sprintf("%d", counter),
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
		EvaluationResponses: make([]*authzenv1.EvaluationResponse, len(bcr.Result)),
	}

	for i := range evaluationsResponse.EvaluationResponses {
		result := bcr.Result[fmt.Sprintf("%d", i)]

		if errResult, ok := result.CheckResult.(*openfgav1.BatchCheckSingleResult_Error); ok {
			// If there's an error, we return it as part of a single
			// evaluation response, the rest of the items of the batch
			// should not include the error
			evaluationsResponse.EvaluationResponses[i] = &authzenv1.EvaluationResponse{
				Decision: false,
				Context: &authzenv1.EvaluationResponseContext{
					Error: &authzenv1.ResponseContextError{
						Status:  404,
						Message: errResult.Error.Message,
					},
				},
			}
		} else {
			evaluationsResponse.EvaluationResponses[i] = &authzenv1.EvaluationResponse{
				Decision: result.GetAllowed(),
			}
		}
	}

	return evaluationsResponse, nil
}
