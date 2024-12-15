package commands

import (
	"fmt"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
)

type BatchEvaluateRequestCommand struct {
	batchCheckParams *openfgav1.BatchCheckRequest
}

func (cmd *BatchEvaluateRequestCommand) GetBatchCheckRequests() *openfgav1.BatchCheckRequest {
	return cmd.batchCheckParams
}

func NewBatchEvaluateRequestCommand(req *authzenv1.EvaluationsRequest) *BatchEvaluateRequestCommand {
	cmd := &BatchEvaluateRequestCommand{
		batchCheckParams: &openfgav1.BatchCheckRequest{
			StoreId: req.GetStoreId(),
			Checks:  []*openfgav1.BatchCheckItem{},
		},
	}

	resource := req.GetResource()
	action := req.GetAction()
	subject := req.GetSubject()
	context := req.GetContext()

	for counter, evaluation := range req.GetEvaluations() {
		batchCheckItem := &openfgav1.BatchCheckItem{
			TupleKey:      &openfgav1.CheckRequestTupleKey{},
			CorrelationId: fmt.Sprintf("%d", counter),
		}

		if evaluation.GetAction() == nil {
			batchCheckItem.TupleKey.Relation = action.GetName()
		} else {
			batchCheckItem.TupleKey.Relation = evaluation.GetAction().GetName()
		}

		if evaluation.GetResource() == nil {
			batchCheckItem.TupleKey.Object = fmt.Sprintf("%s:%s", resource.GetType(), resource.GetId())
		} else {
			batchCheckItem.TupleKey.Object = fmt.Sprintf("%s:%s", evaluation.GetResource().GetType(), evaluation.GetResource().GetId())
		}

		if evaluation.GetSubject() == nil {
			batchCheckItem.TupleKey.User = fmt.Sprintf("%s:%s", subject.GetType(), subject.GetId())
		} else {
			batchCheckItem.TupleKey.User = fmt.Sprintf("%s:%s", evaluation.GetSubject().GetType(), evaluation.GetSubject().GetId())
		}

		if evaluation.GetContext() == nil {
			batchCheckItem.Context = context
		} else {
			batchCheckItem.Context = evaluation.GetContext()
		}

		cmd.batchCheckParams.Checks = append(cmd.batchCheckParams.Checks, batchCheckItem)
	}

	return cmd
}

func TransformResponse(bcr *openfgav1.BatchCheckResponse) (*authzenv1.EvaluationsResponse, error) {
	evaluationsResponse := &authzenv1.EvaluationsResponse{
		EvaluationResponses: make([]*authzenv1.EvaluationResponse, len(bcr.Result)),
	}

	for i := range evaluationsResponse.EvaluationResponses {
		result := bcr.Result[fmt.Sprintf("%d", i)]

		if errResult, ok := result.CheckResult.(*openfgav1.BatchCheckSingleResult_Error); ok {
			// Directly check the Error field and return the appropriate error
			return nil, serverErrors.ValidationError(fmt.Errorf(errResult.Error.Message))
		}

		evaluationsResponse.EvaluationResponses[i] = &authzenv1.EvaluationResponse{
			Decision: result.GetAllowed(),
		}
	}

	return evaluationsResponse, nil
}
