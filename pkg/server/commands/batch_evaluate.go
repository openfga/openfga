package commands

import (
	"fmt"
	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type BatchEvaluateRequestCommand struct {
	checkParams []*openfgav1.CheckRequest
}

func (cmd *BatchEvaluateRequestCommand) GetCheckRequests() []*openfgav1.CheckRequest {
	return cmd.checkParams
}

func NewBatchEvaluateRequestCommand(req *authzenv1.EvaluationRequest) *BatchEvaluateRequestCommand {
	cmd := &BatchEvaluateRequestCommand{
		// TODO
	}
	return cmd
}

func TransformResponse(bcr *openfgav1.BatchCheckResponse) *authzenv1.EvaluationsResponse {
	evaluationsResponse := &authzenv1.EvaluationsResponse{
		EvaluationResponses: make([]*authzenv1.EvaluationResponse, len(bcr.Result)),
	}

	for i := range evaluationsResponse.EvaluationResponses {
		evaluationsResponse.EvaluationResponses[i] = &authzenv1.EvaluationResponse{
			Decision: bcr.Result[fmt.Sprintf("%d", i)].GetAllowed(),
		}
	}

	return evaluationsResponse
}
