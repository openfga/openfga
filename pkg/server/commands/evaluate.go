package commands

import (
	"fmt"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type EvaluateRequestCommand struct {
	checkParams openfgav1.CheckRequest
}

func (cmd *EvaluateRequestCommand) GetCheckRequest() *openfgav1.CheckRequest {
	return &cmd.checkParams
}

func NewEvaluateRequestCommand(req *authzenv1.EvaluationRequest) (*EvaluateRequestCommand, error) {
	mergedContext, err := MergePropertiesToContext(
		req.GetContext(),
		req.GetSubject(),
		req.GetResource(),
		req.GetAction(),
	)
	if err != nil {
		return nil, err
	}

	cmd := &EvaluateRequestCommand{
		checkParams: openfgav1.CheckRequest{
			StoreId:              req.GetStoreId(),
			AuthorizationModelId: req.GetAuthorizationModelId(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     fmt.Sprintf("%s:%s", req.GetSubject().GetType(), req.GetSubject().GetId()),
				Relation: req.GetAction().GetName(),
				Object:   fmt.Sprintf("%s:%s", req.GetResource().GetType(), req.GetResource().GetId()),
			},
			Context: mergedContext,
		},
	}
	return cmd, nil
}
