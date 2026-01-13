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
	var user, relation, object string
	subject := req.GetSubject()
	if subject != nil {
		user = fmt.Sprintf("%s:%s", subject.GetType(), subject.GetId())
	} else {
		user = ":"
	}

	resource := req.GetResource()
	if resource != nil {
		object = fmt.Sprintf("%s:%s", resource.GetType(), resource.GetId())
	} else {
		object = ":"
	}

	action := req.GetAction()
	if action != nil {
		relation = action.GetName()
	} else {
		relation = ""
	}

	mergedContext, err := MergePropertiesToContext(
		req.GetContext(),
		subject,
		resource,
		action,
	)
	if err != nil {
		return nil, err
	}

	cmd := &EvaluateRequestCommand{
		checkParams: openfgav1.CheckRequest{
			StoreId:              req.GetStoreId(),
			AuthorizationModelId: req.GetAuthorizationModelId(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     user,
				Relation: relation,
				Object:   object,
			},
			Context: mergedContext,
		},
	}
	return cmd, nil
}
