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

func NewEvaluateRequestCommand(req *authzenv1.EvaluationRequest, authorizationModelID string) (*EvaluateRequestCommand, error) {
	subject := req.GetSubject()
	if subject == nil {
		return nil, fmt.Errorf("missing subject")
	}

	resource := req.GetResource()
	if resource == nil {
		return nil, fmt.Errorf("missing resource")
	}

	action := req.GetAction()
	if action == nil {
		return nil, fmt.Errorf("missing action")
	}

	user := fmt.Sprintf("%s:%s", subject.GetType(), subject.GetId())
	object := fmt.Sprintf("%s:%s", resource.GetType(), resource.GetId())
	relation := action.GetName()

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
			AuthorizationModelId: authorizationModelID,
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
