package commands

import (
	"testing"

	"github.com/oklog/ulid/v2"
	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	"github.com/stretchr/testify/assert"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestEvaluate(t *testing.T) {
	t.Run("test_build", func(t *testing.T) {
		input := &authzenv1.EvaluationRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "maria",
				Properties: nil,
			},
			Action: &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{
				Type: "repo",
				Id:   "fga",
			},
			Context:              testutils.MustNewStruct(t, map[string]interface{}{"param": true}),
			StoreId:              ulid.Make().String(),
			AuthorizationModelId: ulid.Make().String(),
		}

		reqCommand := NewEvaluateRequestCommand(input)
		assert.Equal(t, input.GetStoreId(), reqCommand.GetCheckRequest().GetStoreId())
		assert.Equal(t, input.GetAuthorizationModelId(), reqCommand.GetCheckRequest().GetAuthorizationModelId())
		assert.Equal(t, "user:maria", reqCommand.GetCheckRequest().GetTupleKey().GetUser())
		assert.Equal(t, "read", reqCommand.GetCheckRequest().GetTupleKey().GetRelation())
		assert.Equal(t, "repo:fga", reqCommand.GetCheckRequest().GetTupleKey().GetObject())
		assert.Equal(t, true, reqCommand.GetCheckRequest().GetContext().AsMap()["param"])
	})
}
