package commands

import (
	"testing"

	"github.com/oklog/ulid/v2"
	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	"github.com/stretchr/testify/assert"
)

func TestBatchEvaluate(t *testing.T) {
	t.Run("test_build", func(t *testing.T) {
		input := &authzenv1.EvaluationsRequest{

			StoreId: ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject: &authzenv1.Subject{
						Type: "user",
						Id:   "CiRmZDA2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs",
					},
					Action: &authzenv1.Action{Name: "can_read_user"},
					Resource: &authzenv1.Resource{
						Type: "user",
						Id:   "rick@the-citadel.com",
					},
				},
				{
					Subject: &authzenv1.Subject{
						Type: "user1",
						Id:   "1CiRmZDA2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs",
					},
					Action: &authzenv1.Action{Name: "can_read_user1"},
					Resource: &authzenv1.Resource{
						Type: "user",
						Id:   "rick@the-citadel.com",
					},
				},
			},
		}

		reqCommand := NewBatchEvaluateRequestCommand(input)
		assert.Equal(t, input.GetStoreId(), reqCommand.GetBatchCheckRequests().GetStoreId())
		assert.Equal(t, 2, len(reqCommand.GetBatchCheckRequests().Checks))

		assert.Equal(t, "user:CiRmZDA2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs", reqCommand.GetBatchCheckRequests().Checks[0].GetTupleKey().GetUser())
		assert.Equal(t, "can_read_user", reqCommand.GetBatchCheckRequests().Checks[0].GetTupleKey().GetRelation())
		assert.Equal(t, "user:rick@the-citadel.com", reqCommand.GetBatchCheckRequests().Checks[0].GetTupleKey().GetObject())
		assert.Equal(t, "0", reqCommand.GetBatchCheckRequests().Checks[0].GetCorrelationId())

		assert.Equal(t, "user1:1CiRmZDA2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs", reqCommand.GetBatchCheckRequests().Checks[1].GetTupleKey().GetUser())
		assert.Equal(t, "can_read_user1", reqCommand.GetBatchCheckRequests().Checks[1].GetTupleKey().GetRelation())
		assert.Equal(t, "user:rick@the-citadel.com", reqCommand.GetBatchCheckRequests().Checks[1].GetTupleKey().GetObject())
		assert.Equal(t, "1", reqCommand.GetBatchCheckRequests().Checks[1].GetCorrelationId())
	})

	t.Run("test_transform", func(t *testing.T) {
		// TODO
	})
}
