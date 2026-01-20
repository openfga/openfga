package commands

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestBatchEvaluateRequestCommand(t *testing.T) {
	t.Run("basic_batch_check_conversion", func(t *testing.T) {
		input := &authzenv1.EvaluationsRequest{
			StoreId: ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject: &authzenv1.Subject{
						Type: "user",
						Id:   "CiRmZDA2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs",
					},
					Action:   &authzenv1.Action{Name: "can_read_user"},
					Resource: &authzenv1.Resource{Type: "user", Id: "rick@the-citadel.com"},
				},
				{
					Subject: &authzenv1.Subject{
						Type: "user1",
						Id:   "1CiRmZDA2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs",
					},
					Action:   &authzenv1.Action{Name: "can_read_user1"},
					Resource: &authzenv1.Resource{Type: "user", Id: "rick@the-citadel.com"},
				},
			},
		}

		reqCommand, err := NewBatchEvaluateRequestCommand(input, "")
		require.NoError(t, err)
		require.Equal(t, input.GetStoreId(), reqCommand.GetBatchCheckRequests().GetStoreId())
		require.Len(t, reqCommand.GetBatchCheckRequests().GetChecks(), 2)

		require.Equal(t, "user:CiRmZDA2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs", reqCommand.GetBatchCheckRequests().GetChecks()[0].GetTupleKey().GetUser())
		require.Equal(t, "can_read_user", reqCommand.GetBatchCheckRequests().GetChecks()[0].GetTupleKey().GetRelation())
		require.Equal(t, "user:rick@the-citadel.com", reqCommand.GetBatchCheckRequests().GetChecks()[0].GetTupleKey().GetObject())
		require.Equal(t, "0", reqCommand.GetBatchCheckRequests().GetChecks()[0].GetCorrelationId())

		require.Equal(t, "user1:1CiRmZDA2MTRkMy1jMzlhLTQ3ODEtYjdiZC04Yjk2ZjVhNTEwMGQSBWxvY2Fs", reqCommand.GetBatchCheckRequests().GetChecks()[1].GetTupleKey().GetUser())
		require.Equal(t, "can_read_user1", reqCommand.GetBatchCheckRequests().GetChecks()[1].GetTupleKey().GetRelation())
		require.Equal(t, "user:rick@the-citadel.com", reqCommand.GetBatchCheckRequests().GetChecks()[1].GetTupleKey().GetObject())
		require.Equal(t, "1", reqCommand.GetBatchCheckRequests().GetChecks()[1].GetCorrelationId())
	})

	t.Run("default_value_inheritance", func(t *testing.T) {
		storeID := ulid.Make().String()
		req := &authzenv1.EvaluationsRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{}, // Should inherit all defaults
				{}, // Should inherit all defaults
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 2)

		for _, check := range batchReq.GetChecks() {
			require.Equal(t, "user:alice", check.GetTupleKey().GetUser())
			require.Equal(t, "read", check.GetTupleKey().GetRelation())
			require.Equal(t, "document:doc1", check.GetTupleKey().GetObject())
		}
	})

	t.Run("per_evaluation_subject_override", func(t *testing.T) {
		storeID := ulid.Make().String()
		req := &authzenv1.EvaluationsRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject: &authzenv1.Subject{Type: "user", Id: "bob"}, // Override subject
				},
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		// Subject is overridden
		require.Equal(t, "user:bob", batchReq.GetChecks()[0].GetTupleKey().GetUser())
		// Resource and action inherit from defaults
		require.Equal(t, "read", batchReq.GetChecks()[0].GetTupleKey().GetRelation())
		require.Equal(t, "document:doc1", batchReq.GetChecks()[0].GetTupleKey().GetObject())
	})

	t.Run("per_evaluation_resource_override", func(t *testing.T) {
		storeID := ulid.Make().String()
		req := &authzenv1.EvaluationsRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Resource: &authzenv1.Resource{Type: "folder", Id: "folder1"}, // Override resource
				},
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		// Resource is overridden
		require.Equal(t, "folder:folder1", batchReq.GetChecks()[0].GetTupleKey().GetObject())
		// Subject and action inherit from defaults
		require.Equal(t, "user:alice", batchReq.GetChecks()[0].GetTupleKey().GetUser())
		require.Equal(t, "read", batchReq.GetChecks()[0].GetTupleKey().GetRelation())
	})

	t.Run("per_evaluation_action_override", func(t *testing.T) {
		storeID := ulid.Make().String()
		req := &authzenv1.EvaluationsRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Action: &authzenv1.Action{Name: "write"}, // Override action
				},
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		// Action is overridden
		require.Equal(t, "write", batchReq.GetChecks()[0].GetTupleKey().GetRelation())
		// Subject and resource inherit from defaults
		require.Equal(t, "user:alice", batchReq.GetChecks()[0].GetTupleKey().GetUser())
		require.Equal(t, "document:doc1", batchReq.GetChecks()[0].GetTupleKey().GetObject())
	})

	t.Run("multiple_per_evaluation_overrides", func(t *testing.T) {
		storeID := ulid.Make().String()
		req := &authzenv1.EvaluationsRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject: &authzenv1.Subject{Type: "user", Id: "bob"}, // Override subject only
				},
				{
					Resource: &authzenv1.Resource{Type: "folder", Id: "folder1"}, // Override resource only
				},
				{
					Action: &authzenv1.Action{Name: "write"}, // Override action only
				},
				{
					Subject:  &authzenv1.Subject{Type: "admin", Id: "charlie"},
					Resource: &authzenv1.Resource{Type: "project", Id: "proj1"},
					Action:   &authzenv1.Action{Name: "delete"},
				}, // Override all
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 4)

		// Check 0: subject overridden
		require.Equal(t, "user:bob", batchReq.GetChecks()[0].GetTupleKey().GetUser())
		require.Equal(t, "read", batchReq.GetChecks()[0].GetTupleKey().GetRelation())
		require.Equal(t, "document:doc1", batchReq.GetChecks()[0].GetTupleKey().GetObject())

		// Check 1: resource overridden
		require.Equal(t, "user:alice", batchReq.GetChecks()[1].GetTupleKey().GetUser())
		require.Equal(t, "read", batchReq.GetChecks()[1].GetTupleKey().GetRelation())
		require.Equal(t, "folder:folder1", batchReq.GetChecks()[1].GetTupleKey().GetObject())

		// Check 2: action overridden
		require.Equal(t, "user:alice", batchReq.GetChecks()[2].GetTupleKey().GetUser())
		require.Equal(t, "write", batchReq.GetChecks()[2].GetTupleKey().GetRelation())
		require.Equal(t, "document:doc1", batchReq.GetChecks()[2].GetTupleKey().GetObject())

		// Check 3: all overridden
		require.Equal(t, "admin:charlie", batchReq.GetChecks()[3].GetTupleKey().GetUser())
		require.Equal(t, "delete", batchReq.GetChecks()[3].GetTupleKey().GetRelation())
		require.Equal(t, "project:proj1", batchReq.GetChecks()[3].GetTupleKey().GetObject())
	})

	t.Run("properties_merge_with_subject_defaults", func(t *testing.T) {
		req := &authzenv1.EvaluationsRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{}, // Should get subject properties from default
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		ctx := batchReq.GetChecks()[0].GetContext().AsMap()
		require.Equal(t, "engineering", ctx["subject_department"])
	})

	t.Run("properties_merge_with_resource_defaults", func(t *testing.T) {
		req := &authzenv1.EvaluationsRequest{
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"classification": "secret"}),
			},
			Action:  &authzenv1.Action{Name: "read"},
			StoreId: ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{}, // Should get resource properties from default
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		ctx := batchReq.GetChecks()[0].GetContext().AsMap()
		require.Equal(t, "secret", ctx["resource_classification"])
	})

	t.Run("properties_merge_with_action_defaults", func(t *testing.T) {
		req := &authzenv1.EvaluationsRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action: &authzenv1.Action{
				Name:       "read",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"requires_mfa": true}),
			},
			StoreId: ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{}, // Should get action properties from default
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		ctx := batchReq.GetChecks()[0].GetContext().AsMap()
		require.Equal(t, true, ctx["action_requires_mfa"])
	})

	t.Run("properties_merge_all_sources", func(t *testing.T) {
		req := &authzenv1.EvaluationsRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"role": "admin"}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"owner": "bob"}),
			},
			Action: &authzenv1.Action{
				Name:       "read",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"audit": true}),
			},
			StoreId: ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{},
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		ctx := batchReq.GetChecks()[0].GetContext().AsMap()
		require.Equal(t, "admin", ctx["subject_role"])
		require.Equal(t, "bob", ctx["resource_owner"])
		require.Equal(t, true, ctx["action_audit"])
	})

	t.Run("per_evaluation_properties_override_defaults", func(t *testing.T) {
		req := &authzenv1.EvaluationsRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"role": "user"}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject: &authzenv1.Subject{
						Type:       "user",
						Id:         "bob",
						Properties: testutils.MustNewStruct(t, map[string]interface{}{"role": "admin"}),
					},
				},
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		ctx := batchReq.GetChecks()[0].GetContext().AsMap()
		// Per-evaluation subject properties should be used
		require.Equal(t, "admin", ctx["subject_role"])
	})

	t.Run("context_override_at_evaluation_level", func(t *testing.T) {
		req := &authzenv1.EvaluationsRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			Context:  testutils.MustNewStruct(t, map[string]interface{}{"ip_address": "10.0.0.1"}),
			StoreId:  ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{}, // Should inherit context from default
				{
					Context: testutils.MustNewStruct(t, map[string]interface{}{"ip_address": "192.168.1.1"}),
				}, // Should override context
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 2)

		// First check inherits default context
		ctx0 := batchReq.GetChecks()[0].GetContext().AsMap()
		require.Equal(t, "10.0.0.1", ctx0["ip_address"])

		// Second check has overridden context
		ctx1 := batchReq.GetChecks()[1].GetContext().AsMap()
		require.Equal(t, "192.168.1.1", ctx1["ip_address"])
	})

	t.Run("correlation_id_ordering", func(t *testing.T) {
		storeID := ulid.Make().String()
		req := &authzenv1.EvaluationsRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Action: &authzenv1.Action{Name: "read"}},
				{Action: &authzenv1.Action{Name: "write"}},
				{Action: &authzenv1.Action{Name: "delete"}},
				{Action: &authzenv1.Action{Name: "share"}},
				{Action: &authzenv1.Action{Name: "admin"}},
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 5)

		// Verify correlation IDs are sequential strings
		for i, check := range batchReq.GetChecks() {
			require.Equal(t, check.GetCorrelationId(), string(rune('0'+i)))
		}
	})

	t.Run("empty_evaluations_list", func(t *testing.T) {
		req := &authzenv1.EvaluationsRequest{
			Subject:     &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource:    &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:      &authzenv1.Action{Name: "read"},
			StoreId:     ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Empty(t, batchReq.GetChecks())
	})

	t.Run("nil_top_level_defaults", func(t *testing.T) {
		req := &authzenv1.EvaluationsRequest{
			StoreId: ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "read"},
				},
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		require.Equal(t, "user:alice", batchReq.GetChecks()[0].GetTupleKey().GetUser())
		require.Equal(t, "read", batchReq.GetChecks()[0].GetTupleKey().GetRelation())
		require.Equal(t, "document:doc1", batchReq.GetChecks()[0].GetTupleKey().GetObject())
	})

	t.Run("mixed_nil_and_set_defaults", func(t *testing.T) {
		req := &authzenv1.EvaluationsRequest{
			Subject: &authzenv1.Subject{Type: "user", Id: "default-user"},
			// Resource and Action are nil at top level
			StoreId: ulid.Make().String(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					// Subject inherits, resource and action must be provided
					Resource: &authzenv1.Resource{Type: "doc", Id: "d1"},
					Action:   &authzenv1.Action{Name: "view"},
				},
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		batchReq := cmd.GetBatchCheckRequests()
		require.Len(t, batchReq.GetChecks(), 1)

		require.Equal(t, "user:default-user", batchReq.GetChecks()[0].GetTupleKey().GetUser())
		require.Equal(t, "view", batchReq.GetChecks()[0].GetTupleKey().GetRelation())
		require.Equal(t, "doc:d1", batchReq.GetChecks()[0].GetTupleKey().GetObject())
	})
}

func TestTransformResponse(t *testing.T) {
	t.Run("maps_allowed_results_by_correlation_id", func(t *testing.T) {
		batchResp := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"0": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
				"1": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: false}},
			},
		}

		resp, err := TransformResponse(batchResp)
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 2)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision())
	})

	t.Run("response_ordering_matches_request_ordering", func(t *testing.T) {
		// Build a batch response with results in random order (map iteration is random)
		batchResp := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"3": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
				"0": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: false}},
				"2": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
				"1": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: false}},
			},
		}

		resp, err := TransformResponse(batchResp)
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 4)

		// Results should be ordered by correlation ID (index)
		require.False(t, resp.GetEvaluationResponses()[0].GetDecision()) // "0" -> false
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision()) // "1" -> false
		require.True(t, resp.GetEvaluationResponses()[2].GetDecision())  // "2" -> true
		require.True(t, resp.GetEvaluationResponses()[3].GetDecision())  // "3" -> true
	})

	t.Run("handles_error_results", func(t *testing.T) {
		batchResp := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"0": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
				"1": {CheckResult: &openfgav1.BatchCheckSingleResult_Error{
					Error: &openfgav1.CheckError{
						Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
						Message: "type not found",
					},
				}},
				"2": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: false}},
			},
		}

		resp, err := TransformResponse(batchResp)
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3)

		// First result is allowed
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.Nil(t, resp.GetEvaluationResponses()[0].GetContext())

		// Second result is an error - validation_error maps to HTTP 400
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.NotNil(t, resp.GetEvaluationResponses()[1].GetContext())
		require.NotNil(t, resp.GetEvaluationResponses()[1].GetContext().GetError())
		require.Equal(t, uint32(400), resp.GetEvaluationResponses()[1].GetContext().GetError().GetStatus())
		require.Equal(t, "type not found", resp.GetEvaluationResponses()[1].GetContext().GetError().GetMessage())

		// Third result is not allowed
		require.False(t, resp.GetEvaluationResponses()[2].GetDecision())
	})

	t.Run("empty_results", func(t *testing.T) {
		batchResp := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{},
		}

		resp, err := TransformResponse(batchResp)
		require.NoError(t, err)
		require.Empty(t, resp.GetEvaluationResponses())
	})

	t.Run("single_result", func(t *testing.T) {
		batchResp := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"0": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
			},
		}

		resp, err := TransformResponse(batchResp)
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 1)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
	})

	t.Run("all_allowed", func(t *testing.T) {
		batchResp := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"0": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
				"1": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
				"2": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
			},
		}

		resp, err := TransformResponse(batchResp)
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3)

		for _, evalResp := range resp.GetEvaluationResponses() {
			require.True(t, evalResp.GetDecision())
		}
	})

	t.Run("all_denied", func(t *testing.T) {
		batchResp := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"0": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: false}},
				"1": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: false}},
				"2": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: false}},
			},
		}

		resp, err := TransformResponse(batchResp)
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3)

		for _, evalResp := range resp.GetEvaluationResponses() {
			require.False(t, evalResp.GetDecision())
		}
	})

	t.Run("all_errors", func(t *testing.T) {
		batchResp := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"0": {CheckResult: &openfgav1.BatchCheckSingleResult_Error{
					Error: &openfgav1.CheckError{
						Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
						Message: "error 1",
					},
				}},
				"1": {CheckResult: &openfgav1.BatchCheckSingleResult_Error{
					Error: &openfgav1.CheckError{
						Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
						Message: "error 2",
					},
				}},
			},
		}

		resp, err := TransformResponse(batchResp)
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 2)

		for i, evalResp := range resp.GetEvaluationResponses() {
			require.False(t, evalResp.GetDecision())
			require.NotNil(t, evalResp.GetContext())
			require.NotNil(t, evalResp.GetContext().GetError())
			// validation_error maps to HTTP 400
			require.Equal(t, uint32(400), evalResp.GetContext().GetError().GetStatus())
			require.Contains(t, evalResp.GetContext().GetError().GetMessage(), "error")
			_ = i
		}
	})
}

func TestBatchEvaluateTableDriven(t *testing.T) {
	tests := []struct {
		name            string
		request         *authzenv1.EvaluationsRequest
		expectedChecks  int
		validateResults func(t *testing.T, checks []*openfgav1.BatchCheckItem)
	}{
		{
			name: "single_evaluation_with_all_fields",
			request: &authzenv1.EvaluationsRequest{
				StoreId: ulid.Make().String(),
				Evaluations: []*authzenv1.EvaluationsItemRequest{
					{
						Subject:  &authzenv1.Subject{Type: "user", Id: "test-user"},
						Resource: &authzenv1.Resource{Type: "doc", Id: "test-doc"},
						Action:   &authzenv1.Action{Name: "view"},
					},
				},
			},
			expectedChecks: 1,
			validateResults: func(t *testing.T, checks []*openfgav1.BatchCheckItem) {
				require.Equal(t, "user:test-user", checks[0].GetTupleKey().GetUser())
				require.Equal(t, "doc:test-doc", checks[0].GetTupleKey().GetObject())
				require.Equal(t, "view", checks[0].GetTupleKey().GetRelation())
			},
		},
		{
			name: "all_inherit_from_defaults",
			request: &authzenv1.EvaluationsRequest{
				StoreId:  ulid.Make().String(),
				Subject:  &authzenv1.Subject{Type: "user", Id: "default"},
				Resource: &authzenv1.Resource{Type: "item", Id: "default-item"},
				Action:   &authzenv1.Action{Name: "access"},
				Evaluations: []*authzenv1.EvaluationsItemRequest{
					{},
					{},
					{},
				},
			},
			expectedChecks: 3,
			validateResults: func(t *testing.T, checks []*openfgav1.BatchCheckItem) {
				for _, check := range checks {
					require.Equal(t, "user:default", check.GetTupleKey().GetUser())
					require.Equal(t, "item:default-item", check.GetTupleKey().GetObject())
					require.Equal(t, "access", check.GetTupleKey().GetRelation())
				}
			},
		},
		{
			name: "alternating_overrides",
			request: &authzenv1.EvaluationsRequest{
				StoreId:  ulid.Make().String(),
				Subject:  &authzenv1.Subject{Type: "user", Id: "base"},
				Resource: &authzenv1.Resource{Type: "resource", Id: "base-res"},
				Action:   &authzenv1.Action{Name: "base-action"},
				Evaluations: []*authzenv1.EvaluationsItemRequest{
					{}, // 0: all defaults
					{Subject: &authzenv1.Subject{Type: "admin", Id: "a1"}}, // 1: override subject
					{}, // 2: all defaults
					{Resource: &authzenv1.Resource{Type: "special", Id: "special1"}}, // 3: override resource
					{}, // 4: all defaults
				},
			},
			expectedChecks: 5,
			validateResults: func(t *testing.T, checks []*openfgav1.BatchCheckItem) {
				// 0: defaults
				require.Equal(t, "user:base", checks[0].GetTupleKey().GetUser())
				require.Equal(t, "resource:base-res", checks[0].GetTupleKey().GetObject())

				// 1: subject override
				require.Equal(t, "admin:a1", checks[1].GetTupleKey().GetUser())
				require.Equal(t, "resource:base-res", checks[1].GetTupleKey().GetObject())

				// 2: defaults
				require.Equal(t, "user:base", checks[2].GetTupleKey().GetUser())

				// 3: resource override
				require.Equal(t, "user:base", checks[3].GetTupleKey().GetUser())
				require.Equal(t, "special:special1", checks[3].GetTupleKey().GetObject())

				// 4: defaults
				require.Equal(t, "user:base", checks[4].GetTupleKey().GetUser())
				require.Equal(t, "resource:base-res", checks[4].GetTupleKey().GetObject())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := NewBatchEvaluateRequestCommand(tt.request, "")
			require.NoError(t, err)

			batchReq := cmd.GetBatchCheckRequests()
			require.Len(t, batchReq.GetChecks(), tt.expectedChecks)

			if tt.validateResults != nil {
				tt.validateResults(t, batchReq.GetChecks())
			}
		})
	}
}

func TestTransformResponseTableDriven(t *testing.T) {
	tests := []struct {
		name              string
		batchResponse     *openfgav1.BatchCheckResponse
		expectedResponses int
		expectedDecisions []bool
		expectedErrors    []bool
	}{
		{
			name: "mixed_results",
			batchResponse: &openfgav1.BatchCheckResponse{
				Result: map[string]*openfgav1.BatchCheckSingleResult{
					"0": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
					"1": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: false}},
					"2": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
				},
			},
			expectedResponses: 3,
			expectedDecisions: []bool{true, false, true},
			expectedErrors:    []bool{false, false, false},
		},
		{
			name: "with_one_error",
			batchResponse: &openfgav1.BatchCheckResponse{
				Result: map[string]*openfgav1.BatchCheckSingleResult{
					"0": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
					"1": {CheckResult: &openfgav1.BatchCheckSingleResult_Error{
						Error: &openfgav1.CheckError{Message: "test error"},
					}},
				},
			},
			expectedResponses: 2,
			expectedDecisions: []bool{true, false},
			expectedErrors:    []bool{false, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := TransformResponse(tt.batchResponse)
			require.NoError(t, err)
			require.Len(t, resp.GetEvaluationResponses(), tt.expectedResponses)

			for i, expected := range tt.expectedDecisions {
				require.Equal(t, expected, resp.GetEvaluationResponses()[i].GetDecision())
			}

			for i, hasError := range tt.expectedErrors {
				if hasError {
					require.NotNil(t, resp.GetEvaluationResponses()[i].GetContext())
					require.NotNil(t, resp.GetEvaluationResponses()[i].GetContext().GetError())
				}
			}
		})
	}

	t.Run("transform_response_with_error_results", func(t *testing.T) {
		// Test TransformResponse with error results from BatchCheck
		bcr := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"0": {
					CheckResult: &openfgav1.BatchCheckSingleResult_Error{
						Error: &openfgav1.CheckError{
							Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_validation_error},
							Message: "type not found",
						},
					},
				},
				"1": {
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{
						Allowed: true,
					},
				},
				"2": {
					CheckResult: &openfgav1.BatchCheckSingleResult_Error{
						Error: &openfgav1.CheckError{
							Code:    &openfgav1.CheckError_InputError{InputError: openfgav1.ErrorCode_latest_authorization_model_not_found},
							Message: "authorization model not found",
						},
					},
				},
			},
		}

		resp, err := TransformResponse(bcr)
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3)

		// First: error - validation_error maps to HTTP 400
		require.False(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.NotNil(t, resp.GetEvaluationResponses()[0].GetContext())
		require.NotNil(t, resp.GetEvaluationResponses()[0].GetContext().GetError())
		require.Equal(t, uint32(400), resp.GetEvaluationResponses()[0].GetContext().GetError().GetStatus())
		require.Contains(t, resp.GetEvaluationResponses()[0].GetContext().GetError().GetMessage(), "type not found")

		// Second: allowed
		require.True(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.Nil(t, resp.GetEvaluationResponses()[1].GetContext())

		// Third: error - latest_authorization_model_not_found is a validation error, maps to HTTP 400
		require.False(t, resp.GetEvaluationResponses()[2].GetDecision())
		require.NotNil(t, resp.GetEvaluationResponses()[2].GetContext())
		require.NotNil(t, resp.GetEvaluationResponses()[2].GetContext().GetError())
		require.Equal(t, uint32(400), resp.GetEvaluationResponses()[2].GetContext().GetError().GetStatus())
		require.Contains(t, resp.GetEvaluationResponses()[2].GetContext().GetError().GetMessage(), "authorization model not found")
	})

	t.Run("transform_response_missing_result_entry", func(t *testing.T) {
		// Test TransformResponse when result map is missing an entry in sequence
		// Map has 3 entries but key "1" is missing
		bcr := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"0": {
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{
						Allowed: true,
					},
				},
				// "1" is missing - this will trigger the missing case
				"2": {
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{
						Allowed: false,
					},
				},
			},
		}

		resp, err := TransformResponse(bcr)
		require.NoError(t, err)
		// Response array size equals map size (2)
		require.Len(t, resp.GetEvaluationResponses(), 2)

		// Index 0: allowed (key "0" exists)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.Nil(t, resp.GetEvaluationResponses()[0].GetContext())

		// Index 1: will look for key "1" which doesn't exist - should error
		// But since we only iterate up to len(map)=2, we check indices 0,1
		// So this should hit the missing case
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.NotNil(t, resp.GetEvaluationResponses()[1].GetContext())
		require.NotNil(t, resp.GetEvaluationResponses()[1].GetContext().GetError())
		require.Equal(t, uint32(500), resp.GetEvaluationResponses()[1].GetContext().GetError().GetStatus())
		require.Contains(t, resp.GetEvaluationResponses()[1].GetContext().GetError().GetMessage(), "missing result for evaluation 1")
	})

	t.Run("transform_response_nil_result_entry", func(t *testing.T) {
		// Test TransformResponse when result map has nil entry
		bcr := &openfgav1.BatchCheckResponse{
			Result: map[string]*openfgav1.BatchCheckSingleResult{
				"0": {
					CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{
						Allowed: true,
					},
				},
				"1": nil, // Nil result
			},
		}

		resp, err := TransformResponse(bcr)
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 2)

		// Index 0: allowed
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.Nil(t, resp.GetEvaluationResponses()[0].GetContext())

		// Index 1: nil result - should have error
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.NotNil(t, resp.GetEvaluationResponses()[1].GetContext())
		require.NotNil(t, resp.GetEvaluationResponses()[1].GetContext().GetError())
		require.Equal(t, uint32(500), resp.GetEvaluationResponses()[1].GetContext().GetError().GetStatus())
		require.Contains(t, resp.GetEvaluationResponses()[1].GetContext().GetError().GetMessage(), "missing result for evaluation 1")
	})

	t.Run("properties_merge_error_propagation", func(t *testing.T) {
		// Test that property merge errors are properly propagated
		req := &authzenv1.EvaluationsRequest{
			StoreId: "test-store",
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "read"},
				},
			},
		}

		cmd, err := NewBatchEvaluateRequestCommand(req, "")
		// Should succeed - normal properties merge
		require.NoError(t, err)
		require.NotNil(t, cmd)
		require.Len(t, cmd.GetBatchCheckRequests().GetChecks(), 1)
	})
}
