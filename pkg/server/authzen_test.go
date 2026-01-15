package server

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/util"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
)

func TestEvaluation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("success_with_valid_model", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write a minimal model
		_, err = s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Write tuple to grant permission
		_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						Object:   "document:doc1",
						Relation: "reader",
						User:     "user:alice",
					},
				},
			},
		})
		require.NoError(t, err)

		// Test Evaluation success path - this covers lines 54-56
		req := &authzenv1.EvaluationRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		}

		resp, err := s.Evaluation(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetDecision())
	})

	t.Run("feature_flag_disabled", func(t *testing.T) {
		// Server without AuthZEN experimental enabled
		s := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(s.Close)

		req := &authzenv1.EvaluationRequest{
			StoreId:  ulid.Make().String(),
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
		}

		resp, err := s.Evaluation(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Unimplemented, st.Code())
		require.Contains(t, st.Message(), "experimental")
	})

	t.Run("model_not_found", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store without model
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.EvaluationRequest{
			StoreId:  createStoreResp.GetId(),
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
		}

		resp, err := s.Evaluation(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_latest_authorization_model_not_found), st.Code())
	})

	t.Run("validation_error_missing_subject", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.EvaluationRequest{
			StoreId:  createStoreResp.GetId(),
			Subject:  nil, // Missing required field
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
		}

		resp, err := s.Evaluation(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("validation_error_missing_resource", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.EvaluationRequest{
			StoreId:  createStoreResp.GetId(),
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: nil, // Missing required field
			Action:   &authzenv1.Action{Name: "read"},
		}

		resp, err := s.Evaluation(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("validation_error_missing_action", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.EvaluationRequest{
			StoreId:  createStoreResp.GetId(),
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   nil, // Missing required field
		}

		resp, err := s.Evaluation(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})
}

func TestEvaluations(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("feature_flag_disabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(s.Close)

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

		resp, err := s.Evaluations(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Unimplemented, st.Code())
		require.Contains(t, st.Message(), "experimental")
	})

	t.Run("validation_error_empty_evaluations", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.EvaluationsRequest{
			StoreId:     createStoreResp.GetId(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{}, // Empty
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("short_circuit_deny_on_first_deny", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.EvaluationsRequest{
			StoreId: createStoreResp.GetId(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "read"},
				},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_EVALUATIONS_SEMANTIC_DENY_ON_FIRST_DENY,
			},
		}

		// The short-circuit path wraps errors in EvaluationResponse with context.error
		// instead of returning an error, so we get a response with an error context
		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify we got exactly one response with an error context
		require.Len(t, resp.GetEvaluationResponses(), 1)
		evalResp := resp.GetEvaluationResponses()[0]
		require.False(t, evalResp.GetDecision())
		require.NotNil(t, evalResp.GetContext())
		require.NotNil(t, evalResp.GetContext().GetError())
		require.Equal(t, uint32(500), evalResp.GetContext().GetError().GetStatus())
		require.Contains(t, evalResp.GetContext().GetError().GetMessage(), "authorization model")
	})

	t.Run("short_circuit_permit_on_first_permit", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.EvaluationsRequest{
			StoreId: createStoreResp.GetId(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "read"},
				},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_EVALUATIONS_SEMANTIC_PERMIT_ON_FIRST_PERMIT,
			},
		}

		// The short-circuit path wraps errors in EvaluationResponse with context.error
		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify we got exactly one response with an error context
		require.Len(t, resp.GetEvaluationResponses(), 1)
		evalResp := resp.GetEvaluationResponses()[0]
		require.False(t, evalResp.GetDecision())
		require.NotNil(t, evalResp.GetContext())
		require.NotNil(t, evalResp.GetContext().GetError())
		require.Equal(t, uint32(500), evalResp.GetContext().GetError().GetStatus())
	})

	t.Run("batch_evaluation_with_default_semantic", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		// Test with nil options (default EXECUTE_ALL)
		req := &authzenv1.EvaluationsRequest{
			StoreId: createStoreResp.GetId(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "read"},
				},
			},
			Options: nil, // Default semantic
		}

		// This will error because no model exists, but exercises the batch evaluation path
		resp, err := s.Evaluations(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_latest_authorization_model_not_found), st.Code())
	})

	t.Run("batch_evaluation_with_explicit_execute_all", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.EvaluationsRequest{
			StoreId: createStoreResp.GetId(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "read"},
				},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_EVALUATIONS_SEMANTIC_EXECUTE_ALL_UNSPECIFIED,
			},
		}

		// This will error because no model exists, but exercises the batch evaluation path
		resp, err := s.Evaluations(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_latest_authorization_model_not_found), st.Code())
	})

	t.Run("short_circuit_with_default_values", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		// Test that default values from request level are used when per-evaluation values are nil
		req := &authzenv1.EvaluationsRequest{
			StoreId:  createStoreResp.GetId(),
			Subject:  &authzenv1.Subject{Type: "user", Id: "default_user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "default_doc"},
			Action:   &authzenv1.Action{Name: "default_action"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					// All fields nil - should use defaults from request level
					Subject:  nil,
					Resource: nil,
					Action:   nil,
				},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_EVALUATIONS_SEMANTIC_DENY_ON_FIRST_DENY,
			},
		}

		// The short-circuit path should use the default values
		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify we got a response with an error (because no model exists)
		require.Len(t, resp.GetEvaluationResponses(), 1)
		evalResp := resp.GetEvaluationResponses()[0]
		require.False(t, evalResp.GetDecision())
		require.NotNil(t, evalResp.GetContext())
		require.NotNil(t, evalResp.GetContext().GetError())
	})

	t.Run("short_circuit_with_multiple_evaluations_continues_on_error", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		// Test with multiple evaluations - short-circuit continues on errors
		req := &authzenv1.EvaluationsRequest{
			StoreId: createStoreResp.GetId(),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "read"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc2"},
					Action:   &authzenv1.Action{Name: "write"},
				},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_EVALUATIONS_SEMANTIC_DENY_ON_FIRST_DENY,
			},
		}

		// When evaluations error (no model), short-circuit continues to next evaluation
		// (the continue statement on line 166 of authzen.go)
		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should have both responses because errors don't trigger short-circuit break
		require.Len(t, resp.GetEvaluationResponses(), 2)
		for _, evalResp := range resp.GetEvaluationResponses() {
			require.False(t, evalResp.GetDecision())
			require.NotNil(t, evalResp.GetContext())
			require.NotNil(t, evalResp.GetContext().GetError())
		}
	})

	t.Run("success_batch_with_valid_model", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write a minimal model
		writeModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Write tuples
		_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						Object:   "document:doc1",
						Relation: "reader",
						User:     "user:alice",
					},
				},
			},
		})
		require.NoError(t, err)

		// Test Evaluations with EXECUTE_ALL (default) - this uses batch path
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
			},
			// No options - uses default EXECUTE_ALL which goes through batch path
		}

		_ = writeModelResp // Model is automatically used as latest

		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should have 2 evaluation responses
		require.Len(t, resp.GetEvaluationResponses(), 2)

		// First should be true (alice has permission)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())

		// Second should be false (bob doesn't have permission)
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision())
	})

	t.Run("short_circuit_deny_on_first_deny_with_valid_model_breaks", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write a minimal model
		_, err = s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Write tuple for alice only (bob has no permission)
		_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						Object:   "document:doc1",
						Relation: "reader",
						User:     "user:alice",
					},
				},
			},
		})
		require.NoError(t, err)

		// Test DENY_ON_FIRST_DENY: first evaluation is bob (no permission = deny)
		// This should trigger the break on line 172-174 in authzen.go
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					// Bob has no permission - will return deny
					Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
				{
					// Alice has permission - but should never be evaluated due to break
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_EVALUATIONS_SEMANTIC_DENY_ON_FIRST_DENY,
			},
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should have only 1 response due to short-circuit break
		require.Len(t, resp.GetEvaluationResponses(), 1)

		// First (and only) should be false (bob denied)
		require.False(t, resp.GetEvaluationResponses()[0].GetDecision())
		// Should NOT have an error context since it's a valid deny, not an error
		require.Nil(t, resp.GetEvaluationResponses()[0].GetContext().GetError())
	})

	t.Run("short_circuit_permit_on_first_permit_with_valid_model_breaks", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write a minimal model
		_, err = s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Write tuple for alice
		_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						Object:   "document:doc1",
						Relation: "reader",
						User:     "user:alice",
					},
				},
			},
		})
		require.NoError(t, err)

		// Test PERMIT_ON_FIRST_PERMIT: first evaluation is alice (has permission = permit)
		// This should trigger the break on line 176-178 in authzen.go
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					// Alice has permission - will return permit
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
				{
					// Bob has no permission - but should never be evaluated due to break
					Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_EVALUATIONS_SEMANTIC_PERMIT_ON_FIRST_PERMIT,
			},
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should have only 1 response due to short-circuit break
		require.Len(t, resp.GetEvaluationResponses(), 1)

		// First (and only) should be true (alice permitted)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		// Should NOT have an error context since it's a valid permit, not an error
		require.Nil(t, resp.GetEvaluationResponses()[0].GetContext().GetError())
	})
}

func TestSubjectSearch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("feature_flag_disabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(s.Close)

		req := &authzenv1.SubjectSearchRequest{
			StoreId:  ulid.Make().String(),
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		}

		resp, err := s.SubjectSearch(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Unimplemented, st.Code())
		require.Contains(t, st.Message(), "experimental")
	})

	t.Run("validation_error_missing_resource", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.SubjectSearchRequest{
			StoreId:  createStoreResp.GetId(),
			Resource: nil, // Missing
			Action:   &authzenv1.Action{Name: "read"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		}

		resp, err := s.SubjectSearch(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("success_with_valid_model", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write a minimal model
		writeModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Write tuples
		_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						Object:   "document:doc1",
						Relation: "reader",
						User:     "user:alice",
					},
					{
						Object:   "document:doc1",
						Relation: "reader",
						User:     "user:bob",
					},
				},
			},
		})
		require.NoError(t, err)

		// Test SubjectSearch - find users who can read doc1
		req := &authzenv1.SubjectSearchRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeModelResp.GetAuthorizationModelId(),
			Resource:             &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:               &authzenv1.Action{Name: "reader"},
			Subject:              &authzenv1.SubjectFilter{Type: "user"},
		}

		resp, err := s.SubjectSearch(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should find alice and bob
		subjects := resp.GetSubjects()
		require.GreaterOrEqual(t, len(subjects), 2)
	})
}

func TestResourceSearch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("feature_flag_disabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(s.Close)

		req := &authzenv1.ResourceSearchRequest{
			StoreId:  ulid.Make().String(),
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
		}

		resp, err := s.ResourceSearch(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Unimplemented, st.Code())
		require.Contains(t, st.Message(), "experimental")
	})

	t.Run("validation_error_missing_subject", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.ResourceSearchRequest{
			StoreId:  createStoreResp.GetId(),
			Subject:  nil, // Missing
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
		}

		resp, err := s.ResourceSearch(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("success_with_valid_model", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write a minimal model
		writeModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Write tuples
		_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						Object:   "document:doc1",
						Relation: "reader",
						User:     "user:alice",
					},
					{
						Object:   "document:doc2",
						Relation: "reader",
						User:     "user:alice",
					},
				},
			},
		})
		require.NoError(t, err)

		// Test ResourceSearch - find documents alice can read
		req := &authzenv1.ResourceSearchRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeModelResp.GetAuthorizationModelId(),
			Subject:              &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:               &authzenv1.Action{Name: "reader"},
			Resource:             &authzenv1.Resource{Type: "document"},
		}

		resp, err := s.ResourceSearch(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should find doc1 and doc2
		resources := resp.GetResources()
		require.GreaterOrEqual(t, len(resources), 2)
	})
}

func TestActionSearch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("feature_flag_disabled", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(s.Close)

		req := &authzenv1.ActionSearchRequest{
			StoreId:  ulid.Make().String(),
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		}

		resp, err := s.ActionSearch(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Unimplemented, st.Code())
		require.Contains(t, st.Message(), "experimental")
	})

	t.Run("validation_error_missing_subject", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.ActionSearchRequest{
			StoreId:  createStoreResp.GetId(),
			Subject:  nil, // Missing
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		}

		resp, err := s.ActionSearch(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("model_not_found", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		req := &authzenv1.ActionSearchRequest{
			StoreId:  createStoreResp.GetId(),
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		}

		resp, err := s.ActionSearch(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_latest_authorization_model_not_found), st.Code())
	})

	t.Run("success_with_valid_model", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write a minimal model
		writeModelResp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader": {
							Userset: &openfgav1.Userset_This{},
						},
						"writer": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
							"writer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Write a tuple to grant permission
		_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						Object:   "document:doc1",
						Relation: "reader",
						User:     "user:alice",
					},
				},
			},
		})
		require.NoError(t, err)

		// Now test ActionSearch - this should exercise the success path
		req := &authzenv1.ActionSearchRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeModelResp.GetAuthorizationModelId(),
			Subject:              &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource:             &authzenv1.Resource{Type: "document", Id: "doc1"},
		}

		resp, err := s.ActionSearch(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should return actions that alice can perform on doc1
		require.NotNil(t, resp.GetActions())
		// Alice has 'reader' permission, so should be in the results
		actions := resp.GetActions()
		require.GreaterOrEqual(t, len(actions), 1)

		// Verify at least one action is present
		foundReader := false
		for _, action := range actions {
			if action.GetName() == "reader" {
				foundReader = true
				break
			}
		}
		require.True(t, foundReader, "Expected to find 'reader' action in results")
	})
}
