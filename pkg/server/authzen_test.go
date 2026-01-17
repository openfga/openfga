package server

import (
	"context"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

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

	t.Run("returns_validation_error_for_invalid_type", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write a model with only user and document types
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

		// Request with a type that doesn't exist in the model
		req := &authzenv1.EvaluationRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "unknown_type", Id: "resource1"},
			Action:   &authzenv1.Action{Name: "reader"},
		}

		resp, err := s.Evaluation(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		// validation_error (Code 2000) for type not found
		require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), st.Code())
		require.Contains(t, st.Message(), "unknown_type")
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
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
		// Error is due to missing type definition, which maps to HTTP 400
		require.Equal(t, uint32(400), evalResp.GetContext().GetError().GetStatus())
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_permit_on_first_permit,
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
		// Error is due to missing type definition, which maps to HTTP 400
		require.Equal(t, uint32(400), evalResp.GetContext().GetError().GetStatus())
	})

	t.Run("batch_evaluation_with_default_semantic", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		// Test with nil options (default execute_all)
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_execute_all,
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
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

	t.Run("short_circuit_returns_400_for_invalid_type", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write a model with only user and document types
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

		// Request with a type that doesn't exist in the model
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "unknown_type", Id: "resource1"}, // Type not in model
					Action:   &authzenv1.Action{Name: "reader"},
				},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
			},
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.GetEvaluationResponses(), 1)

		evalResp := resp.GetEvaluationResponses()[0]
		require.False(t, evalResp.GetDecision())
		require.NotNil(t, evalResp.GetContext())
		require.NotNil(t, evalResp.GetContext().GetError())
		// InvalidArgument maps to HTTP 400
		require.Equal(t, uint32(400), evalResp.GetContext().GetError().GetStatus())
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

		// Test Evaluations with execute_all (default) - this uses batch path
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
			// No options - uses default execute_all which goes through batch path
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

		// Test deny_on_first_deny: first evaluation is bob (no permission = deny)
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
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

		// Test permit_on_first_permit: first evaluation is alice (has permission = permit)
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_permit_on_first_permit,
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
			StoreId:  storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		}

		// Pass model ID via header
		md := metadata.New(map[string]string{
			strings.ToLower(AuthZenAuthorizationModelIDHeader): writeModelResp.GetAuthorizationModelId(),
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.SubjectSearch(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should find alice and bob
		subjects := resp.GetResults()
		require.GreaterOrEqual(t, len(subjects), 2)
	})

	t.Run("json_response_omits_page_when_pagination_not_supported", func(t *testing.T) {
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

		// Write a tuple
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

		// Test SubjectSearch
		req := &authzenv1.SubjectSearchRequest{
			StoreId:  storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		}

		md := metadata.New(map[string]string{
			strings.ToLower(AuthZenAuthorizationModelIDHeader): writeModelResp.GetAuthorizationModelId(),
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.SubjectSearch(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Page should be nil (pagination not supported)
		require.Nil(t, resp.GetPage())

		// Verify JSON serialization omits "page" field per AuthZEN spec
		// "properties with a value of null SHOULD be omitted from JSON objects"
		jsonBytes, err := protojson.Marshal(resp)
		require.NoError(t, err)
		jsonStr := string(jsonBytes)

		// The JSON should NOT contain "page" at all
		require.NotContains(t, jsonStr, "page", "JSON response should not contain 'page' field when pagination is not supported")
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
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "reader"},
			Resource: &authzenv1.Resource{Type: "document"},
		}

		// Pass model ID via header
		md := metadata.New(map[string]string{
			strings.ToLower(AuthZenAuthorizationModelIDHeader): writeModelResp.GetAuthorizationModelId(),
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.ResourceSearch(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should find doc1 and doc2
		resources := resp.GetResults()
		require.GreaterOrEqual(t, len(resources), 2)
	})

	t.Run("json_response_omits_page_when_pagination_not_supported", func(t *testing.T) {
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

		// Write a tuple
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

		// Test ResourceSearch
		req := &authzenv1.ResourceSearchRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "reader"},
			Resource: &authzenv1.Resource{Type: "document"},
		}

		md := metadata.New(map[string]string{
			strings.ToLower(AuthZenAuthorizationModelIDHeader): writeModelResp.GetAuthorizationModelId(),
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.ResourceSearch(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Page should be nil (pagination not supported)
		require.Nil(t, resp.GetPage())

		// Verify JSON serialization omits "page" field per AuthZEN spec
		jsonBytes, err := protojson.Marshal(resp)
		require.NoError(t, err)
		jsonStr := string(jsonBytes)

		// The JSON should NOT contain "page" at all
		require.NotContains(t, jsonStr, "page", "JSON response should not contain 'page' field when pagination is not supported")
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
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		}

		// Pass model ID via header
		md := metadata.New(map[string]string{
			strings.ToLower(AuthZenAuthorizationModelIDHeader): writeModelResp.GetAuthorizationModelId(),
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.ActionSearch(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should return actions that alice can perform on doc1
		require.NotNil(t, resp.GetResults())
		// Alice has 'reader' permission, so should be in the results
		actions := resp.GetResults()
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

	t.Run("json_response_omits_page_when_pagination_not_supported", func(t *testing.T) {
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

		// Write a tuple
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

		// Test ActionSearch
		req := &authzenv1.ActionSearchRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		}

		md := metadata.New(map[string]string{
			strings.ToLower(AuthZenAuthorizationModelIDHeader): writeModelResp.GetAuthorizationModelId(),
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.ActionSearch(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Page should be nil (pagination not supported)
		require.Nil(t, resp.GetPage())

		// Verify JSON serialization omits "page" field per AuthZEN spec
		jsonBytes, err := protojson.Marshal(resp)
		require.NoError(t, err)
		jsonStr := string(jsonBytes)

		// The JSON should NOT contain "page" at all
		require.NotContains(t, jsonStr, "page", "JSON response should not contain 'page' field when pagination is not supported")
	})
}

func TestGetAuthorizationModelIDFromHeader(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("returns_header_value_when_present", func(t *testing.T) {
		headerModelID := ulid.Make().String()

		// Create context with gRPC metadata containing the header
		// grpc-gateway converts header names to lowercase
		md := metadata.New(map[string]string{
			strings.ToLower(AuthZenAuthorizationModelIDHeader): headerModelID,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		result := getAuthorizationModelIDFromHeader(ctx)
		require.Equal(t, headerModelID, result)
	})

	t.Run("returns_empty_when_header_not_present", func(t *testing.T) {
		// Create context without the header
		ctx := context.Background()

		result := getAuthorizationModelIDFromHeader(ctx)
		require.Empty(t, result)
	})

	t.Run("returns_empty_when_header_is_empty", func(t *testing.T) {
		// Create context with empty header value
		md := metadata.New(map[string]string{
			strings.ToLower(AuthZenAuthorizationModelIDHeader): "",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		result := getAuthorizationModelIDFromHeader(ctx)
		require.Empty(t, result)
	})

	t.Run("returns_empty_when_no_metadata", func(t *testing.T) {
		ctx := context.Background()

		result := getAuthorizationModelIDFromHeader(ctx)
		require.Empty(t, result)
	})
}

func TestEvaluationWithAuthorizationModelIDHeader(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("uses_model_id_from_header", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		// Create store
		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Write two models
		writeModelResp1, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{Type: "user"},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader": {Userset: &openfgav1.Userset_This{}},
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
		modelID1 := writeModelResp1.GetAuthorizationModelId()

		// Write second model (this becomes the latest)
		_, err = s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{Type: "user"},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"writer": {Userset: &openfgav1.Userset_This{}},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
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

		// Write tuple using model 1 (which has the reader relation)
		_, err = s.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID1,
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

		// Create context with header specifying the first model ID
		md := metadata.New(map[string]string{
			strings.ToLower(AuthZenAuthorizationModelIDHeader): modelID1,
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		// Test Evaluation - should use modelID1 from header
		req := &authzenv1.EvaluationRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		}

		resp, err := s.Evaluation(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetDecision())
	})
}
