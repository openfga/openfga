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
	"google.golang.org/protobuf/types/known/structpb"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/util"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
)

// getContextError extracts the "error" field from an evaluation response context struct.
func getContextError(ctx *structpb.Struct) *structpb.Value {
	if ctx == nil {
		return nil
	}
	return ctx.GetFields()["error"]
}

// getContextErrorStatus extracts the status from a context error.
func getContextErrorStatus(ctx *structpb.Struct) uint32 {
	errVal := getContextError(ctx)
	if errVal == nil {
		return 0
	}
	return uint32(errVal.GetStructValue().GetFields()["status"].GetNumberValue())
}

// simpleModel returns type definitions for a user + document model with the given relations.
// All relations are directly assignable by users.
func simpleModel(relations ...string) []*openfgav1.TypeDefinition {
	relMap := make(map[string]*openfgav1.Userset, len(relations))
	metaMap := make(map[string]*openfgav1.RelationMetadata, len(relations))
	for _, r := range relations {
		relMap[r] = &openfgav1.Userset{Userset: &openfgav1.Userset_This{}}
		metaMap[r] = &openfgav1.RelationMetadata{
			DirectlyRelatedUserTypes: []*openfgav1.RelationReference{{Type: "user"}},
		}
	}
	return []*openfgav1.TypeDefinition{
		{Type: "user"},
		{
			Type:      "document",
			Relations: relMap,
			Metadata:  &openfgav1.Metadata{Relations: metaMap},
		},
	}
}

// writeModel writes an authorization model and returns its ID.
func writeModel(t *testing.T, s *Server, storeID string, typeDefs []*openfgav1.TypeDefinition) string {
	t.Helper()
	resp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   "1.1",
		TypeDefinitions: typeDefs,
	})
	require.NoError(t, err)
	return resp.GetAuthorizationModelId()
}

func writeTuples(t *testing.T, s *Server, storeID string, tupleKeys ...*openfgav1.TupleKey) {
	t.Helper()

	_, err := s.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: tupleKeys,
		},
	})
	require.NoError(t, err)
}

func writeTuplesWithModel(t *testing.T, s *Server, storeID, modelID string, tupleKeys ...*openfgav1.TupleKey) {
	t.Helper()

	_, err := s.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: tupleKeys,
		},
	})
	require.NoError(t, err)
}

func contextWithAuthorizationModelID(modelID string) context.Context {
	md := metadata.New(map[string]string{
		strings.ToLower(AuthorizationModelIDHeader): modelID,
	})

	return metadata.NewIncomingContext(context.Background(), md)
}

func newExperimentalAuthZenServer(t *testing.T, ds storage.OpenFGADatastore) *Server {
	t.Helper()

	s := MustNewServerWithOpts(
		WithDatastore(ds),
		WithExperimentals(serverconfig.ExperimentalAuthZen),
	)
	t.Cleanup(s.Close)

	return s
}

func newExperimentalAuthZenServerAndStore(t *testing.T, ds storage.OpenFGADatastore) (*Server, string) {
	t.Helper()

	s := newExperimentalAuthZenServer(t, ds)
	resp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
	require.NoError(t, err)

	return s, resp.GetId()
}

func TestAuthZenFeatureFlagDisabled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	s := MustNewServerWithOpts(WithDatastore(ds))
	t.Cleanup(s.Close)

	storeID := ulid.Make().String()

	tests := []struct {
		name string
		call func() error
	}{
		{"Evaluation", func() error {
			_, err := s.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
				StoreId: storeID, Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
				Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}, Action: &authzenv1.Action{Name: "read"},
			})
			return err
		}},
		{"Evaluations", func() error {
			_, err := s.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
				StoreId: storeID, Evaluations: []*authzenv1.EvaluationsItemRequest{{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}, Action: &authzenv1.Action{Name: "read"},
				}},
			})
			return err
		}},
		{"SubjectSearch", func() error {
			_, err := s.SubjectSearch(context.Background(), &authzenv1.SubjectSearchRequest{
				StoreId: storeID, Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
				Action: &authzenv1.Action{Name: "read"}, Subject: &authzenv1.SubjectFilter{Type: "user"},
			})
			return err
		}},
		{"ResourceSearch", func() error {
			_, err := s.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
				StoreId: storeID, Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
				Action: &authzenv1.Action{Name: "read"}, Resource: &authzenv1.ResourceFilter{Type: "document"},
			})
			return err
		}},
		{"ActionSearch", func() error {
			_, err := s.ActionSearch(context.Background(), &authzenv1.ActionSearchRequest{
				StoreId: storeID, Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
				Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			})
			return err
		}},
		{"GetConfiguration", func() error {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
				":authority": "pdp.example.com",
			}))
			_, err := s.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Unimplemented, st.Code())
			require.Contains(t, st.Message(), "experimental")
		})
	}
}

func TestEvaluation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("success_with_valid_model", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID, &openfgav1.TupleKey{
			Object:   "document:doc1",
			Relation: "reader",
			User:     "user:alice",
		})

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

	t.Run("model_not_found", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		req := &authzenv1.EvaluationRequest{
			StoreId:  storeID,
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

	t.Run("validation_errors", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		tests := []struct {
			name     string
			subject  *authzenv1.Subject
			resource *authzenv1.Resource
			action   *authzenv1.Action
		}{
			{"missing_subject", nil, &authzenv1.Resource{Type: "document", Id: "doc1"}, &authzenv1.Action{Name: "read"}},
			{"missing_resource", &authzenv1.Subject{Type: "user", Id: "alice"}, nil, &authzenv1.Action{Name: "read"}},
			{"missing_action", &authzenv1.Subject{Type: "user", Id: "alice"}, &authzenv1.Resource{Type: "document", Id: "doc1"}, nil},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := s.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
					StoreId:  storeID,
					Subject:  tt.subject,
					Resource: tt.resource,
					Action:   tt.action,
				})
				require.Error(t, err)
				require.Nil(t, resp)

				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, codes.InvalidArgument, st.Code())
			})
		}
	})

	t.Run("returns_validation_error_for_invalid_type", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

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

	t.Run("validation_error_empty_evaluations", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		req := &authzenv1.EvaluationsRequest{
			StoreId:     storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{}, // Empty
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("nil_evaluations_falls_back_to_single_evaluation", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID, &openfgav1.TupleKey{Object: "document:doc1", Relation: "reader", User: "user:alice"})

		// Nil evaluations should fall back to a single Evaluation using top-level fields
		req := &authzenv1.EvaluationsRequest{
			StoreId:     storeID,
			Subject:     &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource:    &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:      &authzenv1.Action{Name: "reader"},
			Evaluations: nil,
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.GetEvaluations(), 1)
		require.True(t, resp.GetEvaluations()[0].GetDecision())
	})

	t.Run("invalid_semantic_returns_error", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		// Use an invalid enum value (999)
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic(999),
			},
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
		require.Contains(t, st.Message(), "EvaluationsSemantic")
	})

	t.Run("batch_evaluation_with_default_semantic", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		// Test with nil options (default execute_all)
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
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

	t.Run("short_circuit_with_default_values", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		// Test that default values from request level are used when per-evaluation values are nil
		req := &authzenv1.EvaluationsRequest{
			StoreId:  storeID,
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
		require.Len(t, resp.GetEvaluations(), 1)
		evalResp := resp.GetEvaluations()[0]
		require.False(t, evalResp.GetDecision())
		require.NotNil(t, evalResp.GetContext())
		require.NotNil(t, getContextError(evalResp.GetContext()))
	})

	t.Run("short_circuit_deny_on_first_deny_breaks_on_error", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		// Test with multiple evaluations - deny_on_first_deny short-circuits on errors
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
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

		// When evaluations error (no model), error results in Decision: false
		// which triggers deny_on_first_deny short-circuit
		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should have only 1 response because error triggers short-circuit break
		require.Len(t, resp.GetEvaluations(), 1)
		evalResp := resp.GetEvaluations()[0]
		require.False(t, evalResp.GetDecision())
		require.NotNil(t, evalResp.GetContext())
		require.NotNil(t, getContextError(evalResp.GetContext()))
	})

	t.Run("short_circuit_returns_400_for_invalid_type", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

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
		require.Len(t, resp.GetEvaluations(), 1)

		evalResp := resp.GetEvaluations()[0]
		require.False(t, evalResp.GetDecision())
		require.NotNil(t, evalResp.GetContext())
		require.NotNil(t, getContextError(evalResp.GetContext()))
		// InvalidArgument maps to HTTP 400
		require.Equal(t, uint32(400), getContextErrorStatus(evalResp.GetContext()))
	})

	t.Run("success_batch_with_valid_model", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID, &openfgav1.TupleKey{
			Object:   "document:doc1",
			Relation: "reader",
			User:     "user:alice",
		})

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

		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should have 2 evaluation responses
		require.Len(t, resp.GetEvaluations(), 2)

		// First should be true (alice has permission)
		require.True(t, resp.GetEvaluations()[0].GetDecision())

		// Second should be false (bob doesn't have permission)
		require.False(t, resp.GetEvaluations()[1].GetDecision())
	})

	t.Run("short_circuit_deny_on_first_deny_with_valid_model_breaks", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID, &openfgav1.TupleKey{
			Object:   "document:doc1",
			Relation: "reader",
			User:     "user:alice",
		})

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
		require.Len(t, resp.GetEvaluations(), 1)

		// First (and only) should be false (bob denied)
		require.False(t, resp.GetEvaluations()[0].GetDecision())
		// Should NOT have an error context since it's a valid deny, not an error
		require.Nil(t, getContextError(resp.GetEvaluations()[0].GetContext()))
	})

	t.Run("short_circuit_permit_on_first_permit_with_valid_model_breaks", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID, &openfgav1.TupleKey{
			Object:   "document:doc1",
			Relation: "reader",
			User:     "user:alice",
		})

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
		require.Len(t, resp.GetEvaluations(), 1)

		// First (and only) should be true (alice permitted)
		require.True(t, resp.GetEvaluations()[0].GetDecision())
		// Should NOT have an error context since it's a valid permit, not an error
		require.Nil(t, getContextError(resp.GetEvaluations()[0].GetContext()))
	})

	t.Run("short_circuit_buildcheck_error_deny_on_first_deny_breaks", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		// First item has nil subject → buildCheckRequest error → break (deny_on_first_deny)
		// Second item should never be evaluated
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  nil, // triggers buildCheckRequest error
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
				{
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

		// Should have 1 response: the error triggers break
		require.Len(t, resp.GetEvaluations(), 1)
		require.False(t, resp.GetEvaluations()[0].GetDecision())
		require.NotNil(t, getContextError(resp.GetEvaluations()[0].GetContext()))
		require.Equal(t, uint32(400), getContextErrorStatus(resp.GetEvaluations()[0].GetContext()))
	})

	t.Run("short_circuit_buildcheck_error_permit_on_first_permit_continues", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID, &openfgav1.TupleKey{Object: "document:doc1", Relation: "reader", User: "user:alice"})

		// First item has nil subject → buildCheckRequest error (continue, not break for permit_on_first_permit)
		// Second item succeeds with permit → break
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  nil, // triggers buildCheckRequest error → continue
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
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

		// Should have 2 responses: error (continue) + permit (break)
		require.Len(t, resp.GetEvaluations(), 2)
		require.False(t, resp.GetEvaluations()[0].GetDecision())
		require.NotNil(t, getContextError(resp.GetEvaluations()[0].GetContext()))
		require.True(t, resp.GetEvaluations()[1].GetDecision())
	})

	t.Run("short_circuit_check_error_permit_on_first_permit_continues", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID, &openfgav1.TupleKey{Object: "document:doc1", Relation: "reader", User: "user:alice"})

		// First item has invalid type → Check returns error → continue
		// Second item succeeds with permit → break
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "unknown_type", Id: "resource1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
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

		// Should have 2 responses: error (continue) + permit (break)
		require.Len(t, resp.GetEvaluations(), 2)
		require.False(t, resp.GetEvaluations()[0].GetDecision())
		require.NotNil(t, getContextError(resp.GetEvaluations()[0].GetContext()))
		require.True(t, resp.GetEvaluations()[1].GetDecision())
	})

	t.Run("deny_on_first_deny_returns_results_up_to_deny", func(t *testing.T) {
		// Per AuthZEN spec: deny_on_first_deny returns all results up to and including first deny
		// Example from spec: [doc1=true, doc2=false, doc3=true] -> returns [true, false]
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		// Write tuples: alice can read doc1 and doc3, but NOT doc2
		writeTuples(t, s, storeID,
			&openfgav1.TupleKey{Object: "document:doc1", Relation: "reader", User: "user:alice"},
			&openfgav1.TupleKey{Object: "document:doc3", Relation: "reader", User: "user:alice"},
		)

		// Request: doc1 (permit), doc2 (deny), doc3 (permit - should not be evaluated)
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
			},
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should have 2 responses: doc1=true, doc2=false (doc3 not evaluated)
		require.Len(t, resp.GetEvaluations(), 2)
		require.True(t, resp.GetEvaluations()[0].GetDecision())  // doc1 = permit
		require.False(t, resp.GetEvaluations()[1].GetDecision()) // doc2 = deny
	})

	t.Run("execute_all_invalid_item_returns_error", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		// One valid item + one with nil subject (triggers buildCheckRequest error in evaluateAll)
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
				{
					Subject:  nil, // missing subject
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
			},
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
		require.Contains(t, st.Message(), "evaluation 1")
	})

	t.Run("execute_all_batch_error_result_returns_error_context", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		// Write tuple for alice
		writeTuples(t, s, storeID, &openfgav1.TupleKey{Object: "document:doc1", Relation: "reader", User: "user:alice"})

		// Mix valid and invalid type evaluations — invalid type triggers a per-item
		// error in BatchCheck, which evaluateAll maps to an error context response.
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "unknown_type", Id: "resource1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
			},
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.GetEvaluations(), 2)

		// First should succeed
		require.True(t, resp.GetEvaluations()[0].GetDecision())

		// Second should have an error context (invalid type)
		require.False(t, resp.GetEvaluations()[1].GetDecision())
		require.NotNil(t, getContextError(resp.GetEvaluations()[1].GetContext()))
	})

	t.Run("permit_on_first_permit_returns_results_up_to_permit", func(t *testing.T) {
		// Per AuthZEN spec: permit_on_first_permit returns all results up to and including first permit
		// Like || operator: continues on false until first true
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		writeModel(t, s, storeID, simpleModel("reader"))

		// Write tuple: alice can read doc3 only
		writeTuples(t, s, storeID, &openfgav1.TupleKey{Object: "document:doc3", Relation: "reader", User: "user:alice"})

		// Request: doc1 (deny), doc2 (deny), doc3 (permit), doc4 (should not be evaluated)
		req := &authzenv1.EvaluationsRequest{
			StoreId: storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc4"}},
			},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_permit_on_first_permit,
			},
		}

		resp, err := s.Evaluations(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should have 3 responses: doc1=false, doc2=false, doc3=true (doc4 not evaluated)
		require.Len(t, resp.GetEvaluations(), 3)
		require.False(t, resp.GetEvaluations()[0].GetDecision()) // doc1 = deny
		require.False(t, resp.GetEvaluations()[1].GetDecision()) // doc2 = deny
		require.True(t, resp.GetEvaluations()[2].GetDecision())  // doc3 = permit
	})
}

func TestSubjectSearch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("validation_error_missing_resource", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		req := &authzenv1.SubjectSearchRequest{
			StoreId:  storeID,
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
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		modelID := writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID,
			&openfgav1.TupleKey{Object: "document:doc1", Relation: "reader", User: "user:alice"},
			&openfgav1.TupleKey{Object: "document:doc1", Relation: "reader", User: "user:bob"},
		)

		// Test SubjectSearch - find users who can read doc1
		req := &authzenv1.SubjectSearchRequest{
			StoreId:  storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		}

		ctx := contextWithAuthorizationModelID(modelID)

		resp, err := s.SubjectSearch(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should find alice and bob
		subjects := resp.GetResults()
		require.GreaterOrEqual(t, len(subjects), 2)
	})

	t.Run("json_response_omits_page_when_pagination_not_supported", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		modelID := writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID, &openfgav1.TupleKey{
			Object:   "document:doc1",
			Relation: "reader",
			User:     "user:alice",
		})

		// Test SubjectSearch
		req := &authzenv1.SubjectSearchRequest{
			StoreId:  storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		}

		ctx := contextWithAuthorizationModelID(modelID)

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

	t.Run("returns_error_when_list_users_fails", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		modelID := writeModel(t, s, storeID, simpleModel("reader"))

		ctx := contextWithAuthorizationModelID(modelID)

		// Use a relation that doesn't exist in the model → ListUsers will fail
		req := &authzenv1.SubjectSearchRequest{
			StoreId:  storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "nonexistent_relation"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		}

		resp, err := s.SubjectSearch(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("returns_wildcard_subjects", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		// Model with wildcard support
		modelID := writeModel(t, s, storeID, []*openfgav1.TypeDefinition{
			{Type: "user"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {Userset: &openfgav1.Userset_This{}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
								{Type: "user", RelationOrWildcard: &openfgav1.RelationReference_Wildcard{Wildcard: &openfgav1.Wildcard{}}},
							},
						},
					},
				},
			},
		})

		writeTuples(t, s, storeID, &openfgav1.TupleKey{Object: "document:doc1", Relation: "viewer", User: "user:*"})

		ctx := contextWithAuthorizationModelID(modelID)

		resp, err := s.SubjectSearch(ctx, &authzenv1.SubjectSearchRequest{
			StoreId:  storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "viewer"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should contain wildcard subject
		subjects := resp.GetResults()
		require.Len(t, subjects, 1)
		require.Equal(t, "user", subjects[0].GetType())
		require.Equal(t, "*", subjects[0].GetId())
	})
}

func TestResourceSearch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("validation_error_missing_subject", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		req := &authzenv1.ResourceSearchRequest{
			StoreId:  storeID,
			Subject:  nil, // Missing
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.ResourceFilter{Type: "document"},
		}

		resp, err := s.ResourceSearch(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("returns_error_when_streamed_list_objects_fails", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		modelID := writeModel(t, s, storeID, simpleModel("reader"))

		ctx := contextWithAuthorizationModelID(modelID)

		// Use a relation that doesn't exist → StreamedListObjects will fail
		req := &authzenv1.ResourceSearchRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "nonexistent_relation"},
			Resource: &authzenv1.ResourceFilter{Type: "document"},
		}

		resp, err := s.ResourceSearch(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("success_with_valid_model", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		modelID := writeModel(t, s, storeID, simpleModel("reader"))

		writeTuples(t, s, storeID,
			&openfgav1.TupleKey{Object: "document:doc1", Relation: "reader", User: "user:alice"},
			&openfgav1.TupleKey{Object: "document:doc2", Relation: "reader", User: "user:alice"},
		)

		// Test ResourceSearch - find documents alice can read
		req := &authzenv1.ResourceSearchRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "reader"},
			Resource: &authzenv1.ResourceFilter{Type: "document"},
		}

		ctx := contextWithAuthorizationModelID(modelID)

		resp, err := s.ResourceSearch(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should find doc1 and doc2
		resources := resp.GetResults()
		require.GreaterOrEqual(t, len(resources), 2)
	})
}

func TestActionSearch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("validation_error_missing_subject", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		req := &authzenv1.ActionSearchRequest{
			StoreId:  storeID,
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
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		req := &authzenv1.ActionSearchRequest{
			StoreId:  storeID,
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
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		modelID := writeModel(t, s, storeID, simpleModel("reader", "writer"))

		writeTuples(t, s, storeID, &openfgav1.TupleKey{
			Object:   "document:doc1",
			Relation: "reader",
			User:     "user:alice",
		})

		// Now test ActionSearch - this should exercise the success path
		req := &authzenv1.ActionSearchRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		}

		ctx := contextWithAuthorizationModelID(modelID)

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

	t.Run("handles_batch_error_items_gracefully", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		// Create a model with a condition-based relation that will cause some batch items to error
		resp, err := s.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{Type: "user"},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader":             {Userset: &openfgav1.Userset_This{}},
						"conditional_reader": {Userset: &openfgav1.Userset_This{}},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{{Type: "user"}},
							},
							"conditional_reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user", Condition: "time_check"},
								},
							},
						},
					},
				},
			},
			Conditions: map[string]*openfgav1.Condition{
				"time_check": {
					Name:       "time_check",
					Expression: "current_time > threshold",
					Parameters: map[string]*openfgav1.ConditionParamTypeRef{
						"current_time": {TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_TIMESTAMP},
						"threshold":    {TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_TIMESTAMP},
					},
				},
			},
		})
		require.NoError(t, err)
		modelID := resp.GetAuthorizationModelId()

		// Write tuple for reader (no condition) and conditional_reader (with condition)
		writeTuplesWithModel(t, s, storeID, modelID,
			&openfgav1.TupleKey{Object: "document:doc1", Relation: "reader", User: "user:alice"},
			&openfgav1.TupleKey{
				Object: "document:doc1", Relation: "conditional_reader", User: "user:alice",
				Condition: &openfgav1.RelationshipCondition{Name: "time_check"},
			},
		)

		ctx := contextWithAuthorizationModelID(modelID)

		// ActionSearch will BatchCheck both relations. The conditional_reader check
		// will return an error because the required condition parameters are missing.
		actionReq := &authzenv1.ActionSearchRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		}

		actionResp, err := s.ActionSearch(ctx, actionReq)
		require.NoError(t, err)
		require.NotNil(t, actionResp)

		// Should only return 'reader' since conditional_reader check will error
		actions := actionResp.GetResults()
		require.Len(t, actions, 1)
		require.Equal(t, "reader", actions[0].GetName())
	})

	t.Run("invalid_resource_type_returns_error", func(t *testing.T) {
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		modelID := writeModel(t, s, storeID, simpleModel("reader"))

		ctx := contextWithAuthorizationModelID(modelID)

		// Resource type not in model → GetRelations error
		req := &authzenv1.ActionSearchRequest{
			StoreId:  storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "unknown_type", Id: "doc1"},
		}

		resp, err := s.ActionSearch(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})
}

func TestGetAuthorizationModelIDFromHeader(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("returns_header_value_when_present", func(t *testing.T) {
		headerModelID := ulid.Make().String()

		ctx := contextWithAuthorizationModelID(headerModelID)

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
		ctx := contextWithAuthorizationModelID("")

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
		s, storeID := newExperimentalAuthZenServerAndStore(t, ds)

		// Write two models — first has "reader", second (latest) has "writer"
		modelID1 := writeModel(t, s, storeID, simpleModel("reader"))
		writeModel(t, s, storeID, simpleModel("writer"))

		writeTuplesWithModel(t, s, storeID, modelID1, &openfgav1.TupleKey{
			Object:   "document:doc1",
			Relation: "reader",
			User:     "user:alice",
		})

		// Create context with header specifying the first model ID
		ctx := contextWithAuthorizationModelID(modelID1)

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
