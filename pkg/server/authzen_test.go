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
}
