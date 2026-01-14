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
