package authzen_test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
)

func TestStoreIDValidation(t *testing.T) {
	tc := setupTestContext(t)

	malformedStoreIDs := []struct {
		name    string
		storeID string
	}{
		{name: "empty", storeID: ""},
		{name: "too_short", storeID: "1"},
		{name: "extra_chars", storeID: ulid.Make().String() + "A"},
		{name: "invalid_chars", storeID: "ABCDEFGHIJKLMNOPQRSTUVWXY@"},
	}

	t.Run("Evaluation", func(t *testing.T) {
		for _, tt := range malformedStoreIDs {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
					StoreId:  tt.storeID,
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	})

	t.Run("Evaluations", func(t *testing.T) {
		for _, tt := range malformedStoreIDs {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
					StoreId: tt.storeID,
					Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
					Action:  &authzenv1.Action{Name: "reader"},
					Evaluations: []*authzenv1.EvaluationsItemRequest{
						{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
					},
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	})

	t.Run("SubjectSearch", func(t *testing.T) {
		for _, tt := range malformedStoreIDs {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.authzenClient.SubjectSearch(context.Background(), &authzenv1.SubjectSearchRequest{
					StoreId:  tt.storeID,
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	})

	t.Run("ResourceSearch", func(t *testing.T) {
		for _, tt := range malformedStoreIDs {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
					StoreId: tt.storeID,
					Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
					Action:  &authzenv1.Action{Name: "reader"},
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	})

	t.Run("ActionSearch", func(t *testing.T) {
		for _, tt := range malformedStoreIDs {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.authzenClient.ActionSearch(context.Background(), &authzenv1.ActionSearchRequest{
					StoreId:  tt.storeID,
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	})

	t.Run("GetConfiguration", func(t *testing.T) {
		for _, tt := range malformedStoreIDs {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{
					StoreId: tt.storeID,
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	})
}
