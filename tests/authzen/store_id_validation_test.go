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

	tests := []struct {
		name string
		call func(context.Context, string) error
	}{
		{
			name: "Evaluation",
			call: func(ctx context.Context, storeID string) error {
				_, err := tc.authzenClient.Evaluation(ctx, &authzenv1.EvaluationRequest{
					StoreId:  storeID,
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				})
				return err
			},
		},
		{
			name: "Evaluations",
			call: func(ctx context.Context, storeID string) error {
				_, err := tc.authzenClient.Evaluations(ctx, &authzenv1.EvaluationsRequest{
					StoreId: storeID,
					Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
					Action:  &authzenv1.Action{Name: "reader"},
					Evaluations: []*authzenv1.EvaluationsItemRequest{
						{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
					},
				})
				return err
			},
		},
		{
			name: "SubjectSearch",
			call: func(ctx context.Context, storeID string) error {
				_, err := tc.authzenClient.SubjectSearch(ctx, &authzenv1.SubjectSearchRequest{
					StoreId:  storeID,
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				})
				return err
			},
		},
		{
			name: "ResourceSearch",
			call: func(ctx context.Context, storeID string) error {
				_, err := tc.authzenClient.ResourceSearch(ctx, &authzenv1.ResourceSearchRequest{
					StoreId: storeID,
					Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
					Action:  &authzenv1.Action{Name: "reader"},
				})
				return err
			},
		},
		{
			name: "ActionSearch",
			call: func(ctx context.Context, storeID string) error {
				_, err := tc.authzenClient.ActionSearch(ctx, &authzenv1.ActionSearchRequest{
					StoreId:  storeID,
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
				})
				return err
			},
		},
		{
			name: "GetConfiguration",
			call: func(ctx context.Context, storeID string) error {
				_, err := tc.authzenClient.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
				return err
			},
		},
	}

	for _, endpoint := range tests {
		t.Run(endpoint.name, func(t *testing.T) {
			for _, tt := range malformedStoreIDs {
				t.Run(tt.name, func(t *testing.T) {
					err := endpoint.call(context.Background(), tt.storeID)
					require.Error(t, err)
					require.Equal(t, codes.InvalidArgument, status.Code(err))
				})
			}
		})
	}
}
