package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestListUsersValidation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	model := `
	model
		schema 1.1
	type user

	type document
		relations
			define viewer: [user]`

	tests := []struct {
		name              string
		req               *openfgav1.ListUsersRequest
		model             string
		expectedErrorCode codes.Code
	}{
		{
			name: "invalid_user_filter_type",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "folder", //invalid type
					},
				},
			},
			model:             model,
			expectedErrorCode: codes.Code(2021),
		},
		{
			name: "invalid_user_filter_relation",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "user",
						Relation: "editor", //invalid relation
					},
				},
			},
			model:             model,
			expectedErrorCode: codes.Code(2022),
		},
		{
			name: "invalid_target_object_type",
			req: &openfgav1.ListUsersRequest{
				Object: &openfgav1.Object{
					Type: "folder", // invalid type
					Id:   "1",
				},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model:             model,
			expectedErrorCode: codes.Code(2021),
		},
		{
			name: "invalid_relation",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "owner", // invalid relation
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model:             model,
			expectedErrorCode: codes.Code(2022),
		},
		{
			name: "contextual_tuple_invalid_object_type",
			req: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{{Type: "user"}},
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("invalid_object_type:1", "viewer", "user:will"),
					},
				},
			},
			model:             model,
			expectedErrorCode: codes.Code(2027),
		},
		{
			name: "contextual_tuple_invalid_user_type",
			req: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{{Type: "user"}},
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("document:1", "viewer", "invalid_user_type:will"),
					},
				},
			},
			model:             model,
			expectedErrorCode: codes.Code(2027),
		},
		{
			name: "contextual_tuple_invalid_relation",
			req: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{{Type: "user"}},
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("document:1", "invalid_relation", "user:will"),
					},
				},
			},
			model:             model,
			expectedErrorCode: codes.Code(2027),
		},
	}

	storeID := ulid.Make().String()
	for _, test := range tests {
		ds := memory.New()
		t.Cleanup(ds.Close)
		model := testutils.MustTransformDSLToProtoWithID(test.model)

		t.Run(test.name, func(t *testing.T) {
			typesys, err := typesystem.NewAndValidate(context.Background(), model)
			require.NoError(t, err)

			err = ds.WriteAuthorizationModel(context.Background(), storeID, model)
			require.NoError(t, err)

			s := MustNewServerWithOpts(
				WithDatastore(ds),
			)
			s.experimentals = []ExperimentalFeatureFlag{ExperimentalEnableListUsers}
			t.Cleanup(s.Close)

			ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)

			test.req.AuthorizationModelId = model.GetId()
			test.req.StoreId = storeID

			_, err = s.ListUsers(ctx, test.req)
			e, ok := status.FromError(err)
			require.True(t, ok)

			fmt.Println("Actual", e.Code().String(), "Expected", test.expectedErrorCode.String())
			require.Equal(t, test.expectedErrorCode, e.Code())
		})
	}
}

func TestModelIdNotFound(t *testing.T) {
	ctx := context.Background()

	req := &openfgav1.ListUsersRequest{
		StoreId: "some-store-id",
	}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), gomock.Any()).Return(nil, storage.ErrNotFound)

	server := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)
	server.experimentals = []ExperimentalFeatureFlag{ExperimentalEnableListUsers}
	t.Cleanup(server.Close)

	resp, err := server.ListUsers(ctx, req)
	require.Nil(t, resp)
	require.Error(t, err)

	e, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(2020), e.Code())
}

func TestExperimentalListUsers(t *testing.T) {
	ctx := context.Background()

	req := &openfgav1.ListUsersRequest{}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), gomock.Any()).Return(nil, storage.ErrNotFound) // error demonstrates that main code path is reached

	server := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)
	t.Cleanup(server.Close)

	t.Run("list_users_errors_if_not_experimentally_enabled", func(t *testing.T) {
		_, err := server.ListUsers(ctx, req)
		require.Error(t, err)
		require.Equal(t, "rpc error: code = Unimplemented desc = ListUsers is not enabled. It can be enabled for experimental use by passing the `--experimentals enable-list-users` configuration option when running OpenFGA server", err.Error())

		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Unimplemented, e.Code())
	})

	t.Run("list_users_does_not_error_if_experimentally_enabled", func(t *testing.T) {
		server.experimentals = []ExperimentalFeatureFlag{ExperimentalEnableListUsers}
		_, err := server.ListUsers(ctx, req)

		require.Error(t, err)
		require.Equal(t, "authorization model not found", err.Error())
	})
}
