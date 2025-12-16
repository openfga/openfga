package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/test"
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "folder", // invalid type
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "user",
						Relation: "editor", // invalid relation
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("invalid_object_type:1", "viewer", "user:will"),
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
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "invalid_user_type:will"),
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
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "invalid_relation", "user:will"),
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
			t.Cleanup(s.Close)

			ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)

			test.req.AuthorizationModelId = model.GetId()
			test.req.StoreId = storeID

			_, err = s.ListUsers(ctx, test.req)
			e, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.expectedErrorCode, e.Code())
		})
	}
}

func TestModelIdNotFound(t *testing.T) {
	ctx := context.Background()

	req := &openfgav1.ListUsersRequest{
		StoreId: ulid.Make().String(),
		Object: &openfgav1.Object{
			Type: "document",
			Id:   "1",
		},
		Relation: "viewer",
		UserFilters: []*openfgav1.UserTypeFilter{
			{Type: "user"},
		},
	}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), gomock.Any()).Return(nil, storage.ErrNotFound)

	server := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)
	t.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		server.Close()
	})

	resp, err := server.ListUsers(ctx, req)
	require.Nil(t, resp)
	require.Error(t, err)

	e, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(2020), e.Code())
}

func TestExperimentalListUsers(t *testing.T) {
	ctx := context.Background()

	storeID := ulid.Make().String()

	req := &openfgav1.ListUsersRequest{
		StoreId: storeID,
		Object: &openfgav1.Object{
			Type: "document",
			Id:   "1",
		},
		Relation: "viewer",
		UserFilters: []*openfgav1.UserTypeFilter{
			{Type: "user"},
		},
	}
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	server := MustNewServerWithOpts(
		WithDatastore(mockDatastore),
	)
	t.Cleanup(func() {
		mockDatastore.EXPECT().Close().Times(1)
		server.Close()
	})

	t.Run("list_users_returns_error_if_latest_model_not_found", func(t *testing.T) {
		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), gomock.Any()).Return(nil, storage.ErrNotFound) // error demonstrates that main code path is reached

		_, err := server.ListUsers(ctx, req)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_latest_authorization_model_not_found), st.Code())
	})

	t.Run("list_users_returns_error_if_model_not_found", func(t *testing.T) {
		mockDatastore.EXPECT().
			ReadAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, storage.ErrNotFound)

		_, err := server.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:              storeID,
			AuthorizationModelId: ulid.Make().String(),
			Object: &openfgav1.Object{
				Type: "document",
				Id:   "1",
			},
			Relation: "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{
				{Type: "user"},
			},
		})

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.ErrorCode_authorization_model_not_found), st.Code())
	})
}

func TestListUsers_ErrorCases(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	store := ulid.Make().String()

	t.Run("graph_resolution_errors", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(memory.New()),
			WithResolveNodeLimit(2),
		)
		t.Cleanup(s.Close)

		writeModelResp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       store,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user

				type group
					relations
						define member: [user, group#member]

				type document
					relations
						define viewer: [group#member]`).GetTypeDefinitions(),
		})
		require.NoError(t, err)

		_, err = s.Write(ctx, &openfgav1.WriteRequest{
			StoreId: store,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "group:1#member"),
					tuple.NewTupleKey("group:1", "member", "group:2#member"),
					tuple.NewTupleKey("group:2", "member", "group:3#member"),
					tuple.NewTupleKey("group:3", "member", "user:jon"),
				},
			},
		})
		require.NoError(t, err)

		t.Run("resolution_depth_exceeded_error_unary", func(t *testing.T) {
			res, err := s.ListUsers(ctx, &openfgav1.ListUsersRequest{
				StoreId:              store,
				AuthorizationModelId: writeModelResp.GetAuthorizationModelId(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			})

			require.Nil(t, res)
			require.ErrorIs(t, err, serverErrors.ErrAuthorizationModelResolutionTooComplex)
		})
	})
}

func TestListUsers_Deadline(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	t.Run("return_no_error_and_partial_results_at_deadline", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)

		modelStr := `
			model
				schema 1.1
			type user

			type group
			relations
				define member: [user]

			type document
			relations
				define viewer: [user, group#member]`

		tuples := []string{
			"document:1#viewer@user:jon",
			"document:1#viewer@group:fga#member",
			"group:fga#member@user:maria",
		}

		storeID, model := test.BootstrapFGAStore(t, ds, modelStr, tuples)

		ds = mockstorage.NewMockSlowDataStorage(ds, 20*time.Millisecond)
		t.Cleanup(ds.Close)

		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithListUsersDeadline(30*time.Millisecond), // 30ms is enough for first read, but not others
		)
		t.Cleanup(s.Close)

		resp, err := s.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:              storeID,
			AuthorizationModelId: model.GetId(),
			Object: &openfgav1.Object{
				Type: "document",
				Id:   "1",
			},
			Relation: "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{
				{Type: "user"},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.GetUsers(), 1)
	})

	t.Run("return_no_error_and_partial_results_if_throttled_until_deadline", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)

		modelStr := `
			model
				schema 1.1
			type user

			type group
			relations
				define member: [user, group#member]

			type document
			relations
				define viewer: [user, group#member]`

		tuples := []string{
			"document:1#viewer@user:jon", // Observed before first dispatch
			"document:1#viewer@group:eng#member",
			"group:eng#member@group:backend#member",
			"group:backend#member@user:tyler", // Requires two dispatches, gets throtled
		}

		storeID, model := test.BootstrapFGAStore(t, ds, modelStr, tuples)
		t.Cleanup(ds.Close)

		deadline := 30 * time.Millisecond

		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithListUsersDeadline(deadline),
		)
		t.Cleanup(s.Close)

		resp, err := s.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:              storeID,
			AuthorizationModelId: model.GetId(),
			Object: &openfgav1.Object{
				Type: "document",
				Id:   "1",
			},
			Relation: "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{
				{Type: "user"},
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.GetUsers(), 1)
	})

	t.Run("internal_error_without_meeting_deadline", func(t *testing.T) {
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

		storeID := ulid.Make().String()
		modelID := ulid.Make().String()

		mockDatastore.EXPECT().
			ReadAuthorizationModel(gomock.Any(), storeID, modelID).
			Return(
				testutils.MustTransformDSLToProtoWithID(`
					model
						schema 1.1
					type user

					type document
						relations
							define viewer: [user]`),
				nil,
			).
			Times(1)
		mockDatastore.EXPECT().Close().Times(1)

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("internal error from storage")).
			Times(1)

		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithListUsersDeadline(1*time.Minute),
		)
		t.Cleanup(s.Close)

		resp, err := s.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Object: &openfgav1.Object{
				Type: "document",
				Id:   "1",
			},
			Relation: "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{
				{Type: "user"},
			},
		})
		require.Nil(t, resp)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.InternalErrorCode_internal_error), st.Code())
	})

	t.Run("internal_storage_error_after_deadline", func(t *testing.T) {
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

		storeID := ulid.Make().String()
		modelID := ulid.Make().String()

		mockDatastore.EXPECT().
			ReadAuthorizationModel(gomock.Any(), storeID, modelID).
			Return(
				testutils.MustTransformDSLToProtoWithID(`
					model
						schema 1.1
					type user

					type document
						relations
							define viewer: [user]`),
				nil,
			).
			Times(1)

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, storeID string, filter storage.ReadFilter, _ storage.ReadOptions) (storage.TupleIterator, error) {
				time.Sleep(10 * time.Millisecond)
				return nil, context.Canceled
			}).
			Times(1)
		mockDatastore.EXPECT().Close().Times(1)

		s := MustNewServerWithOpts(
			WithDatastore(mockDatastore),
			WithListUsersDeadline(5*time.Millisecond),
		)
		t.Cleanup(s.Close)

		resp, err := s.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Object: &openfgav1.Object{
				Type: "document",
				Id:   "1",
			},
			Relation: "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{
				{Type: "user"},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.GetUsers())
	})
}

func TestUserFiltersToString(t *testing.T) {
	require.Equal(t, "user", userFiltersToString([]*openfgav1.UserTypeFilter{{
		Type: "user",
	}}))

	require.Equal(t, "group#member", userFiltersToString([]*openfgav1.UserTypeFilter{{
		Type:     "group",
		Relation: "member",
	}}))
}

func TestListUsers_WithListUsersDatabaseThrottle(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	t.Run("WithListUsersDatabaseThrottle_option_sets_configuration", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)

		modelStr := `
			model
				schema 1.1
			type user
			type document
				relations
					define viewer: [user]`

		storeID, model := test.BootstrapFGAStore(t, ds, modelStr, []string{
			"document:1#viewer@user:jon",
		})

		threshold := 100
		duration := 50 * time.Millisecond

		// Create server with datastore throttling options
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithListUsersDatabaseThrottle(threshold, duration),
		)
		t.Cleanup(s.Close)

		// Verify the options were set correctly
		require.Equal(t, threshold, s.listUsersDatastoreThrottleThreshold)
		require.Equal(t, duration, s.listUsersDatastoreThrottleDuration)

		// Verify the endpoint still works correctly
		resp, err := s.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:              storeID,
			AuthorizationModelId: model.GetId(),
			Object: &openfgav1.Object{
				Type: "document",
				Id:   "1",
			},
			Relation: "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{
				{Type: "user"},
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.GetUsers(), 1)
	})
}
