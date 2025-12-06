package graph

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReverseExpandCheckResolver(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("delegates_when_disabled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		resolver, err := NewReverseExpandCheckResolver(false)
		require.NoError(t, err)

		mockDelegate := NewMockCheckResolver(ctrl)
		resolver.SetDelegate(mockDelegate)

		req := &ResolveCheckRequest{
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		}

		mockDelegate.EXPECT().ResolveCheck(gomock.Any(), req).Times(1).Return(&ResolveCheckResponse{Allowed: true}, nil)

		ctx := context.Background()
		resp, err := resolver.ResolveCheck(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.Allowed)
	})

	t.Run("delegates_when_typesystem_missing_from_context", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		resolver, err := NewReverseExpandCheckResolver(
			true,
			WithReverseExpandCheckResolverLogger(logger.NewNoopLogger()),
			WithReverseExpandCheckResolverExecutorFactory(func(ds storage.RelationshipTupleReader, ts *typesystem.TypeSystem) ReverseExpandQueryExecutor {
				return nil // Return nil to test delegation
			}),
		)
		require.NoError(t, err)

		mockDelegate := NewMockCheckResolver(ctrl)
		resolver.SetDelegate(mockDelegate)

		req := &ResolveCheckRequest{
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		}

		mockDelegate.EXPECT().ResolveCheck(gomock.Any(), req).Times(1).Return(&ResolveCheckResponse{Allowed: true}, nil)

		ctx := context.Background()
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		resp, err := resolver.ResolveCheck(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.Allowed)
	})

	t.Run("delegates_when_datastore_missing_from_context", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		model := &openfgav1.AuthorizationModel{
			Id:              "test",
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{},
		}
		typesys, err := typesystem.NewAndValidate(context.Background(), model)
		require.NoError(t, err)

		resolver, err := NewReverseExpandCheckResolver(
			true,
			WithReverseExpandCheckResolverLogger(logger.NewNoopLogger()),
		)
		require.NoError(t, err)

		mockDelegate := NewMockCheckResolver(ctrl)
		resolver.SetDelegate(mockDelegate)

		req := &ResolveCheckRequest{
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		}

		mockDelegate.EXPECT().ResolveCheck(gomock.Any(), req).Times(1).Return(&ResolveCheckResponse{Allowed: true}, nil)

		ctx := context.Background()
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)
		resp, err := resolver.ResolveCheck(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.Allowed)
	})

	t.Run("delegates_when_heuristic_suggests_not_beneficial", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Create a simple model without tuple-to-userset (which triggers reverse expansion)
		model := &openfgav1.AuthorizationModel{
			Id:            "test",
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									typesystem.DirectRelationReference("user", ""),
								},
							},
						},
					},
				},
			},
		}
		typesys, err := typesystem.NewAndValidate(context.Background(), model)
		require.NoError(t, err)

		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		resolver, err := NewReverseExpandCheckResolver(
			true,
			WithReverseExpandCheckResolverLogger(logger.NewNoopLogger()),
			WithReverseExpandCheckResolverTimeout(1*time.Second),
			WithReverseExpandCheckResolverExecutorFactory(func(ds storage.RelationshipTupleReader, ts *typesystem.TypeSystem) ReverseExpandQueryExecutor {
				return nil // Return nil to test delegation
			}),
		)
		require.NoError(t, err)

		mockDelegate := NewMockCheckResolver(ctrl)
		resolver.SetDelegate(mockDelegate)

		req := &ResolveCheckRequest{
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		}

		mockDelegate.EXPECT().ResolveCheck(gomock.Any(), req).Times(1).Return(&ResolveCheckResponse{Allowed: true}, nil)

		ctx := context.Background()
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		resp, err := resolver.ResolveCheck(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.Allowed)
	})

	t.Run("converts_user_to_user_ref_correctly", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		resolver, err := NewReverseExpandCheckResolver(true)
		require.NoError(t, err)

		tests := []struct {
			name     string
			user     string
			expected string
		}{
			{
				name:     "direct_user",
				user:     "user:alice",
				expected: "user:alice",
			},
			{
				name:     "wildcard",
				user:     "user:*",
				expected: "user:*",
			},
			{
				name:     "userset",
				user:     "group:eng#member",
				expected: "group:eng#member",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				userRef := resolver.convertUserToUserRef(test.user)
				require.NotNil(t, userRef)
				require.Equal(t, test.expected, userRef.String())
			})
		}
	})
}
