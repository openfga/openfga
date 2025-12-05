package modelgraph

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

func TestNewResolver(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
	mockCache := mocks.NewMockInMemoryCache[any](ctrl)
	ttl := 10 * time.Minute

	resolver := NewResolver(mockDatastore, mockCache, ttl)

	require.NotNil(t, resolver)
	require.Equal(t, mockDatastore, resolver.datastore)
	require.Equal(t, mockCache, resolver.cache)
	require.Equal(t, ttl, resolver.ttl)
}

func TestResolve(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	t.Run("returns_error_for_invalid_model_id", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		resolver := NewResolver(mockDatastore, mockCache, 10*time.Minute)

		graph, err := resolver.Resolve(context.Background(), storeID, "invalid-ulid")
		require.Error(t, err)
		require.Equal(t, ErrModelNotFound, err)
		require.Nil(t, graph)
	})

	t.Run("retrieves_latest_model_when_model_id_is_empty", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		resolver := NewResolver(mockDatastore, mockCache, 10*time.Minute)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user]
  `)

		mockCache.EXPECT().Get("wg|" + storeID + "|" + model.GetId()).Return(nil)
		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), storeID).Return(model, nil)
		mockCache.EXPECT().Set("wg|"+storeID+"|"+model.GetId(), gomock.Any(), 10*time.Minute)

		graph, err := resolver.Resolve(context.Background(), storeID, "")
		require.NoError(t, err)
		require.NotNil(t, graph)
		require.Equal(t, model.GetId(), graph.GetModelID())
	})

	t.Run("returns_error_when_latest_model_not_found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		resolver := NewResolver(mockDatastore, mockCache, 10*time.Minute)

		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), storeID).Return(nil, storage.ErrNotFound)

		graph, err := resolver.Resolve(context.Background(), storeID, "")
		require.Error(t, err)
		require.Equal(t, ErrModelNotFound, err)
		require.Nil(t, graph)
	})

	t.Run("returns_error_when_FindLatestAuthorizationModel_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		resolver := NewResolver(mockDatastore, mockCache, 10*time.Minute)

		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), storeID).Return(nil, errors.New("database error"))

		graph, err := resolver.Resolve(context.Background(), storeID, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to FindLatestAuthorizationModel")
		require.Nil(t, graph)
	})

	t.Run("returns_cached_graph_when_available", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		resolver := NewResolver(mockDatastore, mockCache, 10*time.Minute)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user]
  `)

		cachedGraph, err := New(model)
		require.NoError(t, err)

		mockCache.EXPECT().Get("wg|" + storeID + "|" + modelID).Return(cachedGraph)

		graph, err := resolver.Resolve(context.Background(), storeID, modelID)
		require.NoError(t, err)
		require.NotNil(t, graph)
		require.Equal(t, cachedGraph, graph)
	})

	t.Run("reads_and_caches_model_when_not_cached", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		resolver := NewResolver(mockDatastore, mockCache, 10*time.Minute)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user]
  `)

		mockCache.EXPECT().Get("wg|" + storeID + "|" + modelID).Return(nil)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
		mockCache.EXPECT().Set("wg|"+storeID+"|"+modelID, gomock.Any(), 10*time.Minute)

		graph, err := resolver.Resolve(context.Background(), storeID, modelID)
		require.NoError(t, err)
		require.NotNil(t, graph)
		require.Equal(t, model.GetId(), graph.GetModelID())
	})

	t.Run("returns_error_when_model_not_found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		resolver := NewResolver(mockDatastore, mockCache, 10*time.Minute)

		mockCache.EXPECT().Get("wg|" + storeID + "|" + modelID).Return(nil)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(nil, storage.ErrNotFound)

		graph, err := resolver.Resolve(context.Background(), storeID, modelID)
		require.Error(t, err)
		require.Equal(t, ErrModelNotFound, err)
		require.Nil(t, graph)
	})

	t.Run("returns_error_when_ReadAuthorizationModel_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		resolver := NewResolver(mockDatastore, mockCache, 10*time.Minute)

		mockCache.EXPECT().Get("wg|" + storeID + "|" + modelID).Return(nil)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(nil, errors.New("database error"))

		graph, err := resolver.Resolve(context.Background(), storeID, modelID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to ReadAuthorizationModel")
		require.Nil(t, graph)
	})

	t.Run("returns_error_when_model_is_invalid", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockAuthorizationModelReadBackend(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		resolver := NewResolver(mockDatastore, mockCache, 10*time.Minute)

		invalidModel := &openfgav1.AuthorizationModel{
			Id:            modelID,
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": {
							Userset: &openfgav1.Userset_Union{
								Union: &openfgav1.Usersets{
									Child: []*openfgav1.Userset{},
								},
							},
						},
					},
				},
			},
		}

		mockCache.EXPECT().Get("wg|" + storeID + "|" + modelID).Return(nil)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(invalidModel, nil)

		graph, err := resolver.Resolve(context.Background(), storeID, modelID)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidModel)
		require.Nil(t, graph)
	})
}
