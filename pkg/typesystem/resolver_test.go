package typesystem

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

func TestMemoizedTypesystemResolverFunc(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("invalid_model_id_returns_model_not_found", func(t *testing.T) {
		store := ulid.Make().String()
		invalidModelID := "invalid"
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)
		resolver, resolverStop := MemoizedTypesystemResolverFunc(
			mockDatastore,
		)
		defer resolverStop()

		_, err := resolver(context.Background(), store, invalidModelID)
		require.ErrorIs(t, err, ErrModelNotFound)
	})

	t.Run("empty_model_id_and_no_latest_returns_model_not_found", func(t *testing.T) {
		store := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), store).
			Return(nil, storage.ErrNotFound).
			Times(1)

		resolver, resolverStop := MemoizedTypesystemResolverFunc(
			mockDatastore,
		)
		defer resolverStop()

		_, err := resolver(context.Background(), store, "")
		require.ErrorIs(t, err, ErrModelNotFound)
	})

	t.Run("empty_model_id_returns_latest", func(t *testing.T) {
		store := ulid.Make().String()
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1

			type user`)
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)

		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), store).
			Return(model, nil).
			Times(1)

		resolver, resolverStop := MemoizedTypesystemResolverFunc(
			mockDatastore,
		)
		defer resolverStop()

		typesys, err := resolver(context.Background(), store, "")
		require.NoError(t, err)
		require.Equal(t, model.GetId(), typesys.GetAuthorizationModelID())
	})

	t.Run("model_not_found", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).
			Return(nil, storage.ErrNotFound).
			Times(1)

		resolver, resolverStop := MemoizedTypesystemResolverFunc(
			mockDatastore,
		)
		defer resolverStop()
		_, err := resolver(context.Background(), store, modelID)
		require.ErrorIs(t, err, ErrModelNotFound)
	})

	t.Run("returns_specific_model_id", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).
			Return(
				&openfgav1.AuthorizationModel{
					Id:            modelID,
					SchemaVersion: SchemaVersion1_1,
				},
				nil,
			).
			Times(1)

		resolver, resolverStop := MemoizedTypesystemResolverFunc(
			mockDatastore,
		)
		defer resolverStop()
		typesys, err := resolver(context.Background(), store, modelID)
		require.NoError(t, err)
		require.Equal(t, modelID, typesys.GetAuthorizationModelID())
	})

	t.Run("two_calls_same_model_id_returns_second_from_cache", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).
			Return(
				&openfgav1.AuthorizationModel{
					Id:            modelID,
					SchemaVersion: SchemaVersion1_1,
				},
				nil,
			).
			Times(1)

		resolver, resolverStop := MemoizedTypesystemResolverFunc(
			mockDatastore,
		)
		defer resolverStop()

		// first call
		typesys, err := resolver(context.Background(), store, modelID)
		require.NoError(t, err)
		require.Equal(t, modelID, typesys.GetAuthorizationModelID())

		// second call from cache asserted by the Times(1) above
		typesys, err = resolver(context.Background(), store, modelID)
		require.NoError(t, err)
		require.Equal(t, modelID, typesys.GetAuthorizationModelID())
	})

	t.Run("two_calls_without_model_id_returns_second_from_cache", func(t *testing.T) {
		store := ulid.Make().String()
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1

			type user`)
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)

		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), store).
			Return(model, nil).
			Times(2)

		resolver, resolverStop := MemoizedTypesystemResolverFunc(
			mockDatastore,
		)
		defer resolverStop()

		// first call
		typesys, err := resolver(context.Background(), store, "")
		require.NoError(t, err)
		require.Equal(t, model.GetId(), typesys.GetAuthorizationModelID())

		// second call from cache, so we skip model validation
		typesys, err = resolver(context.Background(), store, "")
		require.NoError(t, err)
		require.Equal(t, model.GetId(), typesys.GetAuthorizationModelID())
	})
}
