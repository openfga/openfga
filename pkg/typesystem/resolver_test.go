package typesystem

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

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
		resolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore)
		require.NoError(t, err)
		defer resolverStop()

		_, err = resolver(context.Background(), store, invalidModelID)
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

		resolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore)
		require.NoError(t, err)
		defer resolverStop()

		_, err = resolver(context.Background(), store, "")
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

		resolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore)
		require.NoError(t, err)
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

		resolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore)
		require.NoError(t, err)
		defer resolverStop()

		_, err = resolver(context.Background(), store, modelID)
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

		resolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore)
		require.NoError(t, err)
		defer resolverStop()
		typesys, err := resolver(context.Background(), store, modelID)
		require.NoError(t, err)
		require.Equal(t, modelID, typesys.GetAuthorizationModelID())
	})

	// This test is for the scenario where an invalid model was somehow written to the store.
	// We want to make sure it's validated again (maybe with more rules) when we read it.
	t.Run("validation_of_model_fails", func(t *testing.T) {
		store := ulid.Make().String()
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1

			type user
			type group
				relations
					define member: [user_invalid]`)
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)

		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), store).
			Return(model, nil).
			Times(1)

		resolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore)
		require.NoError(t, err)
		defer resolverStop()

		_, err = resolver(context.Background(), store, "")
		require.ErrorContains(t, err, "invalid authorization model encountered: the relation type 'user_invalid' on 'member' in object type 'group' is not valid")
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

		resolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore)
		require.NoError(t, err)
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

		resolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore)
		require.NoError(t, err)
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

	t.Run("returns_different_latest_models_for_same_store", func(t *testing.T) {
		store := ulid.Make().String()
		modelOne := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1

			type user`)
		modelTwo := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1

			type user2`)
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)

		resolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore)
		require.NoError(t, err)
		defer resolverStop()

		// first read returns modelOne
		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), store).
			Return(modelOne, nil).
			Times(1)
		typesys, err := resolver(context.Background(), store, "")
		require.NoError(t, err)
		require.Equal(t, modelOne.GetId(), typesys.GetAuthorizationModelID())

		// simulate a write of a new model
		mockDatastore.EXPECT().FindLatestAuthorizationModel(gomock.Any(), store).
			Return(modelTwo, nil).
			Times(1)

		// second read returns modelTwo
		typesys, err = resolver(context.Background(), store, "")
		require.NoError(t, err)
		require.Equal(t, modelTwo.GetId(), typesys.GetAuthorizationModelID())
	})
}
