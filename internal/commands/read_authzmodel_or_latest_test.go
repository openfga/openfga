package commands

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestResolveAuthorizationModel(t *testing.T) {
	t.Run("invalid_model_id_returns_error", func(t *testing.T) {
		store := ulid.Make().String()
		invalidModelID := "invalid"
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)

		_, err := NewReadAuthorizationModelOrLatestQuery(mockDatastore).Execute(context.Background(), store, invalidModelID)
		require.Equal(t, serverErrors.AuthorizationModelNotFound(invalidModelID), err)
	})

	t.Run("empty_model_id_and_no_latest_returns_not_found", func(t *testing.T) {
		store := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)
		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return("", storage.ErrNotFound)

		_, err := NewReadAuthorizationModelOrLatestQuery(mockDatastore).Execute(context.Background(), store, "")
		require.ErrorIs(t, err, serverErrors.LatestAuthorizationModelNotFound(store))
	})

	t.Run("empty_model_id_returns_latest", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)

		mockDatastore.EXPECT().FindLatestAuthorizationModelID(gomock.Any(), store).Return(modelID, nil)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).Return(
			typesystem.New(&openfgav1.AuthorizationModel{
				Id:            modelID,
				SchemaVersion: typesystem.SchemaVersion1_1,
			}),
			nil,
		)

		typesys, err := NewReadAuthorizationModelOrLatestQuery(mockDatastore).Execute(context.Background(), store, "")
		require.NoError(t, err)
		require.Equal(t, modelID, typesys.GetAuthorizationModelID())
	})

	t.Run("model_not_found", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).Return(nil, storage.ErrNotFound)

		_, err := NewReadAuthorizationModelOrLatestQuery(mockDatastore).Execute(context.Background(), store, modelID)
		require.ErrorIs(t, err, serverErrors.AuthorizationModelNotFound(modelID))
	})

	t.Run("returns_model", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockAuthorizationModelReadBackend(mockController)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), store, modelID).Return(
			typesystem.New(&openfgav1.AuthorizationModel{
				Id:            modelID,
				SchemaVersion: typesystem.SchemaVersion1_1,
			}),
			nil,
		)

		typesys, err := NewReadAuthorizationModelOrLatestQuery(mockDatastore).Execute(context.Background(), store, modelID)
		require.NoError(t, err)
		require.Equal(t, modelID, typesys.GetAuthorizationModelID())
	})
}
