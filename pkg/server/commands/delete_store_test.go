package commands

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestDeleteStore(t *testing.T) {
	t.Run("delete_succeeds_if_datastore_can_get", func(t *testing.T) {
		now := timestamppb.New(time.Now().UTC())
		store := &openfgav1.Store{
			Id:        ulid.Make().String(),
			Name:      "acme",
			CreatedAt: now,
			UpdatedAt: now,
		}
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().GetStore(gomock.Any(), store.GetId()).Times(1).Return(store, nil)
		mockDatastore.EXPECT().DeleteStore(gomock.Any(), store.GetId()).Times(1).Return(nil)

		resp, err := NewDeleteStoreCommand(mockDatastore).Execute(context.Background(), &openfgav1.DeleteStoreRequest{
			StoreId: store.GetId(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("delete_succeeds_if_datastore_returns_not_found", func(t *testing.T) {
		now := timestamppb.New(time.Now().UTC())
		store := &openfgav1.Store{
			Id:        ulid.Make().String(),
			Name:      "acme",
			CreatedAt: now,
			UpdatedAt: now,
		}
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().GetStore(gomock.Any(), store.GetId()).Times(1).Return(nil, storage.ErrNotFound)

		resp, err := NewDeleteStoreCommand(mockDatastore).Execute(context.Background(), &openfgav1.DeleteStoreRequest{
			StoreId: store.GetId(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("delete_fails_if_datastore_returns_error_when_getting", func(t *testing.T) {
		now := timestamppb.New(time.Now().UTC())
		store := &openfgav1.Store{
			Id:        ulid.Make().String(),
			Name:      "acme",
			CreatedAt: now,
			UpdatedAt: now,
		}
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().GetStore(gomock.Any(), store.GetId()).Times(1).Return(nil, errors.New("internal"))

		resp, err := NewDeleteStoreCommand(mockDatastore).Execute(context.Background(), &openfgav1.DeleteStoreRequest{
			StoreId: store.GetId(),
		})
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("delete_fails_if_datastore_returns_error_when_deleting", func(t *testing.T) {
		now := timestamppb.New(time.Now().UTC())
		store := &openfgav1.Store{
			Id:        ulid.Make().String(),
			Name:      "acme",
			CreatedAt: now,
			UpdatedAt: now,
		}
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().GetStore(gomock.Any(), store.GetId()).Times(1).Return(store, nil)
		mockDatastore.EXPECT().DeleteStore(gomock.Any(), store.GetId()).Times(1).Return(errors.New("internal"))

		resp, err := NewDeleteStoreCommand(mockDatastore).Execute(context.Background(), &openfgav1.DeleteStoreRequest{
			StoreId: store.GetId(),
		})
		require.Error(t, err)
		require.Nil(t, resp)
	})
}
