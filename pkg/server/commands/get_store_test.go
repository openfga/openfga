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
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

func TestGetStore(t *testing.T) {
	t.Run("succeeds", func(t *testing.T) {
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

		resp, err := NewGetStoreQuery(mockDatastore).Execute(context.Background(), &openfgav1.GetStoreRequest{
			StoreId: store.GetId(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, store.GetId(), resp.GetId())
		require.Equal(t, store.GetCreatedAt(), resp.GetCreatedAt())
		require.Equal(t, store.GetUpdatedAt(), resp.GetUpdatedAt())
	})

	t.Run("fails_if_store_not_found", func(t *testing.T) {
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

		resp, err := NewGetStoreQuery(mockDatastore).Execute(context.Background(), &openfgav1.GetStoreRequest{
			StoreId: store.GetId(),
		})
		require.Equal(t, err, serverErrors.ErrStoreIDNotFound)
		require.Nil(t, resp)
	})

	t.Run("fails_if_error_from_datastore", func(t *testing.T) {
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

		resp, err := NewGetStoreQuery(mockDatastore).Execute(context.Background(), &openfgav1.GetStoreRequest{
			StoreId: store.GetId(),
		})
		require.Error(t, err)
		require.Nil(t, resp)
	})
}
