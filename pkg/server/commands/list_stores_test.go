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
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

func TestListStores(t *testing.T) {
	stores := []*openfgav1.Store{
		{
			Name:      "store1",
			Id:        ulid.Make().String(),
			CreatedAt: timestamppb.New(time.Now().UTC()),
			UpdatedAt: timestamppb.New(time.Now().UTC()),
		},
	}

	t.Run("success", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().
			ListStores(gomock.Any(), storage.ListStoresOptions{
				IDs:  []string{"store1"},
				Name: "storeName",
				Pagination: storage.PaginationOptions{
					PageSize: 1,
					From:     "",
				},
			}).
			Return([]*openfgav1.Store{stores[0]}, "", nil)

		resp, err := NewListStoresQuery(mockDatastore).Execute(context.Background(), &openfgav1.ListStoresRequest{
			PageSize:          wrapperspb.Int32(1),
			Name:              "storeName",
			ContinuationToken: "",
		}, []string{"store1"})
		require.NoError(t, err)
		require.Len(t, resp.GetStores(), 1)
		require.Equal(t, stores[0].GetName(), resp.GetStores()[0].GetName())
		require.Empty(t, resp.GetContinuationToken())
	})

	t.Run("error_decoding_token", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockEncoder := mocks.NewMockEncoder(mockController)
		mockEncoder.EXPECT().Decode(gomock.Any()).Return(nil, errors.New("error"))

		cmd := NewListStoresQuery(mockDatastore, WithListStoresQueryEncoder(mockEncoder))
		resp, actualError := cmd.Execute(context.Background(), &openfgav1.ListStoresRequest{
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: "",
		}, []string{"store1"})
		require.Nil(t, resp)
		require.ErrorIs(t, actualError, serverErrors.ErrInvalidContinuationToken)
	})

	t.Run("error_encoding_token", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockEncoder := mocks.NewMockEncoder(mockController)
		gomock.InOrder(
			mockEncoder.EXPECT().Decode(gomock.Any()).Return([]byte{}, nil),
			mockDatastore.EXPECT().ListStores(gomock.Any(), gomock.Any()).Return([]*openfgav1.Store{stores[0]}, "cont-token", nil),
			mockEncoder.EXPECT().Encode(gomock.Any()).Return("", errors.New("error")),
		)

		cmd := NewListStoresQuery(mockDatastore, WithListStoresQueryEncoder(mockEncoder))
		resp, err := cmd.Execute(context.Background(), &openfgav1.ListStoresRequest{
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: "",
		}, []string{"store1"})
		require.Nil(t, resp)
		require.Error(t, err)
	})

	t.Run("error_from_datastore", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().ListStores(gomock.Any(), gomock.Any()).Return(nil, "", errors.New("internal"))

		resp, err := NewListStoresQuery(mockDatastore).Execute(context.Background(), &openfgav1.ListStoresRequest{
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: "",
		}, []string{"store1"})
		require.Nil(t, resp)
		require.Error(t, err)
	})
}
