package commands

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

func TestReadChangesQuery(t *testing.T) {
	t.Run("defaults_to_zero_horizon_offset", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		cmd := NewReadChangesQuery(mockDatastore)
		assert.Equal(t, time.Duration(0), cmd.horizonOffset)
	})

	t.Run("calls_storage_read_changes", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		storeID := ulid.Make().String()

		reqStore := storeID
		reqPageSize := 5
		reqToken := ""
		reqType := "folder"
		horizonOffset := 4 * time.Minute

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		// Assert on specific inputs passed in.
		opts := storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{
				PageSize: reqPageSize,
				From:     reqToken,
			},
		}

		filter := storage.ReadChangesFilter{
			ObjectType:    reqType,
			HorizonOffset: horizonOffset,
		}

		mockDatastore.EXPECT().ReadChanges(gomock.Any(), reqStore, filter, opts).Times(1)

		cmd := NewReadChangesQuery(mockDatastore, WithReadChangeQueryHorizonOffset(int(horizonOffset.Minutes())))
		_, err := cmd.Execute(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:           reqStore,
			Type:              reqType,
			PageSize:          wrapperspb.Int32(int32(reqPageSize)),
			ContinuationToken: reqToken,
		})
		require.NoError(t, err)
	})

	t.Run("uses_start_time_as_filter", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		storeID := ulid.Make().String()
		reqStore := storeID

		startTime, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
		reqToken := ""

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		expectedOpts := storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{
				PageSize: storage.DefaultPageSize,
				From:     "",
			},
		}

		filter := storage.ReadChangesFilter{
			StartTime: startTime,
		}

		mockDatastore.EXPECT().ReadChanges(gomock.Any(), reqStore, filter, expectedOpts).Times(1)

		cmd := NewReadChangesQuery(mockDatastore)

		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:           reqStore,
			ContinuationToken: reqToken,
			StartTime:         timestamppb.New(startTime),
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.GetChanges())
		require.Empty(t, resp.GetContinuationToken())
	})

	t.Run("start_time_is_invalid", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		storeID := ulid.Make().String()
		reqStore := storeID

		startTime := ulid.Time(ulid.MaxTime() + 999_999_999)
		reqToken := ""

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		cmd := NewReadChangesQuery(mockDatastore)

		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:           reqStore,
			ContinuationToken: reqToken,
			StartTime:         timestamppb.New(startTime),
		})

		require.Error(t, err)
		require.ErrorContains(t, err, "Invalid start time")
		require.Nil(t, resp)
	})

	t.Run("uses_continuation_token_over_start_time", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		storeID := ulid.Make().String()
		reqStore := storeID

		startTime, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
		reqToken := "continuationToken"
		respToken := "responsetoken"

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		opts := storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{
				PageSize: storage.DefaultPageSize,
				From:     reqToken,
			},
		}

		filter := storage.ReadChangesFilter{
			StartTime: startTime,
		}

		mockDatastore.EXPECT().ReadChanges(gomock.Any(), reqStore, filter, opts).
			Return([]*openfgav1.TupleChange{}, reqToken, nil).Times(1)

		cmd := NewReadChangesQuery(mockDatastore)

		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:           reqStore,
			ContinuationToken: reqToken,
			StartTime:         timestamppb.New(startTime),
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.GetChanges())
		require.Equal(t, respToken, resp.GetContinuationToken())
	})

	t.Run("throws_error_if_continuation_token_deserialize_fails", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		storeID := ulid.Make().String()
		reqStore := storeID

		startTime, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
		reqToken := "bad_token"

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		cmd := NewReadChangesQuery(mockDatastore)

		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:           reqStore,
			ContinuationToken: reqToken,
			StartTime:         timestamppb.New(startTime),
		})

		require.Error(t, err)
		require.ErrorContains(t, err, "Invalid continuation token")
		require.Nil(t, resp)
	})

	t.Run("throws_error_if_continuation_token_type_mismatch", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		storeID := ulid.Make().String()
		reqStore := storeID

		startTime, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
		reqToken := "bad_token"

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		cmd := NewReadChangesQuery(mockDatastore)

		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:           reqStore,
			ContinuationToken: reqToken,
			StartTime:         timestamppb.New(startTime),
			Type:              "good-type",
		})

		require.Error(t, err)
		require.ErrorContains(t, err, "continuation token don't match")
		require.Nil(t, resp)
	})

	t.Run("throws_error_if_input_continuation_token_is_invalid", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		reqToken := "invalid-token"
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		cmd := NewReadChangesQuery(mockDatastore)
		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:           ulid.Make().String(),
			ContinuationToken: reqToken,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, serverErrors.ErrInvalidContinuationToken)
	})

	t.Run("returns_input_request_token_if_storage_returned_no_results", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		storeID := ulid.Make().String()
		reqStore := storeID
		reqToken := "token"

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		opts := storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{
				PageSize: storage.DefaultPageSize,
				From:     "",
			},
		}

		filter := storage.ReadChangesFilter{}

		mockDatastore.EXPECT().ReadChanges(gomock.Any(), reqStore, filter, opts).Times(1).Return(nil, "", storage.ErrNotFound)

		cmd := NewReadChangesQuery(mockDatastore)
		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:           reqStore,
			ContinuationToken: reqToken,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.GetChanges())
		require.Equal(t, reqToken, resp.GetContinuationToken())
	})
}
