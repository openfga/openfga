package commands

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

func TestCreateStore(t *testing.T) {
	tests := []struct {
		name        string
		request     *openfgav1.CreateStoreRequest
		expectError bool
	}{
		{
			name: "succeeds",
			request: &openfgav1.CreateStoreRequest{
				Name: testutils.CreateRandomString(10),
			},
			expectError: false,
		},
		{
			name: "datastore_error",
			request: &openfgav1.CreateStoreRequest{
				Name: testutils.CreateRandomString(10),
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
			if !tc.expectError {
				mockDatastore.EXPECT().CreateStore(gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
						now := timestamppb.New(time.Now().UTC())
						return &openfgav1.Store{
							Id:        store.GetId(),
							Name:      tc.request.GetName(),
							CreatedAt: now,
							UpdatedAt: now,
						}, nil
					})
			} else {
				mockDatastore.EXPECT().CreateStore(gomock.Any(), gomock.Any()).Return(nil, storage.ErrCollision)
			}

			resp, err := NewCreateStoreCommand(mockDatastore).Execute(context.Background(), tc.request)
			if tc.expectError {
				require.Error(t, err)
				require.Nil(t, resp)
				return
			}
			require.NoError(t, err)

			require.Equal(t, tc.request.GetName(), resp.GetName())

			_, err = ulid.Parse(resp.GetId())
			require.NoError(t, err)

			require.NotEmpty(t, resp.GetCreatedAt())
			require.NotEmpty(t, resp.GetUpdatedAt())
			require.Equal(t, resp.GetCreatedAt(), resp.GetUpdatedAt())
		})
	}
}
