package commands

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/storage"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeleteStore(t *testing.T) {
	type deleteStoreTest struct {
		_name    string
		setup    func()
		request  *openfgav1.DeleteStoreRequest
		response *openfgav1.DeleteStoreResponse
		err      error
	}
	logger := logger.NewNoopLogger()
	ctx := context.Background()
	ignoreUnexported := cmpopts.IgnoreUnexported(openfgav1.DeleteStoreResponse{})

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	const existingStoreID = "01G57S5PZAJC895GQP1VA819G0"

	cmd := NewDeleteStoreCommand(mockDatastore, logger)

	tests := []deleteStoreTest{
		{
			_name: "succeeds if store exists",
			setup: func() {
				mockDatastore.EXPECT().CreateStore(ctx, gomock.Any()).Times(1).Return(&openfgav1.Store{Id: existingStoreID}, nil)
				mockDatastore.EXPECT().GetStore(ctx, existingStoreID).Times(1).Return(&openfgav1.Store{Id: existingStoreID}, nil)
				mockDatastore.EXPECT().DeleteStore(ctx, existingStoreID).Times(1).Return(nil)
				createStoreCommand := NewCreateStoreCommand(mockDatastore, logger)
				_, err := createStoreCommand.Execute(ctx, &openfgav1.CreateStoreRequest{Name: "auth0"})
				if err != nil {
					t.Fatal(err)
				}
			},
			request:  &openfgav1.DeleteStoreRequest{StoreId: existingStoreID},
			response: &openfgav1.DeleteStoreResponse{},
			err:      nil,
		},
		{
			_name: "succeeds if store does not exist (idempotency of delete)",
			setup: func() {
				mockDatastore.EXPECT().GetStore(ctx, "non-existent").Times(1).Return(nil, storage.ErrNotFound)
			},
			request:  &openfgav1.DeleteStoreRequest{StoreId: "non-existent"},
			response: &openfgav1.DeleteStoreResponse{},
			err:      nil,
		},
		{
			_name: "fails if storage fails to get store",
			setup: func() {
				mockDatastore.EXPECT().GetStore(ctx, "some-store").Times(1).Return(nil, errors.New("some storage error"))
			},
			request:  &openfgav1.DeleteStoreRequest{StoreId: "some-store"},
			response: nil,
			err:      status.Error(codes.Code(openfgav1.InternalErrorCode_internal_error), "Internal Server Error"),
		},
		{
			_name: "fails if storage fails to delete store",
			setup: func() {
				mockDatastore.EXPECT().CreateStore(ctx, gomock.Any()).Times(1).Return(&openfgav1.Store{Id: existingStoreID}, nil)
				mockDatastore.EXPECT().GetStore(ctx, existingStoreID).Times(1).Return(&openfgav1.Store{Id: existingStoreID}, nil)
				mockDatastore.EXPECT().DeleteStore(ctx, existingStoreID).Times(1).Return(errors.New("asd"))
				createStoreCommand := NewCreateStoreCommand(mockDatastore, logger)
				_, err := createStoreCommand.Execute(ctx, &openfgav1.CreateStoreRequest{Name: "auth0"})
				if err != nil {
					t.Fatal(err)
				}
			},
			request:  &openfgav1.DeleteStoreRequest{StoreId: existingStoreID},
			response: nil,
			err:      status.Error(codes.Code(openfgav1.InternalErrorCode_internal_error), "Error deleting store"),
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			test.setup()
			actualResponse, actualError := cmd.Execute(ctx, test.request)

			if test.err != nil {
				if actualError == nil {
					t.Errorf("Expected error '%s', but got none", test.err)
				}
				if test.err.Error() != actualError.Error() {
					t.Errorf("Expected error '%s', actual '%s'", test.err, actualError)
				}
			}
			if test.err == nil && actualError != nil {
				t.Errorf("Did not expect an error but got one: %v", actualError)
			}

			if actualResponse == nil && test.err == nil {
				t.Error("Expected non nil response, got nil")
			} else {
				if diff := cmp.Diff(actualResponse, test.response, ignoreUnexported, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("Unexpected response (-got +want):\n%s", diff)
				}
			}
		})
	}
}
