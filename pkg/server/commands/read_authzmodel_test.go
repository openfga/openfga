package commands

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadAuthorizationModelQuery(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()
	model := &openfgav1.AuthorizationModel{
		Id:            modelID,
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"reader": {
						Userset: &openfgav1.Userset_This{},
					},
				},
			},
		},
	}

	var tests = []struct {
		name             string
		inputRequest     *openfgav1.ReadAuthorizationModelRequest
		inputModel       *openfgav1.AuthorizationModel
		setMock          func(*mockstorage.MockOpenFGADatastore)
		expectedResponse *openfgav1.ReadAuthorizationModelResponse
		expectedError    error
	}{
		{
			name: "reads_a_model",
			inputRequest: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: storeID,
				Id:      modelID,
			},
			inputModel: model,
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			expectedResponse: &openfgav1.ReadAuthorizationModelResponse{
				AuthorizationModel: model,
			},
		},
		{
			name: "returns_model_not_found_if_model_not_in_database",
			inputRequest: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: storeID,
				Id:      modelID,
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(nil, storage.ErrNotFound)
			},
			expectedError: serverErrors.AuthorizationModelNotFound(modelID),
		},
		{
			name: "returns_error_from_database",
			inputRequest: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: storeID,
				Id:      modelID,
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(nil, errors.New("some error"))
			},
			expectedError: serverErrors.NewInternalError("", errors.New("some error")),
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockController := gomock.NewController(t)
			defer mockController.Finish()

			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			test.setMock(mockDatastore)

			resp, err := NewReadAuthorizationModelQuery(mockDatastore).Execute(ctx, &openfgav1.ReadAuthorizationModelRequest{
				StoreId: storeID,
				Id:      modelID,
			})
			if test.expectedError != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, test.expectedError.Error())
				return
			}
			require.NoError(t, err)

			cmpOpts := []cmp.Option{
				cmpopts.IgnoreUnexported(openfgav1.ReadAuthorizationModelResponse{}),
				protocmp.Transform(),
			}
			if diff := cmp.Diff(test.expectedResponse, resp, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
