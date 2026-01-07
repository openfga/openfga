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
	"github.com/openfga/openfga/pkg/tuple"
)

func TestReadAssertionQuery(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	assertions := []*openfgav1.Assertion{{
		TupleKey:    tuple.NewAssertionTupleKey("repo:openfga", "reader", "user:anne"),
		Expectation: false,
	}}

	var tests = []struct {
		name             string
		inputRequest     *openfgav1.ReadAssertionsRequest
		setMock          func(*mockstorage.MockOpenFGADatastore)
		expectedResponse *openfgav1.ReadAssertionsResponse
		expectedError    error
	}{
		{
			name: "returns_assertions",
			inputRequest: &openfgav1.ReadAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAssertions(gomock.Any(), storeID, modelID).Return(assertions, nil)
			},
			expectedResponse: &openfgav1.ReadAssertionsResponse{
				AuthorizationModelId: modelID,
				Assertions:           assertions,
			},
		},
		{
			name: "returns_error_from_database",
			inputRequest: &openfgav1.ReadAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAssertions(gomock.Any(), storeID, modelID).Return(nil, errors.New("internal"))
			},
			expectedError: serverErrors.NewInternalError("", errors.New("some error")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockController := gomock.NewController(t)
			defer mockController.Finish()

			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			test.setMock(mockDatastore)
			resp, err := NewReadAssertionsQuery(mockDatastore).Execute(context.Background(), test.inputRequest.GetStoreId(), test.inputRequest.GetAuthorizationModelId())
			if test.expectedError != nil {
				require.Nil(t, resp)
				require.Error(t, err)
				require.ErrorContains(t, err, test.expectedError.Error())
				return
			}
			require.NoError(t, err)

			cmpOpts := []cmp.Option{
				cmpopts.IgnoreUnexported(openfgav1.ReadAssertionsResponse{}),
				protocmp.Transform(),
			}
			if diff := cmp.Diff(test.expectedResponse, resp, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
