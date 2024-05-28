package test

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/server/commands"
	"github.com/openfga/openfga/pkg/server"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

func TestReadAssertionQuery(t *testing.T, datastore storage.OpenFGADatastore, s *server.Server) {
	type readAssertionsQueryTest struct {
		_name            string
		request          *openfgav1.ReadAssertionsRequest
		expectedResponse *openfgav1.ReadAssertionsResponse
		expectedError    error
	}

	var tests = []readAssertionsQueryTest{
		{
			_name:   "ReturnsAssertionModelNotFound",
			request: &openfgav1.ReadAssertionsRequest{StoreId: "store", AuthorizationModelId: "test"},
			expectedResponse: &openfgav1.ReadAssertionsResponse{
				AuthorizationModelId: "test",
				Assertions:           []*openfgav1.Assertion{},
			},
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(10)

			query := commands.NewReadAssertionsQuery(datastore)
			test.request.StoreId = store
			actualResponse, actualError := query.Execute(ctx, test.request.GetStoreId(), test.request.GetAuthorizationModelId())

			if test.expectedError != nil {
				require.ErrorIs(t, actualError, test.expectedError)
			} else {
				require.NoError(t, actualError)
				require.Equal(t, test.expectedResponse, actualResponse)
			}
		})
	}
}
