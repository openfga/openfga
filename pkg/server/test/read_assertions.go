package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestReadAssertionQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	type readAssertionsQueryTest struct {
		_name            string
		request          *openfgapb.ReadAssertionsRequest
		expectedResponse *openfgapb.ReadAssertionsResponse
		expectedError    error
	}

	var tests = []readAssertionsQueryTest{
		{
			_name:   "ReturnsAssertionModelNotFound",
			request: &openfgapb.ReadAssertionsRequest{StoreId: "store", AuthorizationModelId: "test"},
			expectedResponse: &openfgapb.ReadAssertionsResponse{
				AuthorizationModelId: "test",
				Assertions:           []*openfgapb.Assertion{},
			},
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(10)

			query := commands.NewReadAssertionsQuery(datastore, logger)
			test.request.StoreId = store
			actualResponse, actualError := query.Execute(ctx, test.request.StoreId, test.request.AuthorizationModelId)

			if test.expectedError != nil {
				require.ErrorIs(t, actualError, test.expectedError)
			} else {
				require.NoError(t, actualError)
				require.Equal(t, test.expectedResponse, actualResponse)
			}
		})
	}
}
