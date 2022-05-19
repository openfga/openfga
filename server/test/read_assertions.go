package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/queries"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestReadAssertionQuery(t *testing.T, dbTester teststorage.DatastoreTester) {
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

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(10)

			query := queries.NewReadAssertionsQuery(datastore, logger)
			test.request.StoreId = store
			actualResponse, actualError := query.Execute(ctx, test.request.StoreId, test.request.AuthorizationModelId)

			if test.expectedError != nil && test.expectedError.Error() != actualError.Error() {
				t.Fatalf("expected '%s', got '%s'", test.expectedError, actualError)
			}

			if test.expectedResponse != nil {
				if actualError != nil {
					t.Fatalf("expected nil, got '%s'", actualError)
				}

				if actualResponse == nil {
					t.Fatalf("expected '%s', got nil", test.expectedResponse)
				}
			}
		})
	}
}
