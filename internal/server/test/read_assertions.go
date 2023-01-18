package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/internal/server/commands"
	"github.com/openfga/openfga/internal/storage"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
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
