package queries

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestReadAssertionQuery(t *testing.T) {
	type readAssertionsQueryTest struct {
		_name            string
		request          *openfgav1pb.ReadAssertionsRequest
		expectedResponse *openfgav1pb.ReadAssertionsResponse
		expectedError    error
	}

	var tests = []readAssertionsQueryTest{
		{
			_name:   "ReturnsAssertionModelNotFound",
			request: &openfgav1pb.ReadAssertionsRequest{StoreId: "store", AuthorizationModelId: "test"},
			expectedResponse: &openfgav1pb.ReadAssertionsResponse{
				AuthorizationModelId: "test",
				Assertions:           []*openfga.Assertion{},
			},
		},
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	backends, err := testutils.BuildAllBackends(ctx, tracer, logger)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(10)

			query := NewReadAssertionsQuery(backends.AssertionsBackend, logger)
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
