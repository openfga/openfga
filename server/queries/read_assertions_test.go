package queries

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
)

type readAssertionsQueryTest struct {
	_name            string
	request          *openfgav1pb.ReadAssertionsRequest
	expectedResponse *openfgav1pb.ReadAssertionsResponse
	expectedError    error
}

var readAssertionsQueriesTests = []readAssertionsQueryTest{
	{
		_name:   "ReturnsAssertionModelNotFound",
		request: &openfgav1pb.ReadAssertionsRequest{StoreId: readTestStore, AuthorizationModelId: "test"},
		expectedResponse: &openfgav1pb.ReadAssertionsResponse{
			AuthorizationModelId: "test",
			Assertions:           []*openfga.Assertion{},
		},
		expectedError: nil,
	},
}

func TestReadAssertionQuery(t *testing.T) {
	tracer := otel.Tracer("noop")
	for _, test := range readAssertionsQueriesTests {
		backend, err := testutils.BuildAllBackends(tracer)
		if err != nil {
			t.Fatalf("Error building backend: %s", err)
		}
		ctx := context.Background()
		query := NewReadAssertionsQuery(backend.AssertionsBackend, logger.NewNoopLogger())
		actualResponse, actualError := query.Execute(ctx, test.request.StoreId, test.request.AuthorizationModelId)

		if test.expectedError != nil && test.expectedError.Error() != actualError.Error() {
			t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.expectedError, actualError)
			continue
		}

		if test.expectedResponse != nil {
			if actualError != nil {
				t.Errorf("[%s] Expected no error but got '%s'", test._name, actualError)
			}

			if actualResponse == nil {
				t.Error("Expected non nil response, got nil")
			}
		}

	}
}
