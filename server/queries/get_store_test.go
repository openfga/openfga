package queries

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type getStoreQueryTest struct {
	_name            string
	request          *openfgav1pb.GetStoreRequest
	expectedResponse *openfgav1pb.GetStoreResponse
	err              error
}

var getStoreTests = []getStoreQueryTest{
	{
		_name:            "ReturnsNotFound",
		request:          &openfgav1pb.GetStoreRequest{StoreId: "non-existent store"},
		expectedResponse: nil,
		err:              serverErrors.StoreIDNotFound,
	},
}

func TestGetStoreQuery(t *testing.T) {
	tracer := telemetry.NewNoopTracer()
	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgav1pb.GetStoreResponse{})
	ignoreStoreFields := cmpopts.IgnoreFields(openfgav1pb.GetStoreResponse{}, "CreatedAt", "UpdatedAt", "Id")

	for _, test := range getStoreTests {
		backend, err := testutils.BuildAllBackends(tracer)
		if err != nil {
			t.Fatalf("[%s] Error building backend: %s", test._name, err)
		}
		ctx := context.Background()

		query := NewGetStoreQuery(backend.StoresBackend, logger.NewNoopLogger())
		actualResponse, actualError := query.Execute(ctx, test.request)

		if test.err != nil {
			if actualError == nil {
				t.Errorf("[%s] Expected error '%s', but got none", test._name, test.err)
			}
			if test.err.Error() != actualError.Error() {
				t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.err, actualError)
			}
		}
		if test.err == nil && actualError != nil {
			t.Errorf("[%s] Did not expect an error but got one: %v", test._name, actualError)
		}

		if actualResponse == nil && test.err == nil {
			t.Errorf("[%s] Expected non nil response, got nil", test._name)
		} else {
			if diff := cmp.Diff(actualResponse, test.expectedResponse, ignoreStateOpts, ignoreStoreFields, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("[%s] store mismatch (-got +want):\n%s", test._name, diff)
			}
		}

	}
}

func TestGetStoreSucceeds(t *testing.T) {
	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgav1pb.GetStoreResponse{})
	ignoreStoreFields := cmpopts.IgnoreFields(openfgav1pb.GetStoreResponse{}, "CreatedAt", "UpdatedAt", "Id")
	tracer := telemetry.NewNoopTracer()
	ctx := context.Background()
	backend, err := testutils.BuildAllBackends(tracer)
	if err != nil {
		t.Fatalf("Error building backend: %s", err)
	}
	createStoreQuery := commands.NewCreateStoreCommand(backend.StoresBackend, logger.NewNoopLogger())
	createStoreResponse, err := createStoreQuery.Execute(ctx, &openfgav1pb.CreateStoreRequest{Name: readTestStore})
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	query := NewGetStoreQuery(backend.StoresBackend, logger.NewNoopLogger())
	actualResponse, actualError := query.Execute(ctx, &openfgav1pb.GetStoreRequest{StoreId: createStoreResponse.Id})

	if actualError != nil {
		t.Errorf("Expected no error, but got %v", actualError)
	}

	expectedResponse := &openfgav1pb.GetStoreResponse{
		Id:   createStoreResponse.Id,
		Name: readTestStore,
	}

	if diff := cmp.Diff(actualResponse, expectedResponse, ignoreStateOpts, ignoreStoreFields, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("unexpected result (-got +want):\n%s", diff)
	}

}
