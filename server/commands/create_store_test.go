package commands

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const createStoreName = "openfgastore"

func TestCreateStore(t *testing.T) {
	type createStoreTestSettings struct {
		_name    string
		request  *openfgav1pb.CreateStoreRequest
		response *openfgav1pb.CreateStoreResponse
		err      error
	}

	var tests = []createStoreTestSettings{
		{
			_name: "CreateStoreSucceeds",
			request: &openfgav1pb.CreateStoreRequest{
				Name: createStoreName,
			},
			response: &openfgav1pb.CreateStoreResponse{
				Name: createStoreName,
			},
			err: nil,
		},
	}

	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgav1pb.CreateStoreResponse{})
	ignoreStoreFields := cmpopts.IgnoreFields(openfgav1pb.CreateStoreResponse{}, "CreatedAt", "UpdatedAt", "Id")

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			tracer := telemetry.NewNoopTracer()
			storage, err := testutils.BuildAllBackends(tracer)
			if err != nil {
				t.Fatalf("Error building backend: %s", err)
			}
			ctx := context.Background()

			logger := logger.NewNoopLogger()

			actualResponse, actualError := NewCreateStoreCommand(storage.StoresBackend, logger).Execute(ctx, test.request)

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

			if actualResponse == nil {
				t.Error("Expected non nil response, got nil")
			} else {
				if diff := cmp.Diff(actualResponse, test.response, ignoreStateOpts, ignoreStoreFields, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("[%s] store mismatch (-got +want):\n%s", test._name, diff)
				}
			}
		})
	}
}
