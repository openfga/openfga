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

func TestCreateStore(t *testing.T) {
	type createStoreTestSettings struct {
		_name    string
		request  *openfgav1pb.CreateStoreRequest
		response *openfgav1pb.CreateStoreResponse
		err      error
	}

	name := testutils.CreateRandomString(10)

	var tests = []createStoreTestSettings{
		{
			_name: "CreateStoreSucceeds",
			request: &openfgav1pb.CreateStoreRequest{
				Name: name,
			},
			response: &openfgav1pb.CreateStoreResponse{
				Name: name,
			},
		},
	}

	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgav1pb.CreateStoreResponse{})
	ignoreStoreFields := cmpopts.IgnoreFields(openfgav1pb.CreateStoreResponse{}, "CreatedAt", "UpdatedAt", "Id")

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	backends, err := testutils.BuildAllBackends(ctx, tracer, logger)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {

			actualResponse, actualError := NewCreateStoreCommand(backends.StoresBackend, logger).Execute(ctx, test.request)

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
