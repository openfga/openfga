package commands

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestDeleteStore(t *testing.T) {
	tracer := telemetry.NewNoopTracer()
	storage, err := testutils.BuildAllBackends(tracer)
	if err != nil {
		t.Fatalf("Error building backend: %s", err)
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	createStoreCmd := NewCreateStoreCommand(storage.StoresBackend, logger)
	createStoreResponse, err := createStoreCmd.Execute(ctx, &openfgav1pb.CreateStoreRequest{
		Name: "acme",
	})
	if err != nil {
		t.Fatalf("Failed to execute createStoreCmd: %s", err)
	}

	type deleteStoreTest struct {
		_name   string
		request *openfgav1pb.DeleteStoreRequest
		err     error
	}
	var tests = []deleteStoreTest{
		{
			_name: "Execute Delete Store With Non Existent Store Succeeds",
			request: &openfgav1pb.DeleteStoreRequest{
				StoreId: "unknownstore",
			},
			err: nil,
		},
		{
			_name: "Execute Succeeds",
			request: &openfgav1pb.DeleteStoreRequest{
				StoreId: createStoreResponse.Id,
			},
			err: nil,
		},
	}

	deleteCmd := NewDeleteStoreCommand(storage.StoresBackend, logger)

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			err := deleteCmd.Execute(ctx, test.request)

			if test.err != nil {
				if err == nil {
					t.Errorf("[%s] Expected error '%s', but got none", test._name, test.err)
				}
				if test.err.Error() != err.Error() {
					t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.err, err)
				}
			}

			if err != nil {
				t.Errorf("[%s] Expected no error but got '%v'", test._name, err)
			}
		})
	}
}
