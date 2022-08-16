package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/server/commands"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestDeleteStore(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	createStoreCmd := commands.NewCreateStoreCommand(datastore, logger)
	createStoreResponse, err := createStoreCmd.Execute(ctx, &openfgapb.CreateStoreRequest{
		Name: "acme",
	})
	if err != nil {
		t.Fatalf("Failed to execute createStoreCmd: %s", err)
	}

	type deleteStoreTest struct {
		_name   string
		request *openfgapb.DeleteStoreRequest
		err     error
	}
	var tests = []deleteStoreTest{
		{
			_name: "Execute Delete Store With Non Existent Store Succeeds",
			request: &openfgapb.DeleteStoreRequest{
				StoreId: "unknownstore",
			},
		},
		{
			_name: "Execute Succeeds",
			request: &openfgapb.DeleteStoreRequest{
				StoreId: createStoreResponse.Id,
			},
		},
	}

	deleteCmd := commands.NewDeleteStoreCommand(datastore, logger)

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			_, err := deleteCmd.Execute(ctx, test.request)

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
