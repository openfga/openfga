package test

import (
	"context"
	"testing"

	commands2 "github.com/openfga/openfga/internal/server/commands"
	"github.com/openfga/openfga/internal/storage"
	"github.com/openfga/openfga/pkg/logger"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestDeleteStore(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	createStoreCmd := commands2.NewCreateStoreCommand(datastore, logger)
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
			_name: "Execute_Delete_Store_With_Non_Existent_Store_Succeeds",
			request: &openfgapb.DeleteStoreRequest{
				StoreId: "unknownstore",
			},
		},
		{
			_name: "Execute_Succeeds",
			request: &openfgapb.DeleteStoreRequest{
				StoreId: createStoreResponse.Id,
			},
		},
	}

	deleteCmd := commands2.NewDeleteStoreCommand(datastore, logger)

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
