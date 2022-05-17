package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/server/commands"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestDeleteStore(t *testing.T, dbTester teststorage.DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	createStoreCmd := commands.NewCreateStoreCommand(datastore, logger)
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
		},
		{
			_name: "Execute Succeeds",
			request: &openfgav1pb.DeleteStoreRequest{
				StoreId: createStoreResponse.Id,
			},
		},
	}

	deleteCmd := commands.NewDeleteStoreCommand(datastore, logger)

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
