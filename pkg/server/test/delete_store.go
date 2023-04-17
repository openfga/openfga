package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestDeleteStore(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	createStoreCmd := commands.NewCreateStoreCommand(datastore, logger)
	createStoreResponse, err := createStoreCmd.Execute(ctx, &openfgapb.CreateStoreRequest{
		Name: "acme",
	})
	require.NoError(t, err)

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

	deleteCmd := commands.NewDeleteStoreCommand(datastore, logger)

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			_, err := deleteCmd.Execute(ctx, test.request)

			if test.err != nil {
				require.ErrorIs(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
