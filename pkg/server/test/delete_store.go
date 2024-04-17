package test

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
)

func TestDeleteStore(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	createStoreCmd := commands.NewCreateStoreCommand(datastore)
	createStoreResponse, err := createStoreCmd.Execute(ctx, &openfgav1.CreateStoreRequest{
		Name: "acme",
	})
	require.NoError(t, err)

	type deleteStoreTest struct {
		_name   string
		request *openfgav1.DeleteStoreRequest
		err     error
	}
	var tests = []deleteStoreTest{
		{
			_name: "Execute_Delete_Store_With_Non_Existent_Store_Succeeds",
			request: &openfgav1.DeleteStoreRequest{
				StoreId: "unknownstore",
			},
		},
		{
			_name: "Execute_Succeeds",
			request: &openfgav1.DeleteStoreRequest{
				StoreId: createStoreResponse.GetId(),
			},
		},
	}

	deleteCmd := commands.NewDeleteStoreCommand(datastore)

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
