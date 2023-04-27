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

func TestUpdateStore(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	createStoreCmd := commands.NewCreateStoreCommand(datastore, logger)
	createStoreResponse, err := createStoreCmd.Execute(ctx, &openfgapb.CreateStoreRequest{
		Name: "acme",
	})
	require.NoError(t, err)

	type updateStoreTest struct {
		_name   string
		request *openfgapb.UpdateStoreRequest
		err     error
	}
	var tests = []updateStoreTest{
		{
			_name: "UpdateStoreSucceeds",
			request: &openfgapb.UpdateStoreRequest{
				StoreId: createStoreResponse.Id,
				Name:    "newName",
			},
		},
	}

	updateCmd := commands.NewUpdateStoreCommand(datastore, logger)

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			resp, err := updateCmd.Execute(ctx, test.request)
			if test.err != nil {
				require.ErrorIs(t, err, test.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, createStoreResponse.Id, resp.Id)
				require.Equal(t, createStoreResponse.CreatedAt, resp.CreatedAt)
				require.Less(t, createStoreResponse.UpdatedAt, resp.UpdatedAt)
				require.NotEqual(t, createStoreResponse.Name, resp.Name)
				require.Equal(t, resp.Name, test.request.Name)
			}
		})
	}
}
