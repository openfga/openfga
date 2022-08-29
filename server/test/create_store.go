package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestCreateStore(t *testing.T, datastore storage.OpenFGADatastore) {
	type createStoreTestSettings struct {
		name    string
		request *openfgapb.CreateStoreRequest
	}

	var tests = []createStoreTestSettings{
		{
			name: "CreateStoreSucceeds",
			request: &openfgapb.CreateStoreRequest{
				Name: testutils.CreateRandomString(10),
			},
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			resp, err := commands.NewCreateStoreCommand(datastore, logger).Execute(ctx, test.request)
			require.NoError(t, err)

			require.Equal(t, test.request.Name, resp.Name)
			require.NotEmpty(t, resp.Id)
			require.NotEmpty(t, resp.CreatedAt)
			require.NotEmpty(t, resp.UpdatedAt)
			require.Equal(t, resp.CreatedAt, resp.UpdatedAt)
		})
	}
}
