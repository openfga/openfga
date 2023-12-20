package test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

func TestCreateStore(t *testing.T, datastore storage.OpenFGADatastore) {
	type createStoreTestSettings struct {
		name    string
		request *openfgav1.CreateStoreRequest
	}

	var tests = []createStoreTestSettings{
		{
			name: "CreateStoreSucceeds",
			request: &openfgav1.CreateStoreRequest{
				Name: testutils.CreateRandomString(10),
			},
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resp, err := commands.NewCreateStoreCommand(datastore).Execute(ctx, test.request)
			require.NoError(t, err)

			require.Equal(t, test.request.Name, resp.Name)

			_, err = ulid.Parse(resp.Id)
			require.NoError(t, err)

			require.NotEmpty(t, resp.CreatedAt)
			require.NotEmpty(t, resp.UpdatedAt)
			require.Equal(t, resp.CreatedAt, resp.UpdatedAt)
		})
	}
}
