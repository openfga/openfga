package test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

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

			require.Equal(t, test.request.GetName(), resp.GetName())

			_, err = ulid.Parse(resp.GetId())
			require.NoError(t, err)

			require.NotEmpty(t, resp.GetCreatedAt())
			require.NotEmpty(t, resp.GetUpdatedAt())
			require.Equal(t, resp.GetCreatedAt(), resp.GetUpdatedAt())
		})
	}
}
