package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestReadAuthorizationModelsWithoutPaging(t *testing.T, datastore storage.OpenFGADatastore) {
	store := testutils.CreateRandomString(20)

	require := require.New(t)
	logger := logger.NewNoopLogger()
	encoder := encoder.NewBase64Encoder()
	ctx := context.Background()

	tests := []struct {
		name                      string
		backendState              map[string][]*openfgapb.TypeDefinition
		request                   *openfgapb.ReadAuthorizationModelsRequest
		expectedNumModelsReturned int
	}{
		{
			name: "empty",
			request: &openfgapb.ReadAuthorizationModelsRequest{
				StoreId: store,
			},
			expectedNumModelsReturned: 0,
		},
		{
			name: "empty for requested store",
			backendState: map[string][]*openfgapb.TypeDefinition{
				"another-store": {},
			},
			request: &openfgapb.ReadAuthorizationModelsRequest{
				StoreId: store,
			},
			expectedNumModelsReturned: 0,
		},
		{
			name: "multiple type definitions",
			backendState: map[string][]*openfgapb.TypeDefinition{
				store: {
					{
						Type: "ns1",
					},
				},
			},
			request: &openfgapb.ReadAuthorizationModelsRequest{
				StoreId: store,
			},
			expectedNumModelsReturned: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.backendState != nil {
				for store, state := range test.backendState {
					err := datastore.WriteAuthorizationModel(ctx, store, id.Must(id.New()).String(), state)
					require.NoError(err)
				}
			}

			query := commands.NewReadAuthorizationModelsQuery(datastore, logger, encoder)
			resp, err := query.Execute(ctx, test.request)

			require.NoError(err)
			require.Equal(test.expectedNumModelsReturned, len(resp.GetAuthorizationModels()))
			require.Empty(resp.ContinuationToken, "expected an empty continuation token")
		})
	}
}

func TestReadAuthorizationModelsWithPaging(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	store := id.Must(id.New()).String()
	tds := []*openfgapb.TypeDefinition{{Type: "repo"}}

	modelID1 := id.Must(id.New()).String()
	err := datastore.WriteAuthorizationModel(ctx, store, modelID1, tds)
	require.NoError(err)

	modelID2 := id.Must(id.New()).String()
	err = datastore.WriteAuthorizationModel(ctx, store, modelID2, tds)
	require.NoError(err)

	encrypter, err := encrypter.NewGCMEncrypter("key")
	require.NoError(err)

	encoder := encoder.NewTokenEncoder(encrypter, encoder.NewBase64Encoder())

	query := commands.NewReadAuthorizationModelsQuery(datastore, logger, encoder)
	firstRequest := &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:  store,
		PageSize: wrapperspb.Int32(1),
	}
	firstResponse, err := query.Execute(ctx, firstRequest)
	require.NoError(err)
	require.Len(firstResponse.AuthorizationModels, 1)
	require.Equal(firstResponse.AuthorizationModels[0].Id, modelID2)
	require.NotEmpty(firstResponse.ContinuationToken, "Expected continuation token")

	secondRequest := &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: firstResponse.ContinuationToken,
	}
	secondResponse, err := query.Execute(ctx, secondRequest)
	require.NoError(err)
	require.Len(secondResponse.AuthorizationModels, 1)
	require.Equal(secondResponse.AuthorizationModels[0].Id, modelID1)
	require.Empty(secondResponse.ContinuationToken, "Expected empty continuation token")

	thirdRequest := &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           store,
		ContinuationToken: "bad",
	}
	_, err = query.Execute(ctx, thirdRequest)
	require.Error(err)
	require.ErrorContains(err, "Invalid continuation token")

	validToken := "eyJwayI6IkxBVEVTVF9OU0NPTkZJR19hdXRoMHN0b3JlIiwic2siOiIxem1qbXF3MWZLZExTcUoyN01MdTdqTjh0cWgifQ=="
	invalidStoreRequest := &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           "non-existent",
		ContinuationToken: validToken,
	}
	_, err = query.Execute(ctx, invalidStoreRequest)
	require.Error(err)
	require.ErrorContains(err, "Invalid continuation token")
}

func TestReadAuthorizationModelsInvalidContinuationToken(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	store := id.Must(id.New()).String()
	modelID := id.Must(id.New()).String()
	err := datastore.WriteAuthorizationModel(ctx, store, modelID, []*openfgapb.TypeDefinition{{Type: "repo"}})
	require.NoError(err)

	_, err = commands.NewReadAuthorizationModelsQuery(datastore, logger, encoder.NewBase64Encoder()).Execute(ctx, &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           store,
		ContinuationToken: "foo",
	})
	require.ErrorIs(err, serverErrors.InvalidContinuationToken)
}
