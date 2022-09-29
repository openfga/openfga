package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestReadAuthorizationModelsWithoutPaging(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	logger := logger.NewNoopLogger()
	encoder := encoder.NewBase64Encoder()
	ctx := context.Background()
	store := id.Must(id.New()).String()

	tests := []struct {
		name                      string
		model                     *openfgapb.AuthorizationModel
		expectedNumModelsReturned int
	}{
		{
			name:                      "empty",
			expectedNumModelsReturned: 0,
		},
		{
			name: "non-empty type definitions",
			model: &openfgapb.AuthorizationModel{
				Id:            id.Must(id.New()).String(),
				SchemaVersion: typesystem.SchemaVersion10,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
					},
				},
			},
			expectedNumModelsReturned: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.model != nil {
				err := datastore.WriteAuthorizationModel(ctx, store, test.model)
				require.NoError(err)
			}

			query := commands.NewReadAuthorizationModelsQuery(datastore, logger, encoder)
			resp, err := query.Execute(ctx, &openfgapb.ReadAuthorizationModelsRequest{StoreId: store})
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

	model1 := &openfgapb.AuthorizationModel{
		Id:            id.Must(id.New()).String(),
		SchemaVersion: typesystem.SchemaVersion10,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
			},
		},
	}
	err := datastore.WriteAuthorizationModel(ctx, store, model1)
	require.NoError(err)

	model2 := &openfgapb.AuthorizationModel{
		Id:            id.Must(id.New()).String(),
		SchemaVersion: typesystem.SchemaVersion10,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
			},
		},
	}
	err = datastore.WriteAuthorizationModel(ctx, store, model2)
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
	require.Equal(firstResponse.AuthorizationModels[0].Id, model2.Id)
	require.NotEmpty(firstResponse.ContinuationToken, "Expected continuation token")

	secondRequest := &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: firstResponse.ContinuationToken,
	}
	secondResponse, err := query.Execute(ctx, secondRequest)
	require.NoError(err)
	require.Len(secondResponse.AuthorizationModels, 1)
	require.Equal(secondResponse.AuthorizationModels[0].Id, model1.Id)
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

	model := &openfgapb.AuthorizationModel{
		Id:              id.Must(id.New()).String(),
		SchemaVersion:   typesystem.SchemaVersion10,
		TypeDefinitions: []*openfgapb.TypeDefinition{{Type: "repo"}},
	}
	err := datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(err)

	_, err = commands.NewReadAuthorizationModelsQuery(datastore, logger, encoder.NewBase64Encoder()).Execute(ctx, &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           store,
		ContinuationToken: "foo",
	})
	require.ErrorIs(err, serverErrors.InvalidContinuationToken)
}
