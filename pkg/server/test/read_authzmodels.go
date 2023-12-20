package test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadAuthorizationModelsWithoutPaging(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	store := ulid.Make().String()

	tests := []struct {
		name                      string
		model                     *openfgav1.AuthorizationModel
		expectedNumModelsReturned int
	}{
		{
			name:                      "empty",
			expectedNumModelsReturned: 0,
		},
		{
			name: "non-empty_type_definitions",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
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

			query := commands.NewReadAuthorizationModelsQuery(datastore)
			resp, err := query.Execute(ctx, &openfgav1.ReadAuthorizationModelsRequest{StoreId: store})
			require.NoError(err)

			require.Len(resp.GetAuthorizationModels(), test.expectedNumModelsReturned)
			require.Empty(resp.ContinuationToken, "expected an empty continuation token")
		})
	}
}

func TestReadAuthorizationModelsWithPaging(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	store := ulid.Make().String()

	model1 := &openfgav1.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "repo",
			},
		},
	}
	err := datastore.WriteAuthorizationModel(ctx, store, model1)
	require.NoError(err)

	model2 := &openfgav1.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgav1.TypeDefinition{
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

	query := commands.NewReadAuthorizationModelsQuery(datastore,
		commands.WithReadAuthModelsQueryEncoder(encoder),
	)

	firstRequest := &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:  store,
		PageSize: wrapperspb.Int32(1),
	}
	firstResponse, err := query.Execute(ctx, firstRequest)
	require.NoError(err)
	require.Len(firstResponse.AuthorizationModels, 1)
	require.Equal(firstResponse.AuthorizationModels[0].Id, model2.Id)
	require.NotEmpty(firstResponse.ContinuationToken, "Expected continuation token")

	secondRequest := &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: firstResponse.ContinuationToken,
	}
	secondResponse, err := query.Execute(ctx, secondRequest)
	require.NoError(err)
	require.Len(secondResponse.AuthorizationModels, 1)
	require.Equal(secondResponse.AuthorizationModels[0].Id, model1.Id)
	require.Empty(secondResponse.ContinuationToken, "Expected empty continuation token")

	thirdRequest := &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           store,
		ContinuationToken: "bad",
	}
	_, err = query.Execute(ctx, thirdRequest)
	require.Error(err)
	require.ErrorContains(err, "Invalid continuation token")

	validToken := "eyJwayI6IkxBVEVTVF9OU0NPTkZJR19hdXRoMHN0b3JlIiwic2siOiIxem1qbXF3MWZLZExTcUoyN01MdTdqTjh0cWgifQ=="
	invalidStoreRequest := &openfgav1.ReadAuthorizationModelsRequest{
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
	store := ulid.Make().String()

	model := &openfgav1.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "repo"}},
	}
	err := datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(err)

	_, err = commands.NewReadAuthorizationModelsQuery(datastore).Execute(ctx,
		&openfgav1.ReadAuthorizationModelsRequest{
			StoreId:           store,
			ContinuationToken: "foo",
		})
	require.ErrorIs(err, serverErrors.InvalidContinuationToken)
}
