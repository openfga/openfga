package test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadAuthorizationModelsWithoutPaging(t *testing.T, datastore storage.OpenFGADatastore) {
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
				require.NoError(t, err)
			}

			query := commands.NewReadAuthorizationModelsQuery(datastore)
			resp, err := query.Execute(ctx, &openfgav1.ReadAuthorizationModelsRequest{StoreId: store})
			require.NoError(t, err)

			require.Len(t, resp.GetAuthorizationModels(), test.expectedNumModelsReturned)
			require.Empty(t, resp.GetContinuationToken(), "expected an empty continuation token")
		})
	}
}

func TestReadAuthorizationModelsWithPaging(t *testing.T, datastore storage.OpenFGADatastore) {
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
	require.NoError(t, err)

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
	require.NoError(t, err)

	encrypter, err := encrypter.NewGCMEncrypter("key")
	require.NoError(t, err)

	encoder := encoder.NewTokenEncoder(encrypter, encoder.NewBase64Encoder())

	query := commands.NewReadAuthorizationModelsQuery(datastore,
		commands.WithReadAuthModelsQueryEncoder(encoder),
	)

	firstRequest := &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:  store,
		PageSize: wrapperspb.Int32(1),
	}
	firstResponse, err := query.Execute(ctx, firstRequest)
	require.NoError(t, err)
	require.Len(t, firstResponse.GetAuthorizationModels(), 1)
	require.Equal(t, firstResponse.GetAuthorizationModels()[0].GetId(), model2.GetId())
	require.NotEmpty(t, firstResponse.GetContinuationToken(), "Expected continuation token")

	secondRequest := &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: firstResponse.GetContinuationToken(),
	}
	secondResponse, err := query.Execute(ctx, secondRequest)
	require.NoError(t, err)
	require.Len(t, secondResponse.GetAuthorizationModels(), 1)
	require.Equal(t, secondResponse.GetAuthorizationModels()[0].GetId(), model1.GetId())
	require.Empty(t, secondResponse.GetContinuationToken(), "Expected empty continuation token")

	thirdRequest := &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           store,
		ContinuationToken: "bad",
	}
	_, err = query.Execute(ctx, thirdRequest)
	require.Error(t, err)
	require.ErrorContains(t, err, "Invalid continuation token")

	validToken := "eyJwayI6IkxBVEVTVF9OU0NPTkZJR19hdXRoMHN0b3JlIiwic2siOiIxem1qbXF3MWZLZExTcUoyN01MdTdqTjh0cWgifQ=="
	invalidStoreRequest := &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           "non-existent",
		ContinuationToken: validToken,
	}
	_, err = query.Execute(ctx, invalidStoreRequest)
	require.Error(t, err)
	require.ErrorContains(t, err, "Invalid continuation token")
}

func TestReadAuthorizationModelsInvalidContinuationToken(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	store := ulid.Make().String()

	model := &openfgav1.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "repo"}},
	}
	err := datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(t, err)

	_, err = commands.NewReadAuthorizationModelsQuery(datastore).Execute(ctx,
		&openfgav1.ReadAuthorizationModelsRequest{
			StoreId:           store,
			ContinuationToken: "foo",
		})
	require.ErrorIs(t, err, serverErrors.InvalidContinuationToken)
}
