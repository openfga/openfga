package test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestSuccessfulReadAuthorizationModelQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	var tests = []struct {
		name    string
		storeID string
		model   *openfgav1.AuthorizationModel
	}{
		{
			name:    "write_and_read_a_1.0_model",
			storeID: ulid.Make().String(),
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_This{},
							},
						},
					},
				},
			},
		},
		{
			name:    "write_and_read_an_1.1_model",
			storeID: ulid.Make().String(),
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type document
  relations
	define reader: [user]`).TypeDefinitions,
			},
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := datastore.WriteAuthorizationModel(ctx, test.storeID, test.model)
			require.NoError(t, err)

			resp, err := commands.NewReadAuthorizationModelQuery(datastore).Execute(ctx, &openfgav1.ReadAuthorizationModelRequest{
				StoreId: test.storeID,
				Id:      test.model.Id,
			})
			require.NoError(t, err)
			require.Equal(t, test.model.Id, resp.GetAuthorizationModel().GetId())
			require.Equal(t, test.model.SchemaVersion, resp.GetAuthorizationModel().GetSchemaVersion())
		})
	}
}

func TestReadAuthorizationModelQueryErrors(t *testing.T, datastore storage.OpenFGADatastore) {
	type readAuthorizationModelQueryTest struct {
		_name         string
		request       *openfgav1.ReadAuthorizationModelRequest
		expectedError error
	}

	var tests = []readAuthorizationModelQueryTest{
		{
			_name: "ReturnsAuthorizationModelNotFoundIfAuthorizationModelNotInDatabase",
			request: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				Id:      "123",
			},
			expectedError: serverErrors.AuthorizationModelNotFound("123"),
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			_, err := commands.NewReadAuthorizationModelQuery(datastore).Execute(ctx, test.request)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}

func ReadAuthorizationModelTest(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	storeID := ulid.Make().String()

	t.Run("writing_without_any_type_definitions_does_not_write_anything", func(t *testing.T) {
		model := &openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{},
		}

		err := datastore.WriteAuthorizationModel(ctx, storeID, model)
		require.NoError(err)

		_, err = commands.NewReadAuthorizationModelQuery(datastore).Execute(ctx, &openfgav1.ReadAuthorizationModelRequest{
			StoreId: storeID,
			Id:      model.Id,
		})
		require.ErrorContains(err, serverErrors.AuthorizationModelNotFound(model.Id).Error())
	})
}
