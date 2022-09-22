package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestSuccessfulReadAuthorizationModelQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	var tests = []struct {
		name            string
		storeID         string
		modelID         string
		schemaVersion   typesystem.SchemaVersion
		typeDefinitions []*openfgapb.TypeDefinition
	}{
		{
			name:          "write and read a 1.0 model",
			storeID:       id.Must(id.New()).String(),
			modelID:       id.Must(id.New()).String(),
			schemaVersion: typesystem.SchemaVersion1_0,
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"reader": {
							Userset: &openfgapb.Userset_This{},
						},
					},
				},
			},
		},
		{
			name:          "write and read an 1.1 model",
			storeID:       id.Must(id.New()).String(),
			modelID:       id.Must(id.New()).String(),
			schemaVersion: typesystem.SchemaVersion1_1,
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"reader": {
							Userset: &openfgapb.Userset_This{},
						},
					},
					Metadata: &openfgapb.Metadata{
						Relations: map[string]*openfgapb.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := datastore.WriteAuthorizationModel(ctx, test.storeID, test.modelID, test.schemaVersion, test.typeDefinitions)
			require.NoError(t, err)

			resp, err := commands.NewReadAuthorizationModelQuery(datastore, logger).Execute(ctx, &openfgapb.ReadAuthorizationModelRequest{
				StoreId: test.storeID,
				Id:      test.modelID,
			})
			require.NoError(t, err)
			require.Equal(t, test.modelID, resp.GetAuthorizationModel().GetId())
			require.Equal(t, test.schemaVersion.String(), resp.GetAuthorizationModel().GetSchemaVersion())
		})
	}
}

func TestReadAuthorizationModelQueryErrors(t *testing.T, datastore storage.OpenFGADatastore) {
	type readAuthorizationModelQueryTest struct {
		_name         string
		request       *openfgapb.ReadAuthorizationModelRequest
		expectedError error
	}

	var tests = []readAuthorizationModelQueryTest{
		{
			_name: "ReturnsAuthorizationModelNotFoundIfAuthorizationModelNotInDatabase",
			request: &openfgapb.ReadAuthorizationModelRequest{
				StoreId: id.Must(id.New()).String(),
				Id:      "123",
			},
			expectedError: serverErrors.AuthorizationModelNotFound("123"),
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			_, err := commands.NewReadAuthorizationModelQuery(datastore, logger).Execute(ctx, test.request)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}

func TestReadAuthorizationModelByIDAndTypeDefinitionsReturnsError(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	store := id.Must(id.New()).String()
	modelID := id.Must(id.New()).String()

	err := datastore.WriteAuthorizationModel(ctx, store, modelID, typesystem.SchemaVersion1_0, []*openfgapb.TypeDefinition{})
	require.NoError(err)

	query := commands.NewReadAuthorizationModelQuery(datastore, logger)
	_, err = query.Execute(ctx, &openfgapb.ReadAuthorizationModelRequest{
		StoreId: store,
		Id:      modelID,
	})
	require.ErrorContains(err, serverErrors.AuthorizationModelNotFound(modelID).Error())
}
