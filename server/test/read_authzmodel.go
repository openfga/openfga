package test

import (
	"context"
	"errors"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

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
		query := commands.NewReadAuthorizationModelQuery(datastore, logger)
		if _, err := query.Execute(ctx, test.request); !errors.Is(test.expectedError, err) {
			t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.expectedError, err)
			continue
		}
	}
}

func TestReadAuthorizationModelByIDAndOneTypeDefinitionReturnsAuthorizationModel(t *testing.T, datastore storage.OpenFGADatastore) {

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	state := []*openfgapb.TypeDefinition{
		{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"viewer": {
					Userset: &openfgapb.Userset_This{},
				},
			},
		},
	}

	store := id.Must(id.New()).String()
	modelID := id.Must(id.New()).String()

	err := datastore.WriteAuthorizationModel(ctx, store, modelID, state)
	require.NoError(err)

	query := commands.NewReadAuthorizationModelQuery(datastore, logger)
	actualResponse, actualError := query.Execute(ctx, &openfgapb.ReadAuthorizationModelRequest{
		StoreId: store,
		Id:      modelID,
	})

	require.NotNil(actualResponse)
	require.Equal(actualResponse.AuthorizationModel.Id, modelID)
	require.Nil(actualError)
}

func TestReadAuthorizationModelByIDAndTypeDefinitionsReturnsError(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	store := id.Must(id.New()).String()
	modelID := id.Must(id.New()).String()

	err := datastore.WriteAuthorizationModel(ctx, store, modelID, []*openfgapb.TypeDefinition{})
	require.NoError(err)

	query := commands.NewReadAuthorizationModelQuery(datastore, logger)
	_, err = query.Execute(ctx, &openfgapb.ReadAuthorizationModelRequest{
		StoreId: store,
		Id:      modelID,
	})
	require.ErrorContains(err, serverErrors.AuthorizationModelNotFound(modelID).Error())
}
