package test

import (
	"context"
	"errors"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/queries"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestReadAuthorizationModelQueryErrors(t *testing.T, dbTester teststorage.DatastoreTester) {
	type readAuthorizationModelQueryTest struct {
		_name         string
		request       *openfgapb.ReadAuthorizationModelRequest
		expectedError error
	}

	var tests = []readAuthorizationModelQueryTest{
		{
			_name: "ReturnsAuthorizationModelNotFoundIfAuthorizationModelNotInDatabase",
			request: &openfgapb.ReadAuthorizationModelRequest{
				StoreId: testutils.CreateRandomString(10),
				Id:      "123",
			},
			expectedError: serverErrors.AuthorizationModelNotFound("123"),
		},
	}

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	for _, test := range tests {
		query := queries.NewReadAuthorizationModelQuery(datastore, logger)
		if _, err := query.Execute(ctx, test.request); !errors.Is(test.expectedError, err) {
			t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.expectedError, err)
			continue
		}
	}
}

func TestReadAuthorizationModelByIDAndOneTypeDefinitionReturnsAuthorizationModel(t *testing.T, dbTester teststorage.DatastoreTester) {

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	state := &openfgapb.TypeDefinitions{
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"viewer": {
						Userset: &openfgapb.Userset_This{},
					},
				},
			},
		},
	}

	store := testutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	if err := datastore.WriteAuthorizationModel(ctx, store, modelID, state); err != nil {
		t.Fatalf("WriteAuthorizationModel err = %v, want nil", err)
	}
	query := queries.NewReadAuthorizationModelQuery(datastore, logger)
	actualResponse, actualError := query.Execute(ctx, &openfgapb.ReadAuthorizationModelRequest{
		StoreId: store,
		Id:      modelID,
	})

	if actualResponse == nil {
		t.Fatal("ReadAuthorizationModelRequest response was nil")
	}
	if actualResponse.AuthorizationModel.Id != modelID {
		t.Fatalf("ReadAuthorizationModelRequest id = %v, want %v", actualResponse.AuthorizationModel.Id, modelID)
	}

	if actualError != nil {
		t.Fatalf("ReadAuthorizationModelRequest err = %v, want nil", err)
	}
}

func TestReadAuthorizationModelByIDAndTypeDefinitionsReturnsError(t *testing.T, dbTester teststorage.DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	emptyState := &openfgapb.TypeDefinitions{
		TypeDefinitions: []*openfgapb.TypeDefinition{},
	}

	store := testutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}

	if err := datastore.WriteAuthorizationModel(ctx, store, modelID, emptyState); err != nil {
		t.Fatalf("WriteAuthorizationModel err = %v, want nil", err)
	}

	query := queries.NewReadAuthorizationModelQuery(datastore, logger)
	_, err = query.Execute(ctx, &openfgapb.ReadAuthorizationModelRequest{
		StoreId: store,
		Id:      modelID,
	})
	if err.Error() != serverErrors.AuthorizationModelNotFound(modelID).Error() {
		t.Fatalf("got '%v', wanted '%v'", err, serverErrors.AuthorizationModelNotFound(modelID))
	}
}
