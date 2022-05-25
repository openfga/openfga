package test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/queries"
	"github.com/openfga/openfga/storage"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestReadAuthorizationModelsWithoutPaging(t *testing.T, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {
	store := testutils.CreateRandomString(20)
	for _, tc := range []struct {
		name                string
		backendState        map[string]*openfgapb.TypeDefinitions
		request             *openfgapb.ReadAuthorizationModelsRequest
		expectedIdsReturned int
	}{
		{
			name: "empty",
			request: &openfgapb.ReadAuthorizationModelsRequest{
				StoreId: store,
			},
			expectedIdsReturned: 0,
		},
		{
			name: "empty for requested store",
			backendState: map[string]*openfgapb.TypeDefinitions{
				"another-store": {
					TypeDefinitions: []*openfgapb.TypeDefinition{},
				},
			},
			request: &openfgapb.ReadAuthorizationModelsRequest{
				StoreId: store,
			},
			expectedIdsReturned: 0,
		},
		{
			name: "multiple type definitions",
			backendState: map[string]*openfgapb.TypeDefinitions{
				store: {
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "ns1",
						},
					},
				},
			},
			request: &openfgapb.ReadAuthorizationModelsRequest{
				StoreId: store,
			},
			expectedIdsReturned: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()
			logger := logger.NewNoopLogger()

			datastore, err := dbTester.New()
			require.NoError(err)

			if tc.backendState != nil {
				for store, state := range tc.backendState {
					modelID, err := id.NewString()
					if err != nil {
						t.Fatal(err)
					}
					if err := datastore.WriteAuthorizationModel(ctx, store, modelID, state); err != nil {
						t.Fatalf("WriteAuthorizationModel(%s), err = %v, want nil", store, err)
					}
				}
			}

			encoder := encoder.NewNoopEncoder()

			query := queries.NewReadAuthorizationModelsQuery(datastore, encoder, logger)
			resp, err := query.Execute(ctx, tc.request)
			if err != nil {
				t.Fatalf("Query.Execute(), err = %v, want nil", err)
			}

			if tc.expectedIdsReturned != len(resp.GetAuthorizationModelIds()) {
				t.Errorf("expected %d, got %d", tc.expectedIdsReturned, len(resp.GetAuthorizationModelIds()))
			}

			if resp.ContinuationToken != "" {
				t.Error("Expected an empty continuation token")
			}
		})
	}
}

func TestReadAuthorizationModelsWithPaging(t *testing.T, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	backendState := &openfgapb.TypeDefinitions{
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "ns1",
			},
		},
	}

	store := testutils.CreateRandomString(10)
	modelID1, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	if err := datastore.WriteAuthorizationModel(ctx, store, modelID1, backendState); err != nil {
		t.Fatal(err)
	}
	modelID2, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	if err := datastore.WriteAuthorizationModel(ctx, store, modelID2, backendState); err != nil {
		t.Fatal(err)
	}

	encoder, err := encoder.NewTokenEncrypter("key")
	if err != nil {
		t.Fatalf("Error building encoder: %s", err)
	}

	query := queries.NewReadAuthorizationModelsQuery(datastore, encoder, logger)
	firstRequest := &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:  store,
		PageSize: wrapperspb.Int32(1),
	}
	firstResponse, err := query.Execute(ctx, firstRequest)
	if err != nil {
		t.Fatalf("Query.Execute(), err = %v, want nil", err)
	}
	if len(firstResponse.AuthorizationModelIds) != 1 {
		t.Fatal("Expected 1 modelID")
	}
	if firstResponse.AuthorizationModelIds[0] != modelID2 {
		t.Fatalf("Expected model id to be %v but was %v", modelID2, firstResponse.AuthorizationModelIds[0])
	}

	if firstResponse.ContinuationToken == "" {
		t.Fatal("Expected continuation token")
	}

	secondRequest := &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: firstResponse.ContinuationToken,
	}
	secondResponse, err := query.Execute(ctx, secondRequest)
	if err != nil {
		t.Fatalf("Query.Execute(), err = %v, want nil", err)
	}
	if len(secondResponse.AuthorizationModelIds) != 1 {
		t.Fatal("Expected 1 modelID")
	}
	if secondResponse.AuthorizationModelIds[0] != modelID1 {
		t.Fatalf("Expected model id to be %v but was %v", modelID1, firstResponse.AuthorizationModelIds[0])
	}
	// no token <=> no more results
	if secondResponse.ContinuationToken != "" {
		t.Fatal("Expected empty continuation token")
	}

	thirdRequest := &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           store,
		ContinuationToken: "bad",
	}
	_, err = query.Execute(ctx, thirdRequest)
	if err == nil {
		t.Fatal("Expected an error")
	}
	expectedError := "Invalid continuation token"
	if !strings.Contains(err.Error(), expectedError) {
		t.Fatalf("Expected error '%s', actual '%s'", expectedError, err.Error())
	}

	validToken := "eyJwayI6IkxBVEVTVF9OU0NPTkZJR19hdXRoMHN0b3JlIiwic2siOiIxem1qbXF3MWZLZExTcUoyN01MdTdqTjh0cWgifQ=="
	invalidStoreRequest := &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           "non-existent",
		ContinuationToken: validToken,
	}
	_, err = query.Execute(ctx, invalidStoreRequest)
	if err == nil {
		t.Fatal("Expected an error")
	}
	expectedError = "Invalid continuation token"
	if !strings.Contains(err.Error(), expectedError) {
		t.Fatalf("Expected error '%s', actual '%s'", expectedError, err.Error())
	}
}

func TestReadAuthorizationModelsInvalidContinuationToken(t *testing.T, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	store := testutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	tds := &openfgapb.TypeDefinitions{
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
			},
		},
	}

	if err := datastore.WriteAuthorizationModel(ctx, store, modelID, tds); err != nil {
		t.Fatal(err)
	}
	encoder, err := encoder.NewTokenEncrypter("key")
	if err != nil {
		t.Fatal(err)
	}

	query := queries.NewReadAuthorizationModelsQuery(datastore, encoder, logger)
	if _, err := query.Execute(ctx, &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           store,
		ContinuationToken: "foo",
	}); !errors.Is(err, serverErrors.InvalidContinuationToken) {
		t.Fatalf("expected '%v', got '%v'", serverErrors.InvalidContinuationToken, err)
	}
}
