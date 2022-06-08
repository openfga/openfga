package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAndReadAuthorizationModel(t *testing.T, dbTester DatastoreTester[storage.OpenFGADatastore]) {

	require := require.New(t)
	ctx := context.Background()

	datastore, err := dbTester.New()
	require.NoError(err)

	store := testutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	expectedModel := &openfgapb.TypeDefinitions{
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "folder",
				Relations: map[string]*openfgapb.Userset{
					"viewer": {
						Userset: &openfgapb.Userset_This{
							This: &openfgapb.DirectUserset{},
						},
					},
				},
			},
		},
	}

	if err := datastore.WriteAuthorizationModel(ctx, store, modelID, expectedModel); err != nil {
		t.Errorf("failed to write authorization model: %v", err)
	}

	model, err := datastore.ReadAuthorizationModel(ctx, store, modelID)
	if err != nil {
		t.Errorf("failed to read authorization model: %v", err)
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			openfgapb.TypeDefinition{},
			openfgapb.Userset{},
			openfgapb.Userset_This{},
			openfgapb.DirectUserset{},
		),
	}

	if diff := cmp.Diff(expectedModel.TypeDefinitions, model.TypeDefinitions, cmpOpts...); diff != "" {
		t.Errorf("mismatch (-got +want):\n%s", diff)
	}

	_, err = datastore.ReadAuthorizationModel(ctx, "undefined", modelID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("got error '%v', want '%v'", err, storage.ErrNotFound)
	}
}

func ReadAuthorizationModelsTest(t *testing.T, dbTester DatastoreTester[storage.OpenFGADatastore]) {

	require := require.New(t)
	ctx := context.Background()

	datastore, err := dbTester.New()
	require.NoError(err)

	store := testutils.CreateRandomString(10)
	modelID1, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	tds1 := []*openfgapb.TypeDefinition{
		{
			Type: "folder",
			Relations: map[string]*openfgapb.Userset{
				"viewer": {
					Userset: &openfgapb.Userset_This{
						This: &openfgapb.DirectUserset{},
					},
				},
			},
		},
	}
	err = datastore.WriteAuthorizationModel(ctx, store, modelID1, &openfgapb.TypeDefinitions{TypeDefinitions: tds1})
	if err != nil {
		t.Fatalf("failed to write authorization model: %v", err)
	}

	modelID2, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	tds2 := []*openfgapb.TypeDefinition{
		{
			Type: "folder",
			Relations: map[string]*openfgapb.Userset{
				"reader": {
					Userset: &openfgapb.Userset_This{
						This: &openfgapb.DirectUserset{},
					},
				},
			},
		},
	}
	err = datastore.WriteAuthorizationModel(ctx, store, modelID2, &openfgapb.TypeDefinitions{TypeDefinitions: tds2})
	if err != nil {
		t.Fatalf("failed to write authorization model: %v", err)
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			openfgapb.TypeDefinition{},
			openfgapb.Userset{},
			openfgapb.Userset_This{},
			openfgapb.DirectUserset{},
		),
	}

	models, continuationToken, err := datastore.ReadAuthorizationModels(ctx, store, storage.PaginationOptions{
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("expected no error but got '%v'", err)
	}
	if len(models) != 1 {
		t.Fatalf("expected 1, got %d", len(models))
	}
	if modelID2 != models[0].Id {
		t.Fatalf("expected '%s', got '%s", modelID2, models[0].Id)
	}
	if diff := cmp.Diff(tds2, models[0].TypeDefinitions, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}
	if len(continuationToken) == 0 {
		t.Fatalf("expected non-empty continuation token")
	}

	models, continuationToken, err = datastore.ReadAuthorizationModels(ctx, store, storage.PaginationOptions{
		PageSize: 2,
		From:     string(continuationToken),
	})
	if err != nil {
		t.Fatalf("expected no error but got '%v'", err)
	}
	if len(models) != 1 {
		t.Fatalf("expected 1, got %d", len(models))
	}
	if modelID1 != models[0].Id {
		t.Fatalf("expected '%s', got '%s", modelID1, models[0].Id)
	}
	if diff := cmp.Diff(tds1, models[0].TypeDefinitions, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}
	if len(continuationToken) != 0 {
		t.Fatalf("expected empty continuation token but got '%v'", string(continuationToken))
	}
}

func FindLatestAuthorizationModelIDTest(t *testing.T, dbTester DatastoreTester[storage.OpenFGADatastore]) {

	require := require.New(t)
	ctx := context.Background()

	datastore, err := dbTester.New()
	require.NoError(err)

	t.Run("find latest authorization model should return not found when no models", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		_, err := datastore.FindLatestAuthorizationModelID(ctx, store)
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("got error '%v', want '%v'", err, storage.ErrNotFound)
		}
	})

	t.Run("find latests authorization model should succeed", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		now := time.Now()
		oldModelID, err := id.NewStringFromTime(now)
		if err != nil {
			t.Fatal(err)
		}
		err = datastore.WriteAuthorizationModel(ctx, store, oldModelID, &openfgapb.TypeDefinitions{
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"viewer": {
							Userset: &openfgapb.Userset_This{},
						},
					},
				},
			},
		})
		if err != nil {
			t.Errorf("failed to write authorization model: %v", err)
		}

		newModelID, err := id.NewStringFromTime(now)
		if err != nil {
			t.Fatal(err)
		}
		err = datastore.WriteAuthorizationModel(ctx, store, newModelID, &openfgapb.TypeDefinitions{
			TypeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"reader": {
							Userset: &openfgapb.Userset_This{},
						},
					},
				},
			},
		})
		if err != nil {
			t.Errorf("failed to write authorization model: %v", err)
		}

		latestID, err := datastore.FindLatestAuthorizationModelID(ctx, store)
		if err != nil {
			t.Errorf("failed to read latest authorization model: %v", err)
		}

		if latestID != newModelID {
			t.Errorf("got '%s', want '%s'", latestID, newModelID)
		}
	})
}

func ReadTypeDefinitionTest(t *testing.T, dbTester DatastoreTester[storage.OpenFGADatastore]) {

	require := require.New(t)
	ctx := context.Background()

	datastore, err := dbTester.New()
	require.NoError(err)

	t.Run("read type definition of nonexistent type should return not found", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		modelID, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}

		_, err = datastore.ReadTypeDefinition(ctx, store, modelID, "folder")
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("got error '%v', want '%v'", err, storage.ErrNotFound)
		}
	})

	t.Run("read type definition should succeed", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		modelID, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}
		expectedTypeDef := &openfgapb.TypeDefinition{
			Type: "folder",
			Relations: map[string]*openfgapb.Userset{
				"viewer": {
					Userset: &openfgapb.Userset_This{
						This: &openfgapb.DirectUserset{},
					},
				},
			},
		}

		err = datastore.WriteAuthorizationModel(ctx, store, modelID, &openfgapb.TypeDefinitions{
			TypeDefinitions: []*openfgapb.TypeDefinition{
				expectedTypeDef,
			},
		})
		if err != nil {
			t.Errorf("failed to write authorization model: %v", err)
		}

		typeDef, err := datastore.ReadTypeDefinition(ctx, store, modelID, "folder")
		if err != nil {
			t.Errorf("expected no error but got '%v'", err)
		}

		cmpOpts := []cmp.Option{
			cmpopts.IgnoreUnexported(
				openfgapb.TypeDefinition{},
				openfgapb.Userset{},
				openfgapb.Userset_This{},
				openfgapb.DirectUserset{},
			),
		}

		if diff := cmp.Diff(expectedTypeDef, typeDef, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})
}
