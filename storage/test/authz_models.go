package test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAndReadAuthorizationModel(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()

	store := id.Must(id.New()).String()
	modelID := id.Must(id.New()).String()
	expectedModel := []*openfgapb.TypeDefinition{
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

	err := datastore.WriteAuthorizationModel(ctx, store, modelID, typesystem.SchemaVersion1_0, expectedModel)
	require.NoError(t, err)

	model, err := datastore.ReadAuthorizationModel(ctx, store, modelID)
	require.NoError(t, err)

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			openfgapb.TypeDefinition{},
			openfgapb.Userset{},
			openfgapb.Userset_This{},
			openfgapb.DirectUserset{},
		),
	}

	if diff := cmp.Diff(expectedModel, model.TypeDefinitions, cmpOpts...); diff != "" {
		t.Errorf("mismatch (-got +want):\n%s", diff)
	}

	_, err = datastore.ReadAuthorizationModel(ctx, "undefined", modelID)
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func ReadAuthorizationModelsTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	store := id.Must(id.New()).String()
	modelID1 := id.Must(id.New()).String()
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

	err := datastore.WriteAuthorizationModel(ctx, store, modelID1, typesystem.SchemaVersion1_0, tds1)
	require.NoError(t, err)

	modelID2 := id.Must(id.New()).String()
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

	err = datastore.WriteAuthorizationModel(ctx, store, modelID2, typesystem.SchemaVersion1_0, tds2)
	require.NoError(t, err)

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
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.Equal(t, modelID2, models[0].Id)
	require.NotEmpty(t, continuationToken)

	if diff := cmp.Diff(tds2, models[0].TypeDefinitions, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}

	models, continuationToken, err = datastore.ReadAuthorizationModels(ctx, store, storage.PaginationOptions{
		PageSize: 2,
		From:     string(continuationToken),
	})
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.Equal(t, modelID1, models[0].Id)
	require.Empty(t, continuationToken)

	if diff := cmp.Diff(tds1, models[0].TypeDefinitions, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}
}

func FindLatestAuthorizationModelIDTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("find latest authorization model should return not found when no models", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		_, err := datastore.FindLatestAuthorizationModelID(ctx, store)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("find latests authorization model should succeed", func(t *testing.T) {
		store := id.Must(id.New()).String()
		now := time.Now()

		oldModelID := id.Must(id.NewFromTime(now)).String()
		err := datastore.WriteAuthorizationModel(ctx, store, oldModelID, typesystem.SchemaVersion1_0, []*openfgapb.TypeDefinition{
			{
				Type: "folder",
				Relations: map[string]*openfgapb.Userset{
					"viewer": {
						Userset: &openfgapb.Userset_This{},
					},
				},
			},
		})
		require.NoError(t, err)

		newModelID := id.Must(id.NewFromTime(now)).String()
		err = datastore.WriteAuthorizationModel(ctx, store, newModelID, typesystem.SchemaVersion1_0, []*openfgapb.TypeDefinition{
			{
				Type: "folder",
				Relations: map[string]*openfgapb.Userset{
					"reader": {
						Userset: &openfgapb.Userset_This{},
					},
				},
			},
		})
		require.NoError(t, err)

		latestID, err := datastore.FindLatestAuthorizationModelID(ctx, store)
		require.NoError(t, err)
		require.Equal(t, newModelID, latestID)
	})
}

func ReadTypeDefinitionTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("read type definition of nonexistent type should return not found", func(t *testing.T) {
		store := id.Must(id.New()).String()
		modelID := id.Must(id.New()).String()

		_, err := datastore.ReadTypeDefinition(ctx, store, modelID, "folder")
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("read type definition should succeed", func(t *testing.T) {
		store := id.Must(id.New()).String()
		modelID := id.Must(id.New()).String()
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

		err := datastore.WriteAuthorizationModel(ctx, store, modelID, typesystem.SchemaVersion1_0, []*openfgapb.TypeDefinition{expectedTypeDef})
		require.NoError(t, err)

		typeDef, err := datastore.ReadTypeDefinition(ctx, store, modelID, "folder")
		require.NoError(t, err)

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
