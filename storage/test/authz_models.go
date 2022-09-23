package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAndReadAuthorizationModel(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	store := id.Must(id.New()).String()

	model := &openfgapb.AuthorizationModel{
		Id:            id.Must(id.New()).String(),
		SchemaVersion: "1.0",
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

	err := datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(t, err)

	got, err := datastore.ReadAuthorizationModel(ctx, store, model.Id)
	require.NoError(t, err)

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			openfgapb.TypeDefinition{},
			openfgapb.Userset{},
			openfgapb.Userset_This{},
			openfgapb.DirectUserset{},
		),
	}

	if diff := cmp.Diff(got.TypeDefinitions, model.TypeDefinitions, cmpOpts...); diff != "" {
		t.Errorf("mismatch (-got +want):\n%s", diff)
	}

	// And the model does not exist in a different store
	_, err = datastore.ReadAuthorizationModel(ctx, "undefined", model.Id)
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func ReadAuthorizationModelsTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	store := id.Must(id.New()).String()

	model1 := &openfgapb.AuthorizationModel{
		Id:            id.Must(id.New()).String(),
		SchemaVersion: "1.0",
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

	err := datastore.WriteAuthorizationModel(ctx, store, model1)
	require.NoError(t, err)

	model2 := &openfgapb.AuthorizationModel{
		Id:            id.Must(id.New()).String(),
		SchemaVersion: "1.0",
		TypeDefinitions: []*openfgapb.TypeDefinition{
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
		},
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model2)
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
	require.Equal(t, model2.Id, models[0].Id)
	require.NotEmpty(t, continuationToken)

	if diff := cmp.Diff(model2.TypeDefinitions, models[0].TypeDefinitions, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}

	models, continuationToken, err = datastore.ReadAuthorizationModels(ctx, store, storage.PaginationOptions{
		PageSize: 2,
		From:     string(continuationToken),
	})
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.Equal(t, model1.Id, models[0].Id)
	require.Empty(t, continuationToken)

	if diff := cmp.Diff(model1.TypeDefinitions, models[0].TypeDefinitions, cmpOpts...); diff != "" {
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

		oldModel := &openfgapb.AuthorizationModel{
			Id:            id.Must(id.New()).String(),
			SchemaVersion: "1.0",
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
		}
		err := datastore.WriteAuthorizationModel(ctx, store, oldModel)
		require.NoError(t, err)

		newModel := &openfgapb.AuthorizationModel{
			Id:            id.Must(id.New()).String(),
			SchemaVersion: "1.0",
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
		}
		err = datastore.WriteAuthorizationModel(ctx, store, newModel)
		require.NoError(t, err)

		latestID, err := datastore.FindLatestAuthorizationModelID(ctx, store)
		require.NoError(t, err)
		require.Equal(t, newModel.Id, latestID)
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
		model := &openfgapb.AuthorizationModel{
			Id:            id.Must(id.New()).String(),
			SchemaVersion: "1.0",
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

		err := datastore.WriteAuthorizationModel(ctx, store, model)
		require.NoError(t, err)

		typeDef, err := datastore.ReadTypeDefinition(ctx, store, model.Id, "folder")
		require.NoError(t, err)

		cmpOpts := []cmp.Option{
			cmpopts.IgnoreUnexported(
				openfgapb.TypeDefinition{},
				openfgapb.Userset{},
				openfgapb.Userset_This{},
				openfgapb.DirectUserset{},
			),
		}

		if diff := cmp.Diff(model.TypeDefinitions[0], typeDef, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})
}
