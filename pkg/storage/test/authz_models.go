package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func WriteAndReadAuthorizationModelTest(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()
	storeID := ulid.Make().String()

	t.Run("write, then read, succeeds", func(t *testing.T) {
		model := &openfgapb.AuthorizationModel{
			Id:              ulid.Make().String(),
			SchemaVersion:   typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgapb.TypeDefinition{{Type: "folder"}},
		}

		err := datastore.WriteAuthorizationModel(ctx, storeID, model)
		require.NoError(t, err)

		got, err := datastore.ReadAuthorizationModel(ctx, storeID, model.Id)
		require.NoError(t, err)

		if diff := cmp.Diff(got, model, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("trying_to_get_a_model_which_does_not_exist_returns_not_found", func(t *testing.T) {
		_, err := datastore.ReadAuthorizationModel(ctx, storeID, ulid.Make().String())
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}

func ReadAuthorizationModelsTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	store := ulid.Make().String()

	model1 := &openfgapb.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
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
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
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

	models, continuationToken, err := datastore.ReadAuthorizationModels(ctx, store, storage.PaginationOptions{
		PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.NotEmpty(t, continuationToken)

	if diff := cmp.Diff(model2, models[0], cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}

	models, continuationToken, err = datastore.ReadAuthorizationModels(ctx, store, storage.PaginationOptions{
		PageSize: 2,
		From:     string(continuationToken),
	})
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.Empty(t, continuationToken)

	if diff := cmp.Diff(model1, models[0], cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}
}

func FindLatestAuthorizationModelIDTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("find_latest_authorization_model_should_return_not_found_when_no_models", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		_, err := datastore.FindLatestAuthorizationModelID(ctx, store)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("find_latest_authorization_model_should_succeed", func(t *testing.T) {
		store := ulid.Make().String()

		oldModel := &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
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
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
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

	t.Run("read_type_definition_of_nonexistent_type_should_return_not_found", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()

		_, err := datastore.ReadTypeDefinition(ctx, store, modelID, "folder")
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("read_type_definition_should_succeed", func(t *testing.T) {
		store := ulid.Make().String()
		model := &openfgapb.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_0,
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

		if diff := cmp.Diff(model.TypeDefinitions[0], typeDef, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})
}
