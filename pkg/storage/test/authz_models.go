package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

func WriteAndReadAuthorizationModelTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	storeID := ulid.Make().String()

	t.Run("write, then read, succeeds", func(t *testing.T) {
		model := &openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			SchemaVersion:   typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "folder"}},
		}

		err := datastore.WriteAuthorizationModel(ctx, storeID, model)
		require.NoError(t, err)

		got, err := datastore.ReadAuthorizationModel(ctx, storeID, model.GetId())
		require.NoError(t, err)

		if diff := cmp.Diff(model, got, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
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

	model1 := &openfgav1.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "folder",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {
						Userset: &openfgav1.Userset_This{
							This: &openfgav1.DirectUserset{},
						},
					},
				},
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
				Type: "folder",
				Relations: map[string]*openfgav1.Userset{
					"reader": {
						Userset: &openfgav1.Userset_This{
							This: &openfgav1.DirectUserset{},
						},
					},
				},
			},
		},
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model2)
	require.NoError(t, err)

	opts := storage.ListOptions{
		Pagination: storage.NewPaginationOptions(1, ""),
	}
	models, continuationToken, err := datastore.ReadAuthorizationModels(ctx, store, opts)
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.NotEmpty(t, continuationToken)

	if diff := cmp.Diff(model2, models[0], cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-want +got):\n%s", diff)
	}

	opts = storage.ListOptions{
		Pagination: storage.NewPaginationOptions(2, string(continuationToken)),
	}
	models, continuationToken, err = datastore.ReadAuthorizationModels(ctx, store, opts)
	require.NoError(t, err)
	require.Len(t, models, 1)
	require.Empty(t, continuationToken)

	if diff := cmp.Diff(model1, models[0], cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-want +got):\n%s", diff)
	}
}

func FindLatestAuthorizationModelTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("find_latest_authorization_model_should_return_not_found_when_no_models", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		_, err := datastore.FindLatestAuthorizationModel(ctx, store)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("find_latest_authorization_model_should_succeed", func(t *testing.T) {
		store := ulid.Make().String()

		oldModel := &openfgav1.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "folder",
					Relations: map[string]*openfgav1.Userset{
						"viewer": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
					},
				},
			},
		}
		err := datastore.WriteAuthorizationModel(ctx, store, oldModel)
		require.NoError(t, err)

		updatedModel := &openfgav1.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "folder",
					Relations: map[string]*openfgav1.Userset{
						"owner": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"owner": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
					},
				},
			},
		}
		err = datastore.WriteAuthorizationModel(ctx, store, updatedModel)
		require.NoError(t, err)

		latestModel, err := datastore.FindLatestAuthorizationModel(ctx, store)
		require.NoError(t, err)
		if diff := cmp.Diff(updatedModel, latestModel, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}

		newModel := &openfgav1.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "folder",
					Relations: map[string]*openfgav1.Userset{
						"reader": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
					},
				},
			},
		}
		err = datastore.WriteAuthorizationModel(ctx, store, newModel)
		require.NoError(t, err)

		latestModel, err = datastore.FindLatestAuthorizationModel(ctx, store)
		require.NoError(t, err)
		if diff := cmp.Diff(newModel, latestModel, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
}
