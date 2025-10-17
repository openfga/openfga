package test

import (
	"context"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

func WriteAndReadAuthorizationModelTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	storeID := ulid.Make().String()

	t.Run("write_model_with_zero_type_succeeds_but_read_returns_not_found", func(t *testing.T) {
		model := &openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			SchemaVersion:   typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgav1.TypeDefinition{},
		}

		modelId, err := datastore.WriteAuthorizationModel(ctx, storeID, model, "fakehash")
		require.NoError(t, err)
		require.Equal(t, modelId, model.GetId())

		got, err := datastore.ReadAuthorizationModel(ctx, storeID, model.GetId())
		require.Nil(t, got)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("write_model_with_one_type_succeeds_and_read_succeeds", func(t *testing.T) {
		model := &openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			SchemaVersion:   typesystem.SchemaVersion1_0,
			TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "folder"}},
		}

		modelId, err := datastore.WriteAuthorizationModel(ctx, storeID, model, "fakehash")
		require.NoError(t, err)
		require.Equal(t, modelId, model.GetId())

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

	t.Run("returns_zero_if_none_written", func(t *testing.T) {
		resp, token, err := datastore.ReadAuthorizationModels(ctx, store, storage.ReadAuthorizationModelsOptions{})
		require.NoError(t, err)
		require.Empty(t, resp)
		require.Empty(t, token)
	})

	const numOfWrites = 300
	modelsWritten := make([]*openfgav1.AuthorizationModel, numOfWrites)
	for i := 0; i < numOfWrites; i++ {
		model := parser.MustTransformDSLToProto(`
			model
				schema 1.1

			type user
			type group
				relations
					define member: [user with condX]
			condition condX(x: int) {
				x < 100
			}`)
		model.Id = ulid.Make().String()
		modelsWritten[i] = model
		modelId, err := datastore.WriteAuthorizationModel(ctx, store, model, "fakehash")
		require.NoError(t, err)
		require.Equal(t, modelId, model.GetId())
	}

	// when read, the models should be in inverse order
	slices.Reverse(modelsWritten)

	t.Run("returns_one_(latest)_if_page_size_one", func(t *testing.T) {
		opts := storage.ReadAuthorizationModelsOptions{
			Pagination: storage.NewPaginationOptions(1, ""),
		}
		models, continuationToken, err := datastore.ReadAuthorizationModels(ctx, store, opts)
		require.NoError(t, err)
		require.Len(t, models, 1)
		require.NotEmpty(t, continuationToken)

		if diff := cmp.Diff(models[len(models)-1], models[0], cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("pagination_works", func(t *testing.T) {
		t.Run("read_page_size_1_returns_everything", func(t *testing.T) {
			seenModels := readModelsWithPageSize(t, datastore, store, 1)
			if diff := cmp.Diff(modelsWritten, seenModels, cmpSortTupleKeys...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("read_page_size_2_returns_everything", func(t *testing.T) {
			seenModels := readModelsWithPageSize(t, datastore, store, 2)
			if diff := cmp.Diff(modelsWritten, seenModels, cmpSortTupleKeys...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("read_page_size_default_returns_everything", func(t *testing.T) {
			seenModels := readModelsWithPageSize(t, datastore, store, storage.DefaultPageSize)
			if diff := cmp.Diff(modelsWritten, seenModels, cmpSortTupleKeys...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("read_page_size_infinite_returns_everything", func(t *testing.T) {
			seenModels := readModelsWithPageSize(t, datastore, store, storage.DefaultPageSize*500000)
			if diff := cmp.Diff(modelsWritten, seenModels, cmpSortTupleKeys...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	})
}

func readModelsWithPageSize(t *testing.T, ds storage.OpenFGADatastore, storeID string, pageSize int) []*openfgav1.AuthorizationModel {
	t.Helper()
	var (
		models            []*openfgav1.AuthorizationModel
		seenModels        []*openfgav1.AuthorizationModel
		continuationToken string
		err               error
	)
	for {
		opts := storage.ReadAuthorizationModelsOptions{
			Pagination: storage.NewPaginationOptions(int32(pageSize), continuationToken),
		}
		models, continuationToken, err = ds.ReadAuthorizationModels(context.Background(), storeID, opts)
		if err != nil {
			require.FailNow(t, "unexpected error when reading models", err)
			break
		}

		seenModels = append(seenModels, models...)
		if len(continuationToken) != 0 {
			require.Equal(t, len(models), pageSize)
		} else {
			require.LessOrEqual(t, len(models), pageSize)
			break
		}
	}

	return seenModels
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
		oldModelId, err := datastore.WriteAuthorizationModel(ctx, store, oldModel, "fakehash-old")
		require.NoError(t, err)
		require.Equal(t, oldModelId, oldModel.GetId())

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
		updatedModelId, err := datastore.WriteAuthorizationModel(ctx, store, updatedModel, "fakehash")
		require.NoError(t, err)
		require.Equal(t, updatedModelId, updatedModel.GetId())

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
		newModelId, err := datastore.WriteAuthorizationModel(ctx, store, newModel, "fakehash")
		require.NoError(t, err)
		require.Equal(t, newModelId, newModel.GetId())

		latestModel, err = datastore.FindLatestAuthorizationModel(ctx, store)
		require.NoError(t, err)
		if diff := cmp.Diff(newModel, latestModel, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
}
