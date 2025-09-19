package test

import (
	"context"
	"strconv"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
)

// Used to avoid compiler optimizations (see https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go)
var listObjectsResponse *commands.ListObjectsResponse //nolint

// setupListObjectsBenchmark writes the model and lots of tuples.
func setupListObjectsBenchmark(b *testing.B, ds storage.OpenFGADatastore, storeID string) (*openfgav1.AuthorizationModel, string, int) {
	b.Helper()
	modelID := ulid.Make().String()
	model := &openfgav1.AuthorizationModel{
		Id:            modelID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		// this model exercises all possible execution paths: "direct" edge and "computed userset" edge and "TTU" edge
		TypeDefinitions: parser.MustTransformDSLToProto(`
			model
				schema 1.1
			type user
			type folder
				relations
					define viewer: [user]
			type document
				relations
					define viewer: [user]
					define parent: [folder]
					define can_view: viewer or viewer from parent`).GetTypeDefinitions(),
	}
	err := ds.WriteAuthorizationModel(context.Background(), storeID, model)
	require.NoError(b, err)

	numberObjectsAccesible := 0
	for i := 0; i < 100; i++ {
		var tuples []*openfgav1.TupleKey

		for j := 0; j < ds.MaxTuplesPerWrite(); j++ {
			obj := "document:" + strconv.Itoa(numberObjectsAccesible)

			tuples = append(tuples, tuple.NewTupleKey(obj, "viewer", "user:maria"))

			numberObjectsAccesible++
		}

		tuples = testutils.Shuffle(tuples)

		err := ds.Write(context.Background(), storeID, nil, tuples)
		require.NoError(b, err)
	}

	return model, modelID, numberObjectsAccesible
}

func BenchmarkListObjects(b *testing.B, ds storage.OpenFGADatastore) {
	ctx := context.Background()
	store := ulid.Make().String()

	model, modelID, numberObjectsAccessible := setupListObjectsBenchmark(b, ds, store)
	ts, err := typesystem.New(model)
	require.NoError(b, err)
	ctx = typesystem.ContextWithTypesystem(ctx, ts)

	req := &openfgav1.ListObjectsRequest{
		StoreId:              store,
		AuthorizationModelId: modelID,
		Type:                 "document",
		Relation:             "can_view",
		User:                 "user:maria",
	}

	var r *commands.ListObjectsResponse

	var oneResultIterations, allResultsIterations int

	checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers().Build()
	require.NoError(b, err)
	b.Cleanup(checkResolverCloser)

	b.Run("oneResult", func(b *testing.B) {
		listObjectsQuery, err := commands.NewListObjectsQuery(
			ds,
			checkResolver,
			commands.WithListObjectsMaxResults(1),
		)
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r, err := listObjectsQuery.Execute(ctx, req)
			require.NoError(b, err)
			require.Len(b, r.Objects, 1)
		}

		listObjectsResponse = r
		oneResultIterations = b.N
	})
	b.Run("allResults", func(b *testing.B) {
		listObjectsQuery, err := commands.NewListObjectsQuery(
			ds,
			checkResolver,
			commands.WithListObjectsMaxResults(0),
		)
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r, err := listObjectsQuery.Execute(ctx, req)
			require.NoError(b, err)
			totalObjects := len(r.Objects)
			require.Equal(b, numberObjectsAccessible, totalObjects, "total number of records returned should match")
		}

		listObjectsResponse = r
		allResultsIterations = b.N
	})

	require.Greater(b, oneResultIterations, allResultsIterations)
}
