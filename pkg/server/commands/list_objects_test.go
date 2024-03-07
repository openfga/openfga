package commands

import (
	"context"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/storage/memory"
)

func TestNewListObjectsQuery(t *testing.T) {
	t.Run("nil_datastore", func(t *testing.T) {
		q, err := NewListObjectsQuery(nil, graph.NewLocalCheckerWithCycleDetection())
		require.Nil(t, q)
		require.Error(t, err)
	})

	t.Run("nil_checkResolver", func(t *testing.T) {
		q, err := NewListObjectsQuery(memory.New(), nil)
		require.Nil(t, q)
		require.Error(t, err)
	})
}

func TestCheckDispatchCount(t *testing.T) {
	ds := memory.New()
	ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)

	t.Run("dispatch_count_ttu", func(t *testing.T) {
		storeID := ulid.Make().String()
		model := parser.MustTransformDSLToProto(`model
  schema 1.1

type user

type folder
  	relations
		define viewer: [user] 
`)

		err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("folder:C", "viewer", "user:jon"),
			tuple.NewTupleKey("folder:B", "viewer", "user:jon"),
			tuple.NewTupleKey("folder:A", "viewer", "user:jon"),
		})
		require.NoError(t, err)

		typesys, err := typesystem.NewAndValidate(
			context.Background(),
			model,
		)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		checker := graph.NewLocalCheckerWithCycleDetection(
			graph.WithMaxConcurrentReads(1),
		)

		q, err := NewListObjectsQuery(
			ds,
			checker,
		)

		resp, err := q.Execute(ctx, &openfgav1.ListObjectsRequest{
			Type:     "folder",
			Relation: "viewer",
			User:     "user:jon",
		})

		require.NoError(t, err)
		t.Log(resp.Objects)
		t.Log(*resp.ResolutionMetadata.DispatchCount)

		//require.Equal(t, uint32(3), resp.ResolutionMetadata.DispatchCount)
	})

}
