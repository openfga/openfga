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

	tests := []struct {
		name                  string
		model                 string
		tuples                []*openfgav1.TupleKey
		objectType            string
		relation              string
		user                  string
		expectedDispatchCount uint32
	}{
		{
			name: "Test direct relation",
			model: `model
schema 1.1

type user

type folder
  	relations
		define viewer: [user] 
`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:C", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:B", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:A", "viewer", "user:jon"),
			},
			objectType:            "folder",
			relation:              "viewer",
			user:                  "user:jon",
			expectedDispatchCount: 3,
		},
		{
			name: "Test union relation",
			model: `model
schema 1.1

type user

type folder
  	relations
		define editor: [user]
		define viewer: [user] or editor 
`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:C", "editor", "user:jon"),
				tuple.NewTupleKey("folder:B", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:A", "viewer", "user:jon"),
			},
			objectType:            "folder",
			relation:              "viewer",
			user:                  "user:jon",
			expectedDispatchCount: 4,
		},
		{
			name: "Test intersection relation",
			model: `model
schema 1.1

type user

type folder
  	relations
		define editor: [user]
		define can_delete: [user] and editor 
`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:C", "can_delete", "user:jon"),
				tuple.NewTupleKey("folder:B", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:A", "viewer", "user:jon"),
			},
			objectType:            "folder",
			relation:              "can_delete",
			user:                  "user:jon",
			expectedDispatchCount: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			storeID := ulid.Make().String()
			model := parser.MustTransformDSLToProto(test.model)

			err := ds.Write(ctx, storeID, nil, test.tuples)
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
				StoreId:  storeID,
				Type:     test.objectType,
				Relation: test.relation,
				User:     test.user,
			})

			require.NoError(t, err)
			t.Log(resp.Objects)
			t.Log(*resp.ResolutionMetadata.DispatchCount)

			require.Equal(t, test.expectedDispatchCount, *resp.ResolutionMetadata.DispatchCount)
		})
	}
}
