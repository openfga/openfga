package commands

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"

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

func TestListObjectsDispatchCount(t *testing.T) {
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
			name: "test_direct_relation",
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
			name: "test_union_relation",
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
			name: "test_intersection_relation",
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
		{
			name: "no_tuples",
			model: `model
			schema 1.1

			type user

			type folder
				 relations
					  define editor: [user]
					  define can_delete: [user] and editor 
			`,
			tuples:                []*openfgav1.TupleKey{},
			objectType:            "folder",
			relation:              "can_delete",
			user:                  "user:jon",
			expectedDispatchCount: 0,
		},
		{
			name: "direct_userset_dispatch",
			model: `model
			schema 1.1

			type user

			type group
			  relations
				define member: [user, group#member]
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng#member", "member", "group:fga#member"),
				tuple.NewTupleKey("group:fga", "member", "user:jon"),
			},
			objectType:            "group",
			relation:              "member",
			user:                  "user:jon",
			expectedDispatchCount: 2,
		},
		{
			name: "computed_userset_dispatch",
			model: `model
			schema 1.1

			type user

			type document
			  relations
				define editor: [user]
				define viewer: editor
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "editor", "user:jon"),
			},
			objectType:            "document",
			relation:              "viewer",
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

			q, _ := NewListObjectsQuery(
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

			require.Equal(t, test.expectedDispatchCount, *resp.ResolutionMetadata.DispatchCount)
		})
	}
}
