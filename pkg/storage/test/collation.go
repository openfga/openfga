package test

import (
	"context"
	"errors"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// IdentifierCaseSensitivityTest asserts that identifiers differing only in
// case are stored and retrieved as distinct values across the datastore's
// read paths. The MySQL backend requires migration 008 to satisfy this;
// Postgres and SQLite are byte-exact under their default collations and
// should satisfy it without any schema change. The in-memory backend
// performs string comparison directly and is unaffected by collation.
func IdentifierCaseSensitivityTest(t *testing.T, ds storage.OpenFGADatastore) {
	ctx := context.Background()

	cases := []struct {
		name   string
		tuples []*openfgav1.TupleKey
	}{
		{
			name: "user_case_distinct",
			tuples: []*openfgav1.TupleKey{
				tupleUtils.NewTupleKey("doc:1", "viewer", "user:Alice"),
				tupleUtils.NewTupleKey("doc:1", "viewer", "user:alice"),
				tupleUtils.NewTupleKey("doc:1", "viewer", "user:ALICE"),
			},
		},
		{
			name: "object_type_case_distinct",
			tuples: []*openfgav1.TupleKey{
				tupleUtils.NewTupleKey("Document:2", "viewer", "user:bob"),
				tupleUtils.NewTupleKey("document:2", "viewer", "user:bob"),
			},
		},
		{
			name: "relation_case_distinct",
			tuples: []*openfgav1.TupleKey{
				tupleUtils.NewTupleKey("doc:3", "Viewer", "user:carol"),
				tupleUtils.NewTupleKey("doc:3", "viewer", "user:carol"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := ulid.Make().String()

			for _, tk := range tc.tuples {
				err := ds.Write(ctx, store, nil, []*openfgav1.TupleKey{tk})
				require.NoErrorf(t, err, "writing case-distinct tuple %q must succeed", tk.String())
			}

			iter, err := ds.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
			require.NoError(t, err)
			defer iter.Stop()

			seen := map[string]struct{}{}
			for {
				cur, err := iter.Next(ctx)
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.NoError(t, err)
				k := cur.GetKey()
				seen[k.GetObject()+"#"+k.GetRelation()+"@"+k.GetUser()] = struct{}{}
			}

			for _, tk := range tc.tuples {
				key := tk.GetObject() + "#" + tk.GetRelation() + "@" + tk.GetUser()
				require.Containsf(t, seen, key, "tuple %q missing from read", key)
			}
			require.Lenf(t, seen, len(tc.tuples), "expected %d distinct rows, got %d", len(tc.tuples), len(seen))

			// Each case-distinct identifier must round-trip through the
			// targeted read path returning exactly its own row.
			for _, tk := range tc.tuples {
				got, err := ds.ReadUserTuple(ctx, store, storage.ReadUserTupleFilter{
					Object:   tk.GetObject(),
					Relation: tk.GetRelation(),
					User:     tk.GetUser(),
				}, storage.ReadUserTupleOptions{})
				require.NoErrorf(t, err, "ReadUserTuple must find exact-case tuple %q", tk.String())
				require.Equal(t, tk.GetObject(), got.GetKey().GetObject())
				require.Equal(t, tk.GetRelation(), got.GetKey().GetRelation())
				require.Equal(t, tk.GetUser(), got.GetKey().GetUser())
			}
		})
	}

	t.Run("changelog_object_type_case_distinct", func(t *testing.T) {
		store := ulid.Make().String()

		err := ds.Write(ctx, store, nil, []*openfgav1.TupleKey{
			tupleUtils.NewTupleKey("Document:42", "viewer", "user:dan"),
		})
		require.NoError(t, err)
		err = ds.Write(ctx, store, nil, []*openfgav1.TupleKey{
			tupleUtils.NewTupleKey("document:42", "viewer", "user:dan"),
		})
		require.NoError(t, err)

		for _, objectType := range []string{"Document", "document"} {
			changes, _, err := ds.ReadChanges(ctx, store, storage.ReadChangesFilter{
				ObjectType: objectType,
			}, storage.ReadChangesOptions{Pagination: storage.NewPaginationOptions(50, "")})
			require.NoError(t, err)
			require.NotEmpty(t, changes, "ReadChanges with ObjectType=%q must find at least one matching change", objectType)
			for _, c := range changes {
				require.Equal(t, objectType+":42", c.GetTupleKey().GetObject(),
					"ReadChanges with ObjectType=%q must not return changes for a differently-cased type", objectType)
			}
		}
	})

	t.Run("authorization_model_type_case_distinct", func(t *testing.T) {
		store := ulid.Make().String()
		modelID := ulid.Make().String()
		model := &openfgav1.AuthorizationModel{
			Id:            modelID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{Type: "User"},
				{Type: "user"},
			},
		}
		err := ds.WriteAuthorizationModel(ctx, store, model)
		require.NoError(t, err, "writing a model with two case-distinct types must succeed")

		got, err := ds.ReadAuthorizationModel(ctx, store, modelID)
		require.NoError(t, err)
		gotTypes := make(map[string]struct{}, len(got.GetTypeDefinitions()))
		for _, td := range got.GetTypeDefinitions() {
			gotTypes[td.GetType()] = struct{}{}
		}
		require.Contains(t, gotTypes, "User")
		require.Contains(t, gotTypes, "user")
		require.Len(t, gotTypes, 2, "expected both case-distinct type definitions to round-trip")
	})
}
