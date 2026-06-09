package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// TestIdentifierColumnCollation asserts that identifier columns on the
// SQLite backend treat byte-distinct values as distinct rows. Locked in
// as a regression guard against future collation drift.
func TestIdentifierColumnCollation(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()

	cases := []struct {
		name   string
		store  string
		tuples []*openfgav1.TupleKey
	}{
		{
			name:  "user_byte_distinct",
			store: "store_user",
			tuples: []*openfgav1.TupleKey{
				tupleUtils.NewTupleKey("doc:1", "viewer", "user:A"),
				tupleUtils.NewTupleKey("doc:1", "viewer", "user:a"),
				tupleUtils.NewTupleKey("doc:1", "viewer", "user:AA"),
			},
		},
		{
			name:  "object_type_byte_distinct",
			store: "store_obj_type",
			tuples: []*openfgav1.TupleKey{
				tupleUtils.NewTupleKey("Document:2", "viewer", "user:b"),
				tupleUtils.NewTupleKey("document:2", "viewer", "user:b"),
			},
		},
		{
			name:  "relation_byte_distinct",
			store: "store_rel",
			tuples: []*openfgav1.TupleKey{
				tupleUtils.NewTupleKey("doc:3", "Viewer", "user:c"),
				tupleUtils.NewTupleKey("doc:3", "viewer", "user:c"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, tk := range tc.tuples {
				err := ds.Write(ctx, tc.store, nil, []*openfgav1.TupleKey{tk})
				require.NoErrorf(t, err, "writing tuple %q must succeed", tk.String())
			}

			iter, err := ds.Read(ctx, tc.store, storage.ReadFilter{}, storage.ReadOptions{})
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

			// Each value must round-trip through the targeted read path
			// returning exactly its own row.
			for _, tk := range tc.tuples {
				got, err := ds.ReadUserTuple(ctx, tc.store, storage.ReadUserTupleFilter{
					Object:   tk.GetObject(),
					Relation: tk.GetRelation(),
					User:     tk.GetUser(),
				}, storage.ReadUserTupleOptions{})
				require.NoErrorf(t, err, "ReadUserTuple must find exact-match tuple %q", tk.String())
				require.Equal(t, tk.GetObject(), got.GetKey().GetObject())
				require.Equal(t, tk.GetRelation(), got.GetKey().GetRelation())
				require.Equal(t, tk.GetUser(), got.GetKey().GetUser())
			}
		})
	}

	// Cover the equivalent read paths on changelog and authorization_model.

	t.Run("changelog_object_type_byte_distinct", func(t *testing.T) {
		store := "store_changelog"
		err := ds.Write(ctx, store, nil, []*openfgav1.TupleKey{
			tupleUtils.NewTupleKey("Document:42", "viewer", "user:d"),
		})
		require.NoError(t, err)
		err = ds.Write(ctx, store, nil, []*openfgav1.TupleKey{
			tupleUtils.NewTupleKey("document:42", "viewer", "user:d"),
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
					"ReadChanges with ObjectType=%q must not return non-matching rows", objectType)
			}
		}
	})

	t.Run("authorization_model_type_byte_distinct", func(t *testing.T) {
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
		require.NoError(t, err, "writing a model with byte-distinct type names must succeed")

		got, err := ds.ReadAuthorizationModel(ctx, store, modelID)
		require.NoError(t, err)
		gotTypes := make(map[string]struct{}, len(got.GetTypeDefinitions()))
		for _, td := range got.GetTypeDefinitions() {
			gotTypes[td.GetType()] = struct{}{}
		}
		require.Contains(t, gotTypes, "User")
		require.Contains(t, gotTypes, "user")
		require.Len(t, gotTypes, 2, "expected both type definitions to round-trip")
	})
}
