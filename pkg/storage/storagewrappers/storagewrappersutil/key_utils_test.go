package storagewrappersutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

func TestReadStartingWithUserKey(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		filter := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:alice"},
			},
		}
		key, err := ReadStartingWithUserKey("store1", filter)
		require.NoError(t, err)
		require.NotEmpty(t, key)
	})

	t.Run("deterministic", func(t *testing.T) {
		filter := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:alice"},
			},
		}
		k1, err := ReadStartingWithUserKey("store1", filter)
		require.NoError(t, err)
		k2, err := ReadStartingWithUserKey("store1", filter)
		require.NoError(t, err)
		require.Equal(t, k1, k2)
	})

	t.Run("different_stores_different_keys", func(t *testing.T) {
		filter := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:alice"},
			},
		}
		k1, err := ReadStartingWithUserKey("store1", filter)
		require.NoError(t, err)
		k2, err := ReadStartingWithUserKey("store2", filter)
		require.NoError(t, err)
		require.NotEqual(t, k1, k2)
	})

	t.Run("different_object_types_different_keys", func(t *testing.T) {
		f1 := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:alice"}},
		}
		f2 := storage.ReadStartingWithUserFilter{
			ObjectType: "folder",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:alice"}},
		}
		k1, err := ReadStartingWithUserKey("store1", f1)
		require.NoError(t, err)
		k2, err := ReadStartingWithUserKey("store1", f2)
		require.NoError(t, err)
		require.NotEqual(t, k1, k2)
	})

	t.Run("with_object_ids", func(t *testing.T) {
		filter := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:alice"}},
			ObjectIDs:  storage.NewSortedSet("id1", "id2"),
		}
		key, err := ReadStartingWithUserKey("store1", filter)
		require.NoError(t, err)
		require.NotEmpty(t, key)

		filterNoIDs := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:alice"}},
		}
		keyNoIDs, err := ReadStartingWithUserKey("store1", filterNoIDs)
		require.NoError(t, err)
		require.NotEqual(t, key, keyNoIDs)
	})

	t.Run("with_relation_on_user", func(t *testing.T) {
		f1 := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "group:eng", Relation: "member"}},
		}
		f2 := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "group:eng"}},
		}
		k1, err := ReadStartingWithUserKey("store1", f1)
		require.NoError(t, err)
		k2, err := ReadStartingWithUserKey("store1", f2)
		require.NoError(t, err)
		require.NotEqual(t, k1, k2)
	})

	t.Run("collision_delimiter_in_object_type", func(t *testing.T) {
		f1 := storage.ReadStartingWithUserFilter{
			ObjectType: "doc",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:alice"}},
		}
		f2 := storage.ReadStartingWithUserFilter{
			ObjectType: "doc#viewer",
			Relation:   "",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:alice"}},
		}
		k1, err := ReadStartingWithUserKey("store1", f1)
		require.NoError(t, err)
		k2, err := ReadStartingWithUserKey("store1", f2)
		require.NoError(t, err)
		require.NotEqual(t, k1, k2)
	})
}

func TestReadUsersetTuplesKey(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		filter := storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				{Type: "user"},
			},
		}
		key := ReadUsersetTuplesKey("store1", filter)
		require.NotEmpty(t, key)
	})

	t.Run("deterministic", func(t *testing.T) {
		filter := storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
				{Type: "user"},
			},
		}
		k1 := ReadUsersetTuplesKey("store1", filter)
		k2 := ReadUsersetTuplesKey("store1", filter)
		require.Equal(t, k1, k2)
	})

	t.Run("different_stores_different_keys", func(t *testing.T) {
		filter := storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
		}
		k1 := ReadUsersetTuplesKey("store1", filter)
		k2 := ReadUsersetTuplesKey("store2", filter)
		require.NotEqual(t, k1, k2)
	})

	t.Run("different_restrictions_different_keys", func(t *testing.T) {
		f1 := storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				{Type: "user"},
			},
		}
		f2 := storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
			},
		}
		k1 := ReadUsersetTuplesKey("store1", f1)
		k2 := ReadUsersetTuplesKey("store1", f2)
		require.NotEqual(t, k1, k2)
	})

	t.Run("collision_delimiter_in_object", func(t *testing.T) {
		f1 := storage.ReadUsersetTuplesFilter{
			Object:   "doc:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
			},
		}
		f2 := storage.ReadUsersetTuplesFilter{
			Object:   "doc:1#viewer/group",
			Relation: "member",
		}
		k1 := ReadUsersetTuplesKey("store1", f1)
		k2 := ReadUsersetTuplesKey("store1", f2)
		require.NotEqual(t, k1, k2)
	})

	t.Run("wildcard_vs_relation", func(t *testing.T) {
		f1 := storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				{Type: "user", RelationOrWildcard: &openfgav1.RelationReference_Wildcard{}},
			},
		}
		f2 := storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				{Type: "user", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
			},
		}
		k1 := ReadUsersetTuplesKey("store1", f1)
		k2 := ReadUsersetTuplesKey("store1", f2)
		require.NotEqual(t, k1, k2)
	})
}

func TestReadKey(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tk := &openfgav1.TupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:alice",
		}
		key := ReadKey("store1", tk)
		require.NotEmpty(t, key)
	})

	t.Run("deterministic", func(t *testing.T) {
		tk := &openfgav1.TupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:alice",
		}
		k1 := ReadKey("store1", tk)
		k2 := ReadKey("store1", tk)
		require.Equal(t, k1, k2)
	})

	t.Run("different_stores_different_keys", func(t *testing.T) {
		tk := &openfgav1.TupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:alice",
		}
		k1 := ReadKey("store1", tk)
		k2 := ReadKey("store2", tk)
		require.NotEqual(t, k1, k2)
	})

	t.Run("different_tuples_different_keys", func(t *testing.T) {
		tk1 := &openfgav1.TupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:alice",
		}
		tk2 := &openfgav1.TupleKey{
			Object:   "document:1",
			Relation: "editor",
			User:     "user:alice",
		}
		k1 := ReadKey("store1", tk1)
		k2 := ReadKey("store1", tk2)
		require.NotEqual(t, k1, k2)
	})
}
