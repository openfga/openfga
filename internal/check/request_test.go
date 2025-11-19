package check

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func TestNewRequest(t *testing.T) {
	t.Run("returns_error_when_store_id_missing", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		})
		require.ErrorIs(t, err, ErrMissingStoreID)
	})

	t.Run("returns_error_when_authorization_model_id_missing", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			StoreID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		})
		require.ErrorIs(t, err, ErrMissingAuthZModelID)
	})

	t.Run("extracts_user_type_for_simple_user", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		})
		require.NoError(t, err)
		require.Equal(t, "user", req.GetUserType())
	})

	t.Run("extracts_user_type_for_object_relation", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "group:eng#member",
			},
		})
		require.NoError(t, err)
		require.Equal(t, "group#member", req.GetUserType())
	})

	t.Run("extracts_object_type", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		})
		require.NoError(t, err)
		require.Equal(t, "document", req.GetObjectType())
	})

	t.Run("generates_cache_key", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, req.GetCacheKey())
		require.Contains(t, req.GetCacheKey(), CacheKeyPrefix)
	})

	t.Run("generates_invariant_cache_key", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, req.GetInvariantCacheKey())
	})
}

func TestGetContextualTuplesByUserID(t *testing.T) {
	t.Run("returns_empty_when_no_matching_tuples", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:bob", Relation: "editor", Object: "document:2"},
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("user:alice", "viewer", "document")
		require.False(t, ok)
		require.Empty(t, result)
	})

	t.Run("returns_matching_tuples_sorted_by_object", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:alice", Relation: "viewer", Object: "document:3"},
				{User: "user:alice", Relation: "viewer", Object: "document:1"},
				{User: "user:alice", Relation: "viewer", Object: "document:2"},
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("user:alice", "viewer", "document")
		require.True(t, ok)
		require.Len(t, result, 3)
		require.Equal(t, "document:1", result[0].GetObject())
		require.Equal(t, "document:2", result[1].GetObject())
		require.Equal(t, "document:3", result[2].GetObject())
	})

	t.Run("handles_object_relation_user", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "group:eng#member", Relation: "viewer", Object: "document:1"},
				{User: "group:eng#member", Relation: "viewer", Object: "document:2"},
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("group:eng#member", "viewer", "document")
		require.True(t, ok)
		require.Len(t, result, 2)
	})

	t.Run("filters_by_relation", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:alice", Relation: "viewer", Object: "document:1"},
				{User: "user:alice", Relation: "editor", Object: "document:2"},
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("user:alice", "viewer", "document")
		require.True(t, ok)
		require.Len(t, result, 1)
		require.Equal(t, "viewer", result[0].GetRelation())
	})

	t.Run("filters_by_object_type", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:alice", Relation: "viewer", Object: "document:1"},
				{User: "user:alice", Relation: "viewer", Object: "folder:2"},
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("user:alice", "viewer", "document")
		require.True(t, ok)
		require.Len(t, result, 1)
		require.Equal(t, "document:1", result[0].GetObject())
	})
}

func TestGetContextualTuplesByObjectID(t *testing.T) {
	t.Run("returns_empty_when_no_matching_tuples", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:bob", Relation: "editor", Object: "document:2"},
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByObjectID("document:1", "viewer", "user")
		require.False(t, ok)
		require.Empty(t, result)
	})

	t.Run("returns_matching_tuples_sorted_by_user", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:charlie", Relation: "viewer", Object: "document:1"},
				{User: "user:alice", Relation: "viewer", Object: "document:1"},
				{User: "user:bob", Relation: "viewer", Object: "document:1"},
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByObjectID("document:1", "viewer", "user")
		require.True(t, ok)
		require.Len(t, result, 3)
		require.Equal(t, "user:alice", result[0].GetUser())
		require.Equal(t, "user:bob", result[1].GetUser())
		require.Equal(t, "user:charlie", result[2].GetUser())
	})

	t.Run("handles_object_relation_user_type", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "group:eng#member", Relation: "viewer", Object: "document:1"},
				{User: "group:sales#member", Relation: "viewer", Object: "document:1"},
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByObjectID("document:1", "viewer", "group#member")
		require.True(t, ok)
		require.Len(t, result, 2)
		require.Equal(t, "group:eng#member", result[0].GetUser())
		require.Equal(t, "group:sales#member", result[1].GetUser())
	})
}

func TestInsertSortedTuple(t *testing.T) {
	t.Run("inserts_into_empty_slice", func(t *testing.T) {
		tuple := &openfgav1.TupleKey{
			User:     "user:alice",
			Relation: "viewer",
			Object:   "document:1",
		}

		result := insertSortedTuple(nil, tuple, "object")
		require.Len(t, result, 1)
		require.Equal(t, tuple, result[0])
	})

	t.Run("maintains_sorted_order_by_object", func(t *testing.T) {
		tuple1 := &openfgav1.TupleKey{Object: "document:1"}
		tuple3 := &openfgav1.TupleKey{Object: "document:3"}
		tuple2 := &openfgav1.TupleKey{Object: "document:2"}

		slice := insertSortedTuple(nil, tuple1, "object")
		slice = insertSortedTuple(slice, tuple3, "object")
		slice = insertSortedTuple(slice, tuple2, "object")

		require.Len(t, slice, 3)
		require.Equal(t, "document:1", slice[0].GetObject())
		require.Equal(t, "document:2", slice[1].GetObject())
		require.Equal(t, "document:3", slice[2].GetObject())
	})

	t.Run("maintains_sorted_order_by_user", func(t *testing.T) {
		tuple1 := &openfgav1.TupleKey{User: "user:alice"}
		tuple3 := &openfgav1.TupleKey{User: "user:charlie"}
		tuple2 := &openfgav1.TupleKey{User: "user:bob"}

		slice := insertSortedTuple(nil, tuple1, "user")
		slice = insertSortedTuple(slice, tuple3, "user")
		slice = insertSortedTuple(slice, tuple2, "user")

		require.Len(t, slice, 3)
		require.Equal(t, "user:alice", slice[0].GetUser())
		require.Equal(t, "user:bob", slice[1].GetUser())
		require.Equal(t, "user:charlie", slice[2].GetUser())
	})

	t.Run("skips_duplicate_tuples", func(t *testing.T) {
		tuple1 := &openfgav1.TupleKey{Object: "document:1"}
		tuple2 := &openfgav1.TupleKey{Object: "document:1"}

		slice := insertSortedTuple(nil, tuple1, "object")
		slice = insertSortedTuple(slice, tuple2, "object")

		require.Len(t, slice, 1)
	})
}

func TestBuildContextualTupleMaps(t *testing.T) {
	t.Run("handles_empty_contextual_tuples", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		})
		require.NoError(t, err)

		req.buildContextualTupleMaps()
		require.Empty(t, req.ctxTuplesByUserID)
		require.Empty(t, req.ctxTuplesByObjectID)
	})

	t.Run("builds_both_maps_correctly", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:alice", Relation: "viewer", Object: "document:1"},
				{User: "user:bob", Relation: "editor", Object: "document:2"},
			},
		})
		require.NoError(t, err)

		require.NotEmpty(t, req.ctxTuplesByUserID)
		require.NotEmpty(t, req.ctxTuplesByObjectID)
	})

	t.Run("deduplicates_tuples", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:alice", Relation: "viewer", Object: "document:1"},
				{User: "user:alice", Relation: "viewer", Object: "document:1"},
				{User: "user:alice", Relation: "viewer", Object: "document:2"},
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("user:alice", "viewer", "document")
		require.True(t, ok)
		require.Len(t, result, 2)
	})
}

func TestCloneWithTupleKey(t *testing.T) {
	t.Run("clones_request_with_new_tuple_key", func(t *testing.T) {
		originalReq, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:bob", Relation: "editor", Object: "document:2"},
			},
		})
		require.NoError(t, err)

		newTupleKey := &openfgav1.TupleKey{
			Object:   "document:2",
			Relation: "editor",
			User:     "user:bob",
		}

		clonedReq := originalReq.cloneWithTupleKey(newTupleKey)

		require.Equal(t, originalReq.GetStoreID(), clonedReq.GetStoreID())
		require.Equal(t, originalReq.GetAuthorizationModelID(), clonedReq.GetAuthorizationModelID())
		require.Equal(t, newTupleKey, clonedReq.GetTupleKey())
		require.Equal(t, originalReq.GetContextualTuples(), clonedReq.GetContextualTuples())
		require.Equal(t, originalReq.GetContext(), clonedReq.GetContext())
		require.Equal(t, originalReq.GetConsistency(), clonedReq.GetConsistency())
		require.Equal(t, originalReq.GetInvariantCacheKey(), clonedReq.GetInvariantCacheKey())
	})

	t.Run("updates_object_type_based_on_new_tuple_key", func(t *testing.T) {
		originalReq, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		})
		require.NoError(t, err)

		newTupleKey := &openfgav1.TupleKey{
			Object:   "folder:2",
			Relation: "viewer",
			User:     "user:alice",
		}

		clonedReq := originalReq.cloneWithTupleKey(newTupleKey)
		require.Equal(t, "folder", clonedReq.GetObjectType())
	})

	t.Run("updates_user_type_for_object_relation", func(t *testing.T) {
		originalReq, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		})
		require.NoError(t, err)

		newTupleKey := &openfgav1.TupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "group:eng#member",
		}

		clonedReq := originalReq.cloneWithTupleKey(newTupleKey)
		require.Equal(t, "group#member", clonedReq.GetUserType())
	})

	t.Run("shares_contextual_tuple_maps", func(t *testing.T) {
		originalReq, err := NewRequest(RequestParams{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
			ContextualTuples: []*openfgav1.TupleKey{
				{User: "user:bob", Relation: "editor", Object: "document:2"},
			},
		})
		require.NoError(t, err)

		newTupleKey := &openfgav1.TupleKey{
			Object:   "document:2",
			Relation: "editor",
			User:     "user:bob",
		}

		clonedReq := originalReq.cloneWithTupleKey(newTupleKey)

		// Verify contextual tuple maps are shared (not copied)
		result, ok := clonedReq.GetContextualTuplesByUserID("user:bob", "editor", "document")
		require.True(t, ok)
		require.NotEmpty(t, result)
	})
}
