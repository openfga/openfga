package check

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func createTestModel(t *testing.T) *modelgraph.AuthorizationModelGraph {
	t.Helper()

	model := testutils.MustTransformDSLToProtoWithID(`
  model
   schema 1.1
  type user
  type group
   relations
    define member: [user]
  type document
   relations
    define viewer: [user, group#member]
    define editor: [user, group#member]
 `)

	graph, err := modelgraph.New(model)
	require.NoError(t, err)
	return graph
}

func TestNewRequest(t *testing.T) {
	t.Run("returns_error_when_store_id_missing", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.ErrorIs(t, err, ErrMissingStoreID)
	})

	t.Run("returns_error_when_model_missing", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.ErrorIs(t, err, ErrMissingAuthZModelID)
	})

	t.Run("returns_error_when_user_is_invalid", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "invalid"),
		})
		require.ErrorIs(t, err, ErrInvalidUser)
	})

	t.Run("returns_error_when_tuple_object_type_not_in_model", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("unknown:1", "viewer", "user:alice"),
		})
		require.ErrorIs(t, err, ErrValidation)
	})

	t.Run("returns_error_when_tuple_relation_not_in_model", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "unknown", "user:alice"),
		})
		require.ErrorIs(t, err, ErrValidation)
	})

	t.Run("returns_error_when_tuple_user_type_not_in_model", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "unknown:alice"),
		})
		require.ErrorIs(t, err, ErrInvalidUser)
	})

	t.Run("extracts_user_type_for_simple_user", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)
		require.Equal(t, "user", req.GetUserType())
	})

	t.Run("extracts_user_type_for_object_relation", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		})
		require.NoError(t, err)
		require.Equal(t, "group#member", req.GetUserType())
	})

	t.Run("extracts_object_type", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)
		require.Equal(t, "document", req.GetObjectType())
	})

	t.Run("generates_cache_key", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, req.GetCacheKey())
		require.Contains(t, req.GetCacheKey(), CacheKeyPrefix)
	})

	t.Run("generates_invariant_cache_key", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, req.GetInvariantCacheKey())
	})
}

func TestCloneWithTupleKey(t *testing.T) {
	t.Run("clones_request_with_new_tuple_key", func(t *testing.T) {
		originalReq, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)

		newTupleKey := tuple.NewTupleKey("document:2", "editor", "user:bob")
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
		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user]
   type folder
    relations
     define viewer: [user]
  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		originalReq, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)

		newTupleKey := tuple.NewTupleKey("folder:1", "viewer", "user:alice")
		clonedReq := originalReq.cloneWithTupleKey(newTupleKey)
		require.Equal(t, "folder", clonedReq.GetObjectType())
	})

	t.Run("updates_user_type_for_object_relation", func(t *testing.T) {
		originalReq, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)

		newTupleKey := tuple.NewTupleKey("document:1", "viewer", "group:eng#member")
		clonedReq := originalReq.cloneWithTupleKey(newTupleKey)
		require.Equal(t, "group#member", clonedReq.GetUserType())
	})

	t.Run("shares_contextual_tuple_maps", func(t *testing.T) {
		originalReq, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:2", "editor", "user:bob"),
			},
		})
		require.NoError(t, err)

		newTupleKey := tuple.NewTupleKey("document:3", "viewer", "user:charlie")
		clonedReq := originalReq.cloneWithTupleKey(newTupleKey)

		result, ok := clonedReq.GetContextualTuplesByUserID("user:bob", "editor", "document")
		require.True(t, ok)
		require.NotEmpty(t, result)
	})
}

func TestGetContextualTuplesByUserID(t *testing.T) {
	t.Run("returns_empty_when_no_matching_tuples", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("user:alice", "viewer", "document")
		require.False(t, ok)
		require.Empty(t, result)
	})

	t.Run("returns_matching_tuples_sorted_by_object", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:0", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:3", "viewer", "user:alice"),
				tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				tuple.NewTupleKey("document:2", "viewer", "user:alice"),
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
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:0", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:2", "viewer", "group:eng#member"),
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("group:eng#member", "viewer", "document")
		require.True(t, ok)
		require.Len(t, result, 2)
	})

	t.Run("filters_by_relation", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:0", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				tuple.NewTupleKey("document:2", "editor", "user:alice"),
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("user:alice", "viewer", "document")
		require.True(t, ok)
		require.Len(t, result, 1)
		require.Equal(t, "viewer", result[0].GetRelation())
	})

	t.Run("filters_by_object_type", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user]
   type folder
    relations
     define viewer: [user]
  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:0", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				tuple.NewTupleKey("folder:1", "viewer", "user:alice"),
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
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByObjectID("document:1", "viewer", "user")
		require.False(t, ok)
		require.Empty(t, result)
	})

	t.Run("returns_matching_tuples_sorted_by_user", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:0", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:charlie"),
				tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				tuple.NewTupleKey("document:1", "viewer", "user:bob"),
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
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:0", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:sales#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
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
		tuple := tuple.NewTupleKey("document:1", "viewer", "user:alice")

		result := insertSortedTuple(nil, tuple, "object")
		require.Len(t, result, 1)
		require.Equal(t, tuple, result[0])
	})

	t.Run("maintains_sorted_order_by_object", func(t *testing.T) {
		tuple1 := tuple.NewTupleKey("document:1", "viewer", "user:alice")
		tuple3 := tuple.NewTupleKey("document:3", "viewer", "user:alice")
		tuple2 := tuple.NewTupleKey("document:2", "viewer", "user:alice")

		slice := insertSortedTuple(nil, tuple1, "object")
		slice = insertSortedTuple(slice, tuple3, "object")
		slice = insertSortedTuple(slice, tuple2, "object")

		require.Len(t, slice, 3)
		require.Equal(t, "document:1", slice[0].GetObject())
		require.Equal(t, "document:2", slice[1].GetObject())
		require.Equal(t, "document:3", slice[2].GetObject())
	})

	t.Run("maintains_sorted_order_by_user", func(t *testing.T) {
		tuple1 := tuple.NewTupleKey("document:1", "viewer", "user:alice")
		tuple3 := tuple.NewTupleKey("document:1", "viewer", "user:charlie")
		tuple2 := tuple.NewTupleKey("document:1", "viewer", "user:bob")

		slice := insertSortedTuple(nil, tuple1, "user")
		slice = insertSortedTuple(slice, tuple3, "user")
		slice = insertSortedTuple(slice, tuple2, "user")

		require.Len(t, slice, 3)
		require.Equal(t, "user:alice", slice[0].GetUser())
		require.Equal(t, "user:bob", slice[1].GetUser())
		require.Equal(t, "user:charlie", slice[2].GetUser())
	})

	t.Run("skips_duplicate_tuples", func(t *testing.T) {
		tuple1 := tuple.NewTupleKey("document:1", "viewer", "user:alice")
		tuple2 := tuple.NewTupleKey("document:1", "viewer", "user:alice")

		slice := insertSortedTuple(nil, tuple1, "object")
		slice = insertSortedTuple(slice, tuple2, "object")

		require.Len(t, slice, 1)
	})
}

func TestBuildContextualTupleMaps(t *testing.T) {
	t.Run("handles_empty_contextual_tuples", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		})
		require.NoError(t, err)

		req.buildContextualTupleMaps()
		require.Empty(t, req.ctxTuplesByUserID)
		require.Empty(t, req.ctxTuplesByObjectID)
	})

	t.Run("builds_both_maps_correctly", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:0", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:bob"),
				tuple.NewTupleKey("document:2", "editor", "user:charlie"),
			},
		})
		require.NoError(t, err)

		require.NotEmpty(t, req.ctxTuplesByUserID)
		require.NotEmpty(t, req.ctxTuplesByObjectID)
	})

	t.Run("deduplicates_tuples", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:0", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				tuple.NewTupleKey("document:2", "viewer", "user:alice"),
				tuple.NewTupleKey("document:1", "viewer", "user:alice"),
			},
		})
		require.NoError(t, err)

		result, ok := req.GetContextualTuplesByUserID("user:alice", "viewer", "document")
		require.True(t, ok)
		require.Len(t, result, 2)
	})
}

func TestContextualTuplesValidation(t *testing.T) {
	t.Run("returns_error_when_contextual_tuple_object_type_not_in_model", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("unknown:1", "viewer", "user:bob"),
			},
		})
		require.Error(t, err)
		var tupleErr *tuple.InvalidTupleError
		require.ErrorAs(t, err, &tupleErr)
	})

	t.Run("returns_error_when_contextual_tuple_relation_not_in_model", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:2", "unknown", "user:bob"),
			},
		})
		require.Error(t, err)
		var tupleErr *tuple.InvalidTupleError
		require.ErrorAs(t, err, &tupleErr)
	})

	t.Run("returns_error_when_contextual_tuple_user_type_not_in_model", func(t *testing.T) {
		_, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:2", "viewer", "unknown:bob"),
			},
		})
		require.Error(t, err)
		var tupleErr *tuple.InvalidTupleError
		require.ErrorAs(t, err, &tupleErr)
	})

	t.Run("accepts_valid_contextual_tuples", func(t *testing.T) {
		req, err := NewRequest(RequestParams{
			StoreID:  ulid.Make().String(),
			Model:    createTestModel(t),
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
			ContextualTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:2", "viewer", "user:bob"),
				tuple.NewTupleKey("document:3", "editor", "group:eng#member"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, req)
	})
}
