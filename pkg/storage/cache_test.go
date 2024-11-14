package storage

import (
	"testing"
	"time"


	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

func TestInMemoryCache(t *testing.T) {
	cache := NewInMemoryLRUCache[string]()
	defer cache.Stop()

	t.Run("set_and_get", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		cache.Set("key", "value", 1*time.Second)
		result := cache.Get("key")
		require.Equal(t, "value", result)
	})

	t.Run("set_and_get_expired", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		cache.Set("key", "value", -1*time.Nanosecond)
		result := cache.Get("key")
		require.Equal(t, "", result)
	})

	t.Run("stop_multiple_times", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})

		cache.Stop()
		cache.Stop()
	})
}
func TestCheckCacheKeyDoNotOverlap(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	key1, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
	})
	require.NoError(t, err)

	key2, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
		ContextualTuples: []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		},
	})
	require.NoError(t, err)

	contextStruct, err := structpb.NewStruct(map[string]interface{}{
		"key1": true,
	})
	require.NoError(t, err)

	key3, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
		Context:              contextStruct,
	})
	require.NoError(t, err)

	// two Check request cache keys should not overlap if contextual tuples are
	// provided in one and not the other and/or if context is provided in one
	// and not the other
	require.NotEqual(t, key1, key2)
	require.NotEqual(t, key2, key3)
	require.NotEqual(t, key1, key3)
}

func TestCheckCacheKeyContextualTuplesOrdering(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	tuples1 := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
		tuple.NewTupleKey("document:2", "admin", "user:jon"),
	}

	tuples2 := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:2", "admin", "user:jon"),
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
	}

	tupleKey := tuple.NewTupleKey("document:x", "viewer", "user:jon")

	key1, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples1,
	})
	require.NoError(t, err)

	key2, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples2,
	})
	require.NoError(t, err)

	require.Equal(t, key1, key2)
}

func TestCheckCacheKeyContextualTuplesWithConditionsOrdering(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	tuples1 := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_condition", nil),
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_other_condition", nil),
	}

	tuples2 := []*openfgav1.TupleKey{
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_other_condition", nil),
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_condition", nil),
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
	}

	tupleKey := tuple.NewTupleKey("document:x", "viewer", "user:jon")

	key1, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples1,
	})
	require.NoError(t, err)

	key2, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples2,
	})
	require.NoError(t, err)

	require.Equal(t, key1, key2)
}

func TestCheckCacheKeyWithContext(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	struct1, err := structpb.NewStruct(map[string]interface{}{
		"key1": "foo",
		"key2": "bar",
	})
	require.NoError(t, err)

	struct2, err := structpb.NewStruct(map[string]interface{}{
		"key2": "bar",
		"key1": "foo",
	})
	require.NoError(t, err)

	struct3, err := structpb.NewStruct(map[string]interface{}{
		"key2": "x",
		"key1": "foo",
	})
	require.NoError(t, err)

	struct4, err := structpb.NewStruct(map[string]interface{}{
		"key2": "x",
		"key1": true,
	})
	require.NoError(t, err)

	key1, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct1,
	})
	require.NoError(t, err)

	key2, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct2,
	})
	require.NoError(t, err)

	key3, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct3,
	})
	require.NoError(t, err)

	key4, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct4,
	})
	require.NoError(t, err)

	require.Equal(t, key1, key2)
	require.NotEqual(t, key1, key3)
	require.NotEqual(t, key1, key4)
	require.NotEqual(t, key3, key4)
}

func BenchmarkCheckRequestCacheKey(b *testing.B) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	var err error

	for n := 0; n < b.N; n++ {
		_, err = GetCheckCacheKey(&CheckCacheKeyParams{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		})
		require.NoError(b, err)
	}
}

func BenchmarkCheckRequestCacheKeyWithContextualTuples(b *testing.B) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	var err error

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "viewer", "user:x"),
		tuple.NewTupleKey("document:y", "viewer", "user:y"),
		tuple.NewTupleKey("document:z", "viewer", "user:z"),
	}

	for n := 0; n < b.N; n++ {
		_, err = GetCheckCacheKey(&CheckCacheKeyParams{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			ContextualTuples:     tuples,
		})
		require.NoError(b, err)
	}
}

func BenchmarkCheckRequestCacheKeyWithContext(b *testing.B) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	var err error

	contextStruct, err := structpb.NewStruct(map[string]interface{}{
		"boolKey":   true,
		"stringKey": "hello",
		"numberKey": 1.2,
		"nullKey":   nil,
		"structKey": map[string]interface{}{
			"key1": "value1",
		},
		"listKey": []interface{}{"item1", "item2"},
	})
	require.NoError(b, err)

	for n := 0; n < b.N; n++ {
		_, err = GetCheckCacheKey(&CheckCacheKeyParams{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			Context:              contextStruct,
		})
		require.NoError(b, err)
	}
}
