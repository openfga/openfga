package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestInMemoryCache(t *testing.T) {
	t.Run("set_and_get", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		cache.Set("key", "value", 1*time.Second)
		result := cache.Get("key")
		require.Equal(t, "value", result)
	})

	t.Run("stop_multiple_times", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})

		cache.Stop()
		cache.Stop()
	})

	t.Run("stop_concurrently", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})

		pool := concurrency.NewPool(context.Background(), 2)
		pool.Go(func(ctx context.Context) error {
			cache.Stop()
			return nil
		})
		pool.Go(func(ctx context.Context) error {
			cache.Stop()
			return nil
		})
		err = pool.Wait()
		require.NoError(t, err)
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

func TestCheckCacheKeyConsidersContextualTuples(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	contextualTuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
	}

	tupleKey := tuple.NewTupleKey("document:x", "viewer", "user:jon")

	// has contextual tuples
	key1, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     contextualTuples,
	})
	require.NoError(t, err)

	// does not have contextual tuples
	key2, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     nil,
	})
	require.NoError(t, err)

	require.NotEqual(t, key1, key2)
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

func TestCheckCacheKeyConsidersCondition(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	tuples1 := []*openfgav1.TupleKey{
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_condition", nil),
	}

	tuples2 := []*openfgav1.TupleKey{
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_other_condition", nil),
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

	require.NotEqual(t, key1, key2)
}

func TestCheckCacheKeyConsidersConditionContext(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	struct1, err := structpb.NewStruct(map[string]interface{}{
		"key1": "foo",
		"key2": "bar",
	})
	require.NoError(t, err)

	struct2, err := structpb.NewStruct(map[string]interface{}{
		"key1": "foo",
		"key3": "baz",
	})
	require.NoError(t, err)

	jonContextOne := tuple.NewTupleKeyWithCondition(
		"document:2",
		"admin",
		"user:jon",
		"some_condition",
		struct1,
	)
	jonContextTwo := tuple.NewTupleKeyWithCondition(
		"document:2",
		"admin",
		"user:jon",
		"some_condition",
		struct2,
	)

	tupleKey := tuple.NewTupleKey("document:x", "viewer", "user:jon")

	key1, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{jonContextOne},
	})
	require.NoError(t, err)

	key2, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{jonContextTwo},
	})
	require.NoError(t, err)

	require.NotEqual(t, key1, key2)
}

func TestCheckCacheKeyConditionContextOrderAgnostic(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	struct1, err := structpb.NewStruct(map[string]interface{}{
		"key1": "foo",
		"key2": "bar",
	})
	require.NoError(t, err)

	// same object, keys in different order
	struct2, err := structpb.NewStruct(map[string]interface{}{
		"key2": "bar",
		"key1": "foo",
	})
	require.NoError(t, err)

	jonContextOne := tuple.NewTupleKeyWithCondition(
		"document:2",
		"admin",
		"user:jon",
		"some_condition",
		struct1,
	)
	jonContextTwo := tuple.NewTupleKeyWithCondition(
		"document:2",
		"admin",
		"user:jon",
		"some_condition",
		struct2,
	)

	tupleKey := tuple.NewTupleKey("document:x", "viewer", "user:jon")

	key1, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{jonContextOne, jonContextTwo},
	})
	require.NoError(t, err)

	key2, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{jonContextTwo, jonContextOne},
	})
	require.NoError(t, err)

	require.Equal(t, key1, key2)
}

func TestCheckCacheKeyContextualTuplesConditionsOrderDoesNotMatter(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	anne := tuple.NewTupleKey("document:1", "viewer", "user:anne")
	jonCondOne := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_condition", nil)
	jonCondTwo := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_other_condition", nil)

	tupleKey := tuple.NewTupleKey("document:x", "viewer", "user:jon")

	key1, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{anne, jonCondOne, jonCondTwo},
	})
	require.NoError(t, err)

	key2, err := GetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,

		// same tuples, conditions are in different order
		ContextualTuples: []*openfgav1.TupleKey{anne, jonCondTwo, jonCondOne},
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

func TestWriteTupleCheckCacheKey(t *testing.T) {
	contextStruct, err := structpb.NewStruct(map[string]interface{}{"key1": true})
	require.NoError(t, err)

	var validWriter strings.Builder
	var cases = map[string]struct {
		writer testutils.ResetableStringWriter
		params *CheckCacheKeyParams
		output string
		error  bool
	}{
		"writes_cache_key": {
			writer: &validWriter,
			params: &CheckCacheKeyParams{
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:1",
					Relation: "can_view",
					User:     "user:anne",
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:anne", "condition_name", contextStruct),
				},
			},
			output: "document:1#can_view@user:anne",
			error:  false,
		},
		"writer_error": {
			writer: &testutils.ErrorStringWriter{TriggerAt: 0},
			params: &CheckCacheKeyParams{},
			output: "",
			error:  true,
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			err := WriteTupleCheckCacheKey(test.writer, test.params)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, test.writer.String())
			}
		})
	}
}

func TestWriteInvariantCheckCacheKey(t *testing.T) {
	contextStruct, err := structpb.NewStruct(map[string]interface{}{"key1": true})
	require.NoError(t, err)

	var validWriter strings.Builder
	var cases = map[string]struct {
		writer testutils.ResetableStringWriter
		params *CheckCacheKeyParams
		output string
		error  bool
	}{
		"writes_cache_key": {
			writer: &validWriter,
			params: &CheckCacheKeyParams{
				AuthorizationModelID: "fake_model_id",
				StoreID:              "fake_store_id",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:1",
					Relation: "can_view",
					User:     "user:anne",
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:anne", "condition_name", contextStruct),
				},
				Context: contextStruct,
			},
			output: " sp.fake_store_id/fake_model_id/document:1#viewer with condition_name 'key1:'true,@user:anne'key1:'true,",
			error:  false,
		},
		"writer_error": {
			writer: &testutils.ErrorStringWriter{TriggerAt: 0},
			params: &CheckCacheKeyParams{},
			output: "",
			error:  true,
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			err := WriteInvariantCheckCacheKey(test.writer, test.params)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, test.writer.String())
			}
		})
	}
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
