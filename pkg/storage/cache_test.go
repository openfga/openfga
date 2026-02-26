package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/tuple"
)

// MustNewStruct returns a new *structpb.Struct or panics
// on error. The new *structpb.Struct value is built from
// the map m.
func MustNewStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err == nil {
		return s
	}
	panic(err)
}

// ResetableStringWriter is an interface that groups the
// io.StringWriter and fmt.Stringer interfaces with a
// Reset method.
type ResetableStringWriter interface {
	io.StringWriter
	fmt.Stringer
	Reset()
}

// ErrWriteString is an error used exclusively for testing
// Write functions.
var ErrWriteString = errors.New("test error")

// An ErrorStringWriter counts the calls made to its WriteString
// method and returns an error when the number of calls is
// greater than or equal to TriggerAt.
type ErrorStringWriter struct {
	TriggerAt int // number of calls to WriteString before returning an error
	current   int // the number of calls to WriteString up to this point
}

// WriteString ignores string s and returns the length of string s
// in bytes with a nil error. When the number of calls to WriteString
// is greater than or equal to TriggerAt WriteString will return
// 0 bytes and an error.
func (e *ErrorStringWriter) WriteString(s string) (int, error) {
	e.current++
	if e.current >= e.TriggerAt {
		return 0, ErrWriteString
	}
	return len([]byte(s)), nil
}

// Reset sets the number of calls made to WriteString up to this
// point, to 0.
func (e *ErrorStringWriter) Reset() {
	e.current = 0
}

// String always returns an empty string value.
func (e *ErrorStringWriter) String() string {
	return ""
}

var validWriter strings.Builder // A global string builder intended for reuse across tests

func TestWriteValue(t *testing.T) {
	var cases = map[string]struct {
		writer ResetableStringWriter
		value  *structpb.Value
		output string
		error  bool
	}{
		"list": {
			writer: &validWriter,
			value: structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("A"),
					structpb.NewNullValue(),
					structpb.NewBoolValue(true),
					structpb.NewNumberValue(1111111111),
					structpb.NewStructValue(MustNewStruct(map[string]any{
						"key": "value",
					})),
				},
			}),
			output: "A,null,true,1111111111,'key:'value,",
		},
		"list_write_value_error": {
			writer: &ErrorStringWriter{},
			value: structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("A"),
					structpb.NewNullValue(),
					structpb.NewBoolValue(true),
					structpb.NewNumberValue(1111111111),
					structpb.NewStructValue(MustNewStruct(map[string]any{
						"key": "value",
					})),
				},
			}),
			error: true,
		},
		"list_write_comma_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 2,
			},
			value: structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewNullValue(),
					structpb.NewNullValue(),
				},
			}),
			error: true,
		},
		"nil": {
			writer: &validWriter,
			value:  nil,
			error:  true,
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			test.writer.Reset()
			err := writeValue(test.writer, test.value)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, test.writer.String())
			}
		})
	}
}

func TestWriteStruct(t *testing.T) {
	var cases = map[string]struct {
		writer ResetableStringWriter
		value  *structpb.Struct
		output string
		error  bool
	}{
		"general": {
			writer: &validWriter,
			value: MustNewStruct(map[string]any{
				"keyA": "valueA",
				"keyB": "valueB",
			}),
			output: "'keyA:'valueA,'keyB:'valueB,",
		},
		"fields_write_key_error": {
			writer: &ErrorStringWriter{},
			value: MustNewStruct(map[string]any{
				"keyA": "valueA",
				"keyB": "valueB",
			}),
			error: true,
		},
		"fields_write_value_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 2,
			},
			value: MustNewStruct(map[string]any{
				"keyA": "valueA",
				"keyB": "valueB",
			}),
			error: true,
		},
		"fields_write_comma_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 3,
			},
			value: MustNewStruct(map[string]any{
				"keyA": "valueA",
				"keyB": "valueB",
			}),
			error: true,
		},
		"nil": {
			writer: &validWriter,
			value:  nil,
			output: "",
		},
		"nil_error": {
			writer: &ErrorStringWriter{},
			value:  nil,
			output: "",
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			test.writer.Reset()
			err := writeStruct(test.writer, test.value)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, test.writer.String())
			}
		})
	}
}

func TestWriteTuples(t *testing.T) {
	var cases = map[string]struct {
		writer ResetableStringWriter
		tuples []*openfgav1.TupleKey
		output string
		error  bool
	}{
		"sans_condition": {
			writer: &validWriter,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:C", "relationC", "user:C"),
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
				tuple.NewTupleKey("document:B", "relationB", "user:B"),
			},
			output: "/document:A#relationA@user:A,document:B#relationB@user:B,document:C#relationC@user:C",
		},
		"write_forward_slash_error": {
			writer: &ErrorStringWriter{},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
			},
			error: true,
		},
		"write_object_relation_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 2,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
			},
			error: true,
		},
		"write_user_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 3,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
			},
			error: true,
		},
		"write_comma_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 4,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
				tuple.NewTupleKey("document:B", "relationB", "user:B"),
			},
			error: true,
		},
		"with_condition": {
			writer: &validWriter,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"B",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"A",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
			},
			output: "/document:A#relationA with A 'key:'value,@user:A,document:A#relationA with B 'key:'value,@user:A",
		},
		"with_condition_write_with_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 3,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"B",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"A",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
			},
			error: true,
		},
		"with_condition_write_space_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 4,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"B",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"A",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
			},
			error: true,
		},
		"with_condition_write_context_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 5,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"B",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"A",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
			},
			error: true,
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			test.writer.Reset()
			err := writeTuples(test.writer, test.tuples...)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, test.writer.String())
			}
		})
	}
}

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
	t.Run("set_and_get_more_than_one_year", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		cache.Set("key", "value", math.MaxInt64)
		result := cache.Get("key")
		require.Equal(t, "value", result)
	})
	t.Run("negative_ttl_ignored", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		cache.Set("key", "value", -2)
		result := cache.Get("key")
		require.NotEqual(t, "value", result)
	})

	t.Run("cache_item_count_increments_on_new_key", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()

		before := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))
		cache.Set("key", "value", time.Second)
		after := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))
		require.InDelta(t, 1, after-before, 0)
	})

	t.Run("cache_item_count_doesnt_double_count_on_overwrite", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		k := "key"

		cache.Set(k, "value1", time.Second)
		before := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))

		cache.Set(k, "value2", time.Second)
		after := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))

		// Should not have changed
		require.InDelta(t, 0, after-before, 0)
	})

	t.Run("cache_item_count_decrements_on_delete", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()

		cache.Set("key", "value", time.Second)
		before := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))
		cache.Delete("key")
		cache.client.Wait()
		after := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))
		require.InDelta(t, -1, after-before, 0)
	})

	t.Run("cache_item_count_set_delete_set_same_key", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		k := "key"

		before := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))
		cache.Set(k, "value1", time.Second)
		cache.Delete(k)
		cache.client.Wait()

		cache.Set(k, "value2", time.Second)
		after := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))
		require.InDelta(t, 1, after-before, 0)
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

func MustGetCheckCacheKey(params *CheckCacheKeyParams) string {
	w := &strings.Builder{}

	err := WriteCheckCacheKey(w, params)
	if err == nil {
		return w.String()
	}

	panic(err)
}

func TestCheckCacheKeyDoNotOverlap(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	key1 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
	})

	key2 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
		ContextualTuples: []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		},
	})

	contextStruct, err := structpb.NewStruct(map[string]interface{}{
		"key1": true,
	})
	require.NoError(t, err)

	key3 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
		Context:              contextStruct,
	})

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
	key1 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     contextualTuples,
	})

	// does not have contextual tuples
	key2 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     nil,
	})

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

	key1 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples1,
	})

	key2 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples2,
	})

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

	key1 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples1,
	})

	key2 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples2,
	})

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

	key1 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{jonContextOne},
	})

	key2 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{jonContextTwo},
	})

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

	key1 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{jonContextOne, jonContextTwo},
	})

	key2 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{jonContextTwo, jonContextOne},
	})

	require.Equal(t, key1, key2)
}

func TestCheckCacheKeyContextualTuplesConditionsOrderDoesNotMatter(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	anne := tuple.NewTupleKey("document:1", "viewer", "user:anne")
	jonCondOne := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_condition", nil)
	jonCondTwo := tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_other_condition", nil)

	tupleKey := tuple.NewTupleKey("document:x", "viewer", "user:jon")

	key1 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     []*openfgav1.TupleKey{anne, jonCondOne, jonCondTwo},
	})

	key2 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,

		// same tuples, conditions are in different order
		ContextualTuples: []*openfgav1.TupleKey{anne, jonCondTwo, jonCondOne},
	})

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

	key1 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct1,
	})

	key2 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct2,
	})

	key3 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct3,
	})

	key4 := MustGetCheckCacheKey(&CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct4,
	})

	require.Equal(t, key1, key2)
	require.NotEqual(t, key1, key3)
	require.NotEqual(t, key1, key4)
	require.NotEqual(t, key3, key4)
}

func TestWriteInvariantCheckCacheKey(t *testing.T) {
	contextStruct, err := structpb.NewStruct(map[string]interface{}{"key1": true})
	require.NoError(t, err)

	var validWriter strings.Builder
	var cases = map[string]struct {
		writer ResetableStringWriter
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
			output: "fake_model_id/document:1#viewer with condition_name 'key1:'true,@user:anne'key1:'true,",
			error:  false,
		},
		"writer_error": {
			writer: &ErrorStringWriter{TriggerAt: 0},
			params: &CheckCacheKeyParams{},
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

func TestWriteCheckCacheKey(t *testing.T) {
	contextStruct, err := structpb.NewStruct(map[string]interface{}{"key1": true})
	require.NoError(t, err)

	var cases = map[string]struct {
		writer ResetableStringWriter
		params *CheckCacheKeyParams
		output string
		error  bool
	}{
		"errors_if_first_write_fails": {
			writer: &ErrorStringWriter{TriggerAt: 0},
			params: &CheckCacheKeyParams{},
			error:  true,
		},
		"errors_if_second_write_fails": {
			writer: &ErrorStringWriter{TriggerAt: 1},
			params: &CheckCacheKeyParams{},
			error:  true,
		},
		"writes_full_cache_key": {
			writer: &strings.Builder{},
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
			output: "document:1#can_view@user:annefake_model_id/document:1#viewer with condition_name 'key1:'true,@user:anne'key1:'true,",
		},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			err := WriteCheckCacheKey(test.writer, test.params)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, test.writer.String())
			}
		})
	}
}

func BenchmarkWriteCheckCacheKey(b *testing.B) {
	var err error
	writer := &strings.Builder{}

	// The TupleKey portion of the cache key is calculated on every invocation of check,
	// potentially multiple times per request.
	params := &CheckCacheKeyParams{
		TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
	}

	for n := 0; n < b.N; n++ {
		err = WriteCheckCacheKey(writer, params)
		require.NoError(b, err)
		writer.Reset()
	}
}

// The invariant cache key is calculated once per check request. Any sub-problems re-use this portion of the key.
func BenchmarkInvariantCacheKeyWithContextualTuples(b *testing.B) {
	var err error
	writer := &strings.Builder{}

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "viewer", "user:x"),
		tuple.NewTupleKey("document:y", "viewer", "user:y"),
		tuple.NewTupleKey("document:z", "viewer", "user:z"),
	}

	require.NoError(b, err)

	params := &CheckCacheKeyParams{
		AuthorizationModelID: ulid.Make().String(),
		StoreID:              ulid.Make().String(),
		ContextualTuples:     tuples,
	}

	for n := 0; n < b.N; n++ {
		err = WriteInvariantCheckCacheKey(writer, params)
		require.NoError(b, err)
		writer.Reset()
	}
}

// The invariant cache key is calculated once per check request. Any sub-problems re-use this portion of the key.
func BenchmarkInvariantCacheKeyWithContext(b *testing.B) {
	var err error
	writer := &strings.Builder{}

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

	params := &CheckCacheKeyParams{
		AuthorizationModelID: ulid.Make().String(),
		StoreID:              ulid.Make().String(),
		Context:              contextStruct,
	}

	for n := 0; n < b.N; n++ {
		err = WriteInvariantCheckCacheKey(writer, params)
		require.NoError(b, err)
		writer.Reset()
	}
}

func BenchmarkGetInvalidIteratorByUserObjectTypeCacheKeys(b *testing.B) {
	storeID := "abc123"
	objectType := "document"
	users := []string{
		"a",
		"b",
		"c",
		"d",
		"e",
		"f",
		"g",
		"h",
		"i",
		"j",
		"k",
		"l",
		"m",
		"n",
		"o",
		"p",
		"q",
		"r",
		"s",
		"t",
		"u",
		"v",
		"w",
		"x",
		"y",
		"z",
	}

	for n := 0; n < b.N; n++ {
		_ = GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, users, objectType)
	}
}
