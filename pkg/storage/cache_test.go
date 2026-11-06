package storage

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/tuple"
)

func testKey(s string) keys.Key {
	var b keys.Builder
	b.EncodeString(s)
	return b.Key()
}

func TestInMemoryCache(t *testing.T) {
	t.Run("set_and_get", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		k := testKey("key")
		cache.Set(k, "value", 1*time.Second)
		result := cache.Get(k)
		require.Equal(t, "value", result)
	})
	t.Run("set_and_get_more_than_one_year", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		k := testKey("key")
		cache.Set(k, "value", math.MaxInt64)
		result := cache.Get(k)
		require.Equal(t, "value", result)
	})
	t.Run("negative_ttl_ignored", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		k := testKey("key")
		cache.Set(k, "value", -2)
		result := cache.Get(k)
		require.NotEqual(t, "value", result)
	})

	t.Run("cache_item_count_unchanged_on_overwrite", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		k := testKey("key")

		// Ensure metric is zero before we do anything
		cacheItemCount.WithLabelValues(unspecifiedLabel).Set(float64(0))

		cache.Set(k, "value1", time.Second)

		// This .Wait() is needed as cache client.EstimatedSize() is eventually consistent
		// due it its use of a shared mutex with theine's maintenance routine
		cache.client.Wait()

		cache.Set(k, "value2", time.Second)
		before := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))
		cache.client.Wait()

		cache.Set(k, "value3", time.Second)
		after := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))

		// There should only be 1
		require.InDelta(t, 0, after, 1)

		// Should not have changed further
		require.InDelta(t, 0, after-before, 0)
	})

	t.Run("cache_item_count_decrements_after_deletes", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()

		for i := range 10 {
			cache.Set(testKey(strconv.Itoa(i)), "value"+strconv.Itoa(i), time.Second)
		}
		cache.client.Wait()

		cache.Set(testKey("10"), "value10", time.Second)

		before := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))

		for i := range 11 {
			cache.Delete(testKey(strconv.Itoa(i)))
		}
		cache.client.Wait()

		// The cache item count is only updated on Set()
		cache.Set(testKey("10"), "value10", time.Second)

		after := testutil.ToFloat64(cacheItemCount.WithLabelValues(unspecifiedLabel))

		// expect before and after to differ by 9 elements, + or - 2 due to race
		require.InDelta(t, 9, before-after, 2)
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

func TestJitteredTTL(t *testing.T) {
	tests := []struct {
		name             string
		baseTTL          time.Duration
		jitterPercentage uint32
		expectedMin      time.Duration
		expectedMax      time.Duration
	}{
		{
			name:             "zero_jitter_returns_base_ttl",
			baseTTL:          10 * time.Second,
			jitterPercentage: 0,
			expectedMin:      10 * time.Second,
			expectedMax:      10 * time.Second,
		},
		{
			name:             "zero_base_ttl_returns_zero",
			baseTTL:          0,
			jitterPercentage: 10,
			expectedMin:      0,
			expectedMax:      0,
		},
		{
			name:             "negative_base_ttl_returns_base",
			baseTTL:          -5 * time.Second,
			jitterPercentage: 10,
			expectedMin:      -5 * time.Second,
			expectedMax:      -5 * time.Second,
		},
		{
			name:             "ten_percent_jitter",
			baseTTL:          10 * time.Second,
			jitterPercentage: 10,
			expectedMin:      10 * time.Second,
			expectedMax:      11 * time.Second,
		},
		{
			name:             "hundred_percent_jitter",
			baseTTL:          10 * time.Second,
			jitterPercentage: 100,
			expectedMin:      10 * time.Second,
			expectedMax:      20 * time.Second,
		},
		{
			name:             "jitter_percentage_over_hundred_is_capped",
			baseTTL:          10 * time.Second,
			jitterPercentage: 150,
			expectedMin:      10 * time.Second,
			expectedMax:      20 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				result := JitteredTTL(tt.baseTTL, tt.jitterPercentage)
				require.GreaterOrEqual(t, result, tt.expectedMin, "result %v is less than minimum %v", result, tt.expectedMin)
				require.LessOrEqual(t, result, tt.expectedMax, "result %v is greater than maximum %v", result, tt.expectedMax)
			}
		})
	}

	t.Run("produces_variation", func(t *testing.T) {
		baseTTL := 10 * time.Second
		jitterPct := uint32(50)
		results := make(map[time.Duration]struct{})
		for i := 0; i < 1000; i++ {
			results[JitteredTTL(baseTTL, jitterPct)] = struct{}{}
		}
		require.Greater(t, len(results), 1, "expected variation in jittered TTL values")
	})

	t.Run("does_not_overflow_for_max_duration", func(t *testing.T) {
		var result time.Duration
		require.NotPanics(t, func() {
			result = JitteredTTL(time.Duration(math.MaxInt64), 100)
		})
		require.Equal(t, time.Duration(math.MaxInt64), result)
	})
}

// BenchmarkInvariantCacheKeyWithContextualTuples measures the cost of computing
// the invariant cache key for a typical contextual-tuples payload. The
// invariant key is computed once per check request, so this is the per-request
// cost of the invariant section.
func BenchmarkInvariantCacheKeyWithContextualTuples(b *testing.B) {
	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "viewer", "user:x"),
		tuple.NewTupleKey("document:y", "viewer", "user:y"),
		tuple.NewTupleKey("document:z", "viewer", "user:z"),
	}

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	for n := 0; n < b.N; n++ {
		_ = InvariantCacheKey(storeID, modelID, nil, tuples...)
	}
}

// BenchmarkInvariantCacheKeyWithContext measures the cost of computing the
// invariant cache key for a representative request context.
func BenchmarkInvariantCacheKeyWithContext(b *testing.B) {
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

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	for n := 0; n < b.N; n++ {
		_ = InvariantCacheKey(storeID, modelID, contextStruct)
	}
}

func BenchmarkInvalidIteratorByUserObjectTypeCacheKey(b *testing.B) {
	storeID := "abc123"
	objectType := "document"

	for n := 0; n < b.N; n++ {
		_ = InvalidIteratorByUserObjectTypeCacheKey(storeID, "user_x", objectType)
	}
}
