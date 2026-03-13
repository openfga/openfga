package check

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

func newTestCache(t *testing.T) storage.InMemoryCache[any] {
	t.Helper()
	cache, err := storage.NewInMemoryLRUCache[any]()
	require.NoError(t, err)
	t.Cleanup(cache.Stop)
	return cache
}

func TestBuildUsersetTuplesCacheKey(t *testing.T) {
	tests := []struct {
		name       string
		store      string
		object     string
		relation   string
		conditions []string
		want       string
	}{
		{
			name:       "no_conditions",
			store:      "store1",
			object:     "document:1",
			relation:   "viewer",
			conditions: nil,
			want:       "cic/rut/store1/document:1#viewer",
		},
		{
			name:       "single_condition",
			store:      "store1",
			object:     "document:1",
			relation:   "viewer",
			conditions: []string{"cond_a"},
			want:       "cic/rut/store1/document:1#viewer/cond_a",
		},
		{
			name:       "multiple_conditions_sorted",
			store:      "store1",
			object:     "document:1",
			relation:   "viewer",
			conditions: []string{"cond_b", "cond_a", "cond_c"},
			want:       "cic/rut/store1/document:1#viewer/cond_a,cond_b,cond_c",
		},
		{
			name:       "empty_conditions_slice",
			store:      "store1",
			object:     "document:1",
			relation:   "viewer",
			conditions: []string{},
			want:       "cic/rut/store1/document:1#viewer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildUsersetTuplesCacheKey(tt.store, tt.object, tt.relation, tt.conditions)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBuildReadCacheKey(t *testing.T) {
	tests := []struct {
		name       string
		store      string
		object     string
		relation   string
		userPrefix string
		conditions []string
		want       string
	}{
		{
			name:       "no_conditions",
			store:      "store1",
			object:     "document:1",
			relation:   "parent",
			userPrefix: "folder:",
			conditions: nil,
			want:       "cic/r/store1/document:1#parent/folder:",
		},
		{
			name:       "with_conditions",
			store:      "store1",
			object:     "document:1",
			relation:   "parent",
			userPrefix: "folder:",
			conditions: []string{"cond_x"},
			want:       "cic/r/store1/document:1#parent/folder:/cond_x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildReadCacheKey(tt.store, tt.object, tt.relation, tt.userPrefix, tt.conditions)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBuildRSWUCacheKey(t *testing.T) {
	tests := []struct {
		name       string
		store      string
		objectType string
		relation   string
		user       string
		conditions []string
		want       string
	}{
		{
			name:       "no_conditions",
			store:      "store1",
			objectType: "document",
			relation:   "viewer",
			user:       "user:alice",
			conditions: nil,
			want:       "cic/rswu/store1/document#viewer/user:alice",
		},
		{
			name:       "with_conditions",
			store:      "store1",
			objectType: "document",
			relation:   "viewer",
			user:       "user:alice",
			conditions: []string{"cond_y", "cond_x"},
			want:       "cic/rswu/store1/document#viewer/user:alice/cond_x,cond_y",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildRSWUCacheKey(tt.store, tt.objectType, tt.relation, tt.user, tt.conditions)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapFunctions(t *testing.T) {
	t.Run("MapUser", func(t *testing.T) {
		tk := &openfgav1.TupleKey{User: "user:alice"}
		require.Equal(t, "user:alice", MapUser(tk))
	})

	t.Run("MapUserObjectID", func(t *testing.T) {
		tk := &openfgav1.TupleKey{User: "folder:123"}
		require.Equal(t, "123", MapUserObjectID(tk))
	})

	t.Run("MapObjectID", func(t *testing.T) {
		tk := &openfgav1.TupleKey{Object: "document:456"}
		require.Equal(t, "456", MapObjectID(tk))
	})
}

func TestWrapIterator_CacheDisabled(t *testing.T) {
	ctx := context.Background()
	tuples := []*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "user:1"}},
		{Key: &openfgav1.TupleKey{User: "user:2"}},
	}
	inner := storage.NewStaticTupleIterator(tuples)

	// No cache - returns original iterator
	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    nil,
		CacheKey: "key",
		MapFunc:  MapUser,
	})

	// Should be the same iterator
	tuple, err := wrapped.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:1", tuple.GetKey().GetUser())

	wrapped.Stop()
}

func TestWrapIterator_CacheHit(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t)
	cacheKey := "test-key"

	// Pre-populate cache
	cache.Set(cacheKey, &CheckIteratorCacheEntry{Values: []string{"cached:1", "cached:2"}}, time.Hour)

	// Create iterator that should NOT be used
	inner := storage.NewStaticTupleIterator([]*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "original:1"}},
	})

	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: cacheKey,
		MapFunc:  MapUser,
	})

	// Should return cached values
	tuple, err := wrapped.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "cached:1", tuple.GetKey().GetUser())

	tuple, err = wrapped.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "cached:2", tuple.GetKey().GetUser())

	_, err = wrapped.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)

	wrapped.Stop()
}

func TestCachingIterator_FullConsumption(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t)
	cacheKey := "test-full-consumption"

	tuples := []*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "user:1"}},
		{Key: &openfgav1.TupleKey{User: "user:2"}},
		{Key: &openfgav1.TupleKey{User: "user:3"}},
	}
	inner := storage.NewStaticTupleIterator(tuples)

	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: cacheKey,
		TTL:      time.Hour,
		MaxSize:  100,
		MapFunc:  MapUser,
	})

	// Consume all tuples
	var results []string
	for {
		tuple, err := wrapped.Next(ctx)
		if err != nil {
			require.ErrorIs(t, err, storage.ErrIteratorDone)
			break
		}
		results = append(results, tuple.GetKey().GetUser())
	}

	require.Equal(t, []string{"user:1", "user:2", "user:3"}, results)

	wrapped.Stop()

	// Verify cached
	entry := cache.Get(cacheKey)
	require.NotNil(t, entry)
	cached, ok := entry.(*CheckIteratorCacheEntry)
	require.True(t, ok)
	require.Equal(t, []string{"user:1", "user:2", "user:3"}, cached.Values)
}

func TestCachingIterator_EarlyStopWithSyncDrain(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t)
	cacheKey := "test-early-stop-sync"

	tuples := []*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "user:1"}},
		{Key: &openfgav1.TupleKey{User: "user:2"}},
		{Key: &openfgav1.TupleKey{User: "user:3"}},
		{Key: &openfgav1.TupleKey{User: "user:4"}},
	}
	inner := storage.NewStaticTupleIterator(tuples)

	// No drainCtx/sf - will drain synchronously
	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: cacheKey,
		TTL:      time.Hour,
		MaxSize:  100,
		MapFunc:  MapUser,
		DrainCtx: nil,
		SF:       nil,
	})

	// Read only first tuple
	tuple, err := wrapped.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:1", tuple.GetKey().GetUser())

	// Stop early - should drain synchronously
	wrapped.Stop()

	// Verify all tuples cached
	entry := cache.Get(cacheKey)
	require.NotNil(t, entry)
	cached, ok := entry.(*CheckIteratorCacheEntry)
	require.True(t, ok)
	require.Equal(t, []string{"user:1", "user:2", "user:3", "user:4"}, cached.Values)
}

func TestCachingIterator_EarlyStopWithAsyncDrain(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t)
	cacheKey := "test-early-stop-async"
	sf := &singleflight.Group{}
	drainCtx := context.Background()

	tuples := []*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "user:1"}},
		{Key: &openfgav1.TupleKey{User: "user:2"}},
		{Key: &openfgav1.TupleKey{User: "user:3"}},
	}
	inner := storage.NewStaticTupleIterator(tuples)

	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: cacheKey,
		TTL:      time.Hour,
		MaxSize:  100,
		MapFunc:  MapUser,
		DrainCtx: drainCtx,
		SF:       sf,
	})

	// Read only first tuple
	_, err := wrapped.Next(ctx)
	require.NoError(t, err)

	// Stop early - should drain async
	wrapped.Stop()

	// Wait for async drain
	require.Eventually(t, func() bool {
		return cache.Get(cacheKey) != nil
	}, time.Second, 10*time.Millisecond)

	// Verify cached
	entry := cache.Get(cacheKey)
	require.NotNil(t, entry)
	cached, ok := entry.(*CheckIteratorCacheEntry)
	require.True(t, ok)
	require.Equal(t, []string{"user:1", "user:2", "user:3"}, cached.Values)
}

func TestCachingIterator_MaxSizeExceeded(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t)
	cacheKey := "test-max-size"

	tuples := []*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "user:1"}},
		{Key: &openfgav1.TupleKey{User: "user:2"}},
		{Key: &openfgav1.TupleKey{User: "user:3"}},
	}
	inner := storage.NewStaticTupleIterator(tuples)

	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: cacheKey,
		TTL:      time.Hour,
		MaxSize:  2, // Only allow 2
		MapFunc:  MapUser,
	})

	// Consume all
	for {
		_, err := wrapped.Next(ctx)
		if err != nil {
			break
		}
	}

	wrapped.Stop()

	// Should NOT be cached (exceeded max)
	require.Nil(t, cache.Get(cacheKey))
}

func TestCachingIterator_AlreadyCached(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t)
	cacheKey := "test-already-cached"
	sf := &singleflight.Group{}

	// Pre-populate cache
	cache.Set(cacheKey, &CheckIteratorCacheEntry{Values: []string{"existing:1"}}, time.Hour)

	tuples := []*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "user:1"}},
		{Key: &openfgav1.TupleKey{User: "user:2"}},
	}
	inner := storage.NewStaticTupleIterator(tuples)

	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: cacheKey,
		TTL:      time.Hour,
		MaxSize:  100,
		MapFunc:  MapUser,
		DrainCtx: ctx,
		SF:       sf,
	})

	// Should return cached values (not original iterator)
	tuple, err := wrapped.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "existing:1", tuple.GetKey().GetUser())

	wrapped.Stop()

	// Cache should still have original value
	entry := cache.Get(cacheKey)
	cached := entry.(*CheckIteratorCacheEntry)
	require.Equal(t, []string{"existing:1"}, cached.Values)
}

func TestCachingIterator_Head(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t)

	tuples := []*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "user:1"}},
		{Key: &openfgav1.TupleKey{User: "user:2"}},
	}
	inner := storage.NewStaticTupleIterator(tuples)

	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: "test-head",
		TTL:      time.Hour,
		MaxSize:  100,
		MapFunc:  MapUser,
	})

	// Head should not advance
	tuple, err := wrapped.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:1", tuple.GetKey().GetUser())

	// Next should return same
	tuple, err = wrapped.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:1", tuple.GetKey().GetUser())

	wrapped.Stop()
}

func TestCachingIterator_CancelledDrainContext(t *testing.T) {
	cache := newTestCache(t)
	cacheKey := "test-cancelled-ctx"
	sf := &singleflight.Group{}

	drainCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	tuples := []*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "user:1"}},
	}
	inner := storage.NewStaticTupleIterator(tuples)

	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: cacheKey,
		TTL:      time.Hour,
		MaxSize:  100,
		MapFunc:  MapUser,
		DrainCtx: drainCtx,
		SF:       sf,
	})

	// Stop with cancelled context - should not drain
	wrapped.Stop()

	// Should NOT be cached
	require.Nil(t, cache.Get(cacheKey))
}

func TestCachingIterator_SingleflightDedup(t *testing.T) {
	cache := newTestCache(t)
	cacheKey := "test-singleflight"
	sf := &singleflight.Group{}
	drainCtx := context.Background()

	// Create multiple iterators with same cache key
	const numIterators = 10
	var wg sync.WaitGroup
	wg.Add(numIterators)

	for i := 0; i < numIterators; i++ {
		go func() {
			defer wg.Done()

			tuples := []*openfgav1.Tuple{
				{Key: &openfgav1.TupleKey{User: "user:1"}},
				{Key: &openfgav1.TupleKey{User: "user:2"}},
			}
			inner := storage.NewStaticTupleIterator(tuples)

			wrapped := WrapIterator(inner, IteratorCacheConfig{
				Cache:    cache,
				CacheKey: cacheKey,
				TTL:      time.Hour,
				MaxSize:  100,
				MapFunc:  MapUser,
				DrainCtx: drainCtx,
				SF:       sf,
			})

			// Read one tuple
			_, _ = wrapped.Next(context.Background())

			// Stop - triggers async drain
			wrapped.Stop()
		}()
	}

	wg.Wait()

	// Wait for cache to be populated
	require.Eventually(t, func() bool {
		return cache.Get(cacheKey) != nil
	}, time.Second, 10*time.Millisecond)

	// Verify cached correctly
	entry := cache.Get(cacheKey)
	require.NotNil(t, entry)
	cached := entry.(*CheckIteratorCacheEntry)
	require.Equal(t, []string{"user:1", "user:2"}, cached.Values)
}

func TestStaticIter(t *testing.T) {
	ctx := context.Background()

	t.Run("empty", func(t *testing.T) {
		iter := &staticIter{values: []string{}}
		_, err := iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("with_values", func(t *testing.T) {
		iter := &staticIter{values: []string{"a", "b", "c"}}

		// Head doesn't advance
		tuple, err := iter.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, "a", tuple.GetKey().GetUser())

		// Next advances
		tuple, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "a", tuple.GetKey().GetUser())

		tuple, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "b", tuple.GetKey().GetUser())

		tuple, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "c", tuple.GetKey().GetUser())

		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("stop_is_noop", func(t *testing.T) {
		iter := &staticIter{values: []string{"a"}}
		iter.Stop() // Should not panic
		tuple, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "a", tuple.GetKey().GetUser())
	})
}

func TestCheckIteratorCacheEntry_CacheEntityType(t *testing.T) {
	entry := &CheckIteratorCacheEntry{}
	require.Equal(t, "check_iterator", entry.CacheEntityType())
}

func TestCachingIterator_EmptyIterator(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t)
	cacheKey := "test-empty"

	inner := storage.NewStaticTupleIterator([]*openfgav1.Tuple{})

	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: cacheKey,
		TTL:      time.Hour,
		MaxSize:  100,
		MapFunc:  MapUser,
	})

	_, err := wrapped.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)

	wrapped.Stop()

	// Empty result should not be cached
	require.Nil(t, cache.Get(cacheKey))
}

func TestCachingIterator_StopTwice(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t)
	cacheKey := "test-stop-twice"

	tuples := []*openfgav1.Tuple{
		{Key: &openfgav1.TupleKey{User: "user:1"}},
	}
	inner := storage.NewStaticTupleIterator(tuples)

	wrapped := WrapIterator(inner, IteratorCacheConfig{
		Cache:    cache,
		CacheKey: cacheKey,
		TTL:      time.Hour,
		MaxSize:  100,
		MapFunc:  MapUser,
	})

	_, _ = wrapped.Next(ctx)

	// Stop twice should not panic
	wrapped.Stop()
	wrapped.Stop()

	// Should still be cached
	require.NotNil(t, cache.Get(cacheKey))
}

// Benchmarks

func BenchmarkCachingIterator_Next(b *testing.B) {
	cache, _ := storage.NewInMemoryLRUCache[any]()
	b.Cleanup(cache.Stop)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tuples := make([]*openfgav1.Tuple, 100)
		for j := 0; j < 100; j++ {
			tuples[j] = &openfgav1.Tuple{Key: &openfgav1.TupleKey{User: "user:test"}}
		}
		inner := storage.NewStaticTupleIterator(tuples)

		wrapped := WrapIterator(inner, IteratorCacheConfig{
			Cache:    cache,
			CacheKey: "bench-key",
			TTL:      time.Hour,
			MaxSize:  1000,
			MapFunc:  MapUser,
		})

		for {
			_, err := wrapped.Next(context.Background())
			if err != nil {
				break
			}
		}
		wrapped.Stop()

		// Clear cache for next iteration
		cache.Delete("bench-key")
	}
}

func BenchmarkCachingIterator_CacheHit(b *testing.B) {
	cache, _ := storage.NewInMemoryLRUCache[any]()
	b.Cleanup(cache.Stop)
	cacheKey := "bench-cache-hit"

	// Pre-populate cache
	values := make([]string, 100)
	for i := 0; i < 100; i++ {
		values[i] = "user:test"
	}
	cache.Set(cacheKey, &CheckIteratorCacheEntry{Values: values}, time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inner := storage.NewStaticTupleIterator([]*openfgav1.Tuple{})

		wrapped := WrapIterator(inner, IteratorCacheConfig{
			Cache:    cache,
			CacheKey: cacheKey,
			TTL:      time.Hour,
			MaxSize:  1000,
			MapFunc:  MapUser,
		})

		for {
			_, err := wrapped.Next(context.Background())
			if err != nil {
				break
			}
		}
		wrapped.Stop()
	}
}

func BenchmarkBuildUsersetTuplesCacheKey(b *testing.B) {
	conditions := []string{"cond_b", "cond_a"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BuildUsersetTuplesCacheKey("store123", "document:1", "viewer", conditions)
	}
}

func BenchmarkMapUser(b *testing.B) {
	tk := &openfgav1.TupleKey{User: "user:alice"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MapUser(tk)
	}
}
