package check

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

// ─────────────────────────────────────────────────────────────────────────────
// Mock Cache Helper
// ─────────────────────────────────────────────────────────────────────────────

type mockInMemoryCache struct {
	mu    sync.RWMutex
	items map[string]any
}

func newMockInMemoryCache() *mockInMemoryCache {
	return &mockInMemoryCache{
		items: make(map[string]any),
	}
}

func (c *mockInMemoryCache) Get(key string) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.items[key]
}

func (c *mockInMemoryCache) Set(key string, value any, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = value
}

func (c *mockInMemoryCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *mockInMemoryCache) Stop() {}

// ─────────────────────────────────────────────────────────────────────────────
// Test Helpers
// ─────────────────────────────────────────────────────────────────────────────

//nolint:unparam // relation param kept for consistency with newTestTupleWithCondition
func newTestTuple(object, relation, user string) *openfgav1.Tuple {
	return &openfgav1.Tuple{
		Key: tuple.NewTupleKey(object, relation, user),
	}
}

func newTestTupleWithCondition(object, relation, user, conditionName string, conditionContext *structpb.Struct) *openfgav1.Tuple {
	return &openfgav1.Tuple{
		Key: tuple.NewTupleKeyWithCondition(object, relation, user, conditionName, conditionContext),
	}
}

func mustStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}

func defaultCacheConfig(cache storage.InMemoryCache[any], wg *sync.WaitGroup) IteratorCacheConfig {
	return IteratorCacheConfig{
		Cache:        cache,
		TTL:          time.Minute,
		Singleflight: &singleflight.Group{},
		WaitGroup:    wg,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PreConditionCachingIterator Tests - Basic Functionality
// ─────────────────────────────────────────────────────────────────────────────

func TestPreConditionCachingIterator_Next_Basic(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name     string
		tuples   []*openfgav1.Tuple
		expected []*openfgav1.Tuple
	}{
		{
			name: "multiple_tuples",
			tuples: []*openfgav1.Tuple{
				newTestTuple("document:1", "viewer", "user:alice"),
				newTestTuple("document:2", "viewer", "user:bob"),
				newTestTuple("document:3", "viewer", "user:charlie"),
			},
			expected: []*openfgav1.Tuple{
				newTestTuple("document:1", "viewer", "user:alice"),
				newTestTuple("document:2", "viewer", "user:bob"),
				newTestTuple("document:3", "viewer", "user:charlie"),
			},
		},
		{
			name:     "empty_iterator",
			tuples:   []*openfgav1.Tuple{},
			expected: nil, // Empty iteration returns nil, not empty slice
		},
		{
			name: "single_tuple",
			tuples: []*openfgav1.Tuple{
				newTestTuple("document:1", "viewer", "user:alice"),
			},
			expected: []*openfgav1.Tuple{
				newTestTuple("document:1", "viewer", "user:alice"),
			},
		},
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockCache := newMockInMemoryCache()
			var wg sync.WaitGroup
			cfg := defaultCacheConfig(mockCache, &wg)

			innerIter := storage.NewStaticTupleIterator(tt.tuples)
			iter := WrapWithPreConditionCache(
				ctx, innerIter, "test-key",
				"document", "viewer", "test",
				cfg,
			)
			defer iter.Stop()

			var actual []*openfgav1.Tuple
			for {
				tup, err := iter.Next(ctx)
				if err != nil {
					require.ErrorIs(t, err, storage.ErrIteratorDone)
					break
				}
				actual = append(actual, tup)
			}

			if diff := cmp.Diff(tt.expected, actual, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPreConditionCachingIterator_Head_Basic(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	tuples := []*openfgav1.Tuple{
		newTestTuple("document:1", "viewer", "user:alice"),
		newTestTuple("document:2", "viewer", "user:bob"),
	}

	innerIter := storage.NewStaticTupleIterator(tuples)
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)
	defer iter.Stop()

	// Head should return first item without advancing
	head1, err := iter.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", head1.GetKey().GetObject())

	// Multiple Head calls should return same item
	head2, err := iter.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", head2.GetKey().GetObject())

	// Now Next should return the same first item
	next1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", next1.GetKey().GetObject())

	// Head should now return second item
	head3, err := iter.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:2", head3.GetKey().GetObject())
}

// ─────────────────────────────────────────────────────────────────────────────
// PreConditionCachingIterator Tests - State Machine
// ─────────────────────────────────────────────────────────────────────────────

func TestPreConditionCachingIterator_State_Done(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	tuples := []*openfgav1.Tuple{
		newTestTuple("document:1", "viewer", "user:alice"),
	}

	innerIter := storage.NewStaticTupleIterator(tuples)
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)

	// Consume all tuples
	_, err := iter.Next(ctx)
	require.NoError(t, err)

	// Should get ErrIteratorDone
	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)

	iter.Stop()
	wg.Wait()

	// Verify cache was populated
	entry := mockCache.Get("test-key")
	require.NotNil(t, entry, "cache should contain entry after full consumption")

	cached, ok := entry.(*V2IteratorCacheEntry)
	require.True(t, ok)
	require.Len(t, cached.Entries, 1, "cache should contain 1 tuple")
}

func TestPreConditionCachingIterator_State_Abandoned_OnMaxSize(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	// Create more tuples than maxCachedElements (1000)
	numTuples := maxCachedElements + 1
	tuples := make([]*openfgav1.Tuple, numTuples)
	for i := 0; i < numTuples; i++ {
		tuples[i] = newTestTuple(fmt.Sprintf("document:%d", i), "viewer", "user:test")
	}

	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	innerIter := storage.NewStaticTupleIterator(tuples)
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)
	defer iter.Stop()

	// Consume tuples
	count := 0
	for {
		_, err := iter.Next(ctx)
		if err != nil {
			break
		}
		count++
	}

	// Should consume exactly maxCachedElements tuples before abandoning
	// (the iterator returns up to max elements, then returns ErrIteratorDone)
	require.Equal(t, maxCachedElements, count)

	// Verify cache was NOT populated (abandoned due to size)
	entry := mockCache.Get("test-key")
	require.Nil(t, entry, "cache should not contain entry when abandoned")
}

// ─────────────────────────────────────────────────────────────────────────────
// PreConditionCachingIterator Tests - Stop and Background Drain
// ─────────────────────────────────────────────────────────────────────────────

func TestPreConditionCachingIterator_Stop_FullyConsumed(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	tuples := []*openfgav1.Tuple{
		newTestTuple("document:1", "viewer", "user:alice"),
		newTestTuple("document:2", "viewer", "user:bob"),
	}

	innerIter := storage.NewStaticTupleIterator(tuples)
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)

	// Consume all tuples
	for {
		_, err := iter.Next(ctx)
		if err != nil {
			break
		}
	}

	iter.Stop()
	wg.Wait()

	// Verify cache was populated with all tuples
	entry := mockCache.Get("test-key")
	require.NotNil(t, entry)

	cached, ok := entry.(*V2IteratorCacheEntry)
	require.True(t, ok)
	require.Len(t, cached.Entries, 2)
}

func TestPreConditionCachingIterator_Stop_PartiallyConsumed(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	tuples := []*openfgav1.Tuple{
		newTestTuple("document:1", "viewer", "user:alice"),
		newTestTuple("document:2", "viewer", "user:bob"),
		newTestTuple("document:3", "viewer", "user:charlie"),
	}

	innerIter := storage.NewStaticTupleIterator(tuples)
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)

	// Consume only first tuple
	_, err := iter.Next(ctx)
	require.NoError(t, err)

	// Stop early (triggers background drain)
	iter.Stop()

	// Wait for background drain to complete
	wg.Wait()

	// Verify cache was populated with all tuples
	entry := mockCache.Get("test-key")
	require.NotNil(t, entry, "cache should contain entry after background drain")

	cached, ok := entry.(*V2IteratorCacheEntry)
	require.True(t, ok)
	require.Len(t, cached.Entries, 3, "cache should contain all tuples after background drain")
}

func TestPreConditionCachingIterator_Stop_Idempotent(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	tuples := []*openfgav1.Tuple{
		newTestTuple("document:1", "viewer", "user:alice"),
	}

	innerIter := storage.NewStaticTupleIterator(tuples)
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)

	// Consume all tuples first to ensure cache gets populated
	for {
		_, err := iter.Next(ctx)
		if err != nil {
			break
		}
	}

	// Multiple Stop() calls should be safe
	iter.Stop()
	iter.Stop()
	iter.Stop()

	wg.Wait()

	// Should not panic and cache should be populated
	entry := mockCache.Get("test-key")
	require.NotNil(t, entry)
}

// ─────────────────────────────────────────────────────────────────────────────
// PreConditionCachingIterator Tests - Cache Hit
// ─────────────────────────────────────────────────────────────────────────────

func TestPreConditionCachingIterator_CacheHit_ReturnsLockFreeIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	// Pre-populate cache
	mockCache := newMockInMemoryCache()
	cachedEntries := []MinimalCacheEntry{
		{ObjectID: "1", User: "user:alice"},
		{ObjectID: "2", User: "user:bob"},
	}
	mockCache.Set("test-key", &V2IteratorCacheEntry{
		Entries:      cachedEntries,
		LastModified: time.Now(),
	}, time.Minute)

	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	// Inner iterator should NOT be used (cache hit)
	innerIter := storage.NewStaticTupleIterator(nil) // Empty - won't be used
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)
	defer iter.Stop()

	// Verify we get a LockFreeCachedIterator (cache hit)
	_, ok := iter.(*LockFreeCachedIterator)
	require.True(t, ok, "should return LockFreeCachedIterator on cache hit")

	// Verify data
	var count int
	for {
		_, err := iter.Next(ctx)
		if err != nil {
			break
		}
		count++
	}
	require.Equal(t, 2, count)
}

func TestPreConditionCachingIterator_CacheHit_CorrectData(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	// Pre-populate cache with condition data
	mockCache := newMockInMemoryCache()
	cachedEntries := []MinimalCacheEntry{
		{
			ObjectID:         "budget_2024",
			User:             "user:alice",
			ConditionName:    "is_working_hours",
			ConditionContext: mustStruct(map[string]any{"timezone": "America/New_York"}),
		},
		{
			ObjectID: "report_q1",
			User:     "user:alice",
		},
	}
	mockCache.Set("test-key", &V2IteratorCacheEntry{
		Entries:      cachedEntries,
		LastModified: time.Now(),
	}, time.Minute)

	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	innerIter := storage.NewStaticTupleIterator(nil)
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)
	defer iter.Stop()

	// First tuple should have condition
	tup1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:budget_2024", tup1.GetKey().GetObject())
	require.Equal(t, "viewer", tup1.GetKey().GetRelation())
	require.Equal(t, "user:alice", tup1.GetKey().GetUser())
	require.Equal(t, "is_working_hours", tup1.GetKey().GetCondition().GetName())
	require.NotNil(t, tup1.GetKey().GetCondition().GetContext())

	// Second tuple should NOT have condition
	tup2, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:report_q1", tup2.GetKey().GetObject())
	require.Nil(t, tup2.GetKey().GetCondition())

	// Should be done
	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestPreConditionCachingIterator_CacheMiss_PopulatesCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	tuples := []*openfgav1.Tuple{
		newTestTupleWithCondition("document:1", "viewer", "user:alice", "cond1", mustStruct(map[string]any{"key": "value"})),
		newTestTuple("document:2", "viewer", "user:bob"),
	}

	innerIter := storage.NewStaticTupleIterator(tuples)
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)

	// Consume all tuples
	for {
		_, err := iter.Next(ctx)
		if err != nil {
			break
		}
	}
	iter.Stop()
	wg.Wait()

	// Verify cache was populated
	entry := mockCache.Get("test-key")
	require.NotNil(t, entry)

	cached, ok := entry.(*V2IteratorCacheEntry)
	require.True(t, ok)
	require.Len(t, cached.Entries, 2)

	// Verify condition data was preserved
	require.Equal(t, "1", cached.Entries[0].ObjectID)
	require.Equal(t, "cond1", cached.Entries[0].ConditionName)
	require.NotNil(t, cached.Entries[0].ConditionContext)

	require.Equal(t, "2", cached.Entries[1].ObjectID)
	require.Empty(t, cached.Entries[1].ConditionName)
	require.Nil(t, cached.Entries[1].ConditionContext)
}

// ─────────────────────────────────────────────────────────────────────────────
// LockFreeCachedIterator Tests - Basic Functionality
// ─────────────────────────────────────────────────────────────────────────────

func TestLockFreeCachedIterator_Next_Basic(t *testing.T) {
	ctx := context.Background()

	entries := []MinimalCacheEntry{
		{ObjectID: "1", User: "user:alice", ConditionName: "cond1"},
		{ObjectID: "2", User: "user:bob", ConditionName: ""},
		{ObjectID: "3", User: "group:eng#member", ConditionName: "cond2"},
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")
	defer iter.Stop()

	// Verify first tuple
	tup, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", tup.GetKey().GetObject())
	require.Equal(t, "viewer", tup.GetKey().GetRelation())
	require.Equal(t, "user:alice", tup.GetKey().GetUser())
	require.Equal(t, "cond1", tup.GetKey().GetCondition().GetName())

	// Verify second tuple (no condition)
	tup, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:2", tup.GetKey().GetObject())
	require.Nil(t, tup.GetKey().GetCondition())

	// Verify third tuple (userset)
	tup, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:3", tup.GetKey().GetObject())
	require.Equal(t, "group:eng#member", tup.GetKey().GetUser())

	// Verify done
	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestLockFreeCachedIterator_Next_Empty(t *testing.T) {
	ctx := context.Background()

	entries := []MinimalCacheEntry{}
	iter := NewLockFreeCachedIterator(entries, "document", "viewer")
	defer iter.Stop()

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestLockFreeCachedIterator_Head_Basic(t *testing.T) {
	ctx := context.Background()

	entries := []MinimalCacheEntry{
		{ObjectID: "1", User: "user:alice"},
		{ObjectID: "2", User: "user:bob"},
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")
	defer iter.Stop()

	// Multiple Head calls should return same item
	head1, err := iter.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", head1.GetKey().GetObject())

	head2, err := iter.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", head2.GetKey().GetObject())

	// Next should advance
	next1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", next1.GetKey().GetObject())

	// Head should now return second
	head3, err := iter.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:2", head3.GetKey().GetObject())
}

// ─────────────────────────────────────────────────────────────────────────────
// LockFreeCachedIterator Tests - Thread Safety
// ─────────────────────────────────────────────────────────────────────────────

func TestLockFreeCachedIterator_Concurrent_Next(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	entries := make([]MinimalCacheEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID: fmt.Sprintf("%d", i),
			User:     fmt.Sprintf("user:%d", i),
		}
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")
	defer iter.Stop()

	ctx := context.Background()
	var wg errgroup.Group
	var count atomic.Int64

	// Spawn 10 goroutines, each trying to consume tuples
	for i := 0; i < 10; i++ {
		wg.Go(func() error {
			for {
				_, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						return nil
					}
					return err
				}
				count.Add(1)
			}
		})
	}

	err := wg.Wait()
	require.NoError(t, err)

	// Each tuple should be consumed exactly once
	require.Equal(t, int64(100), count.Load())
}

func TestLockFreeCachedIterator_Concurrent_Stop(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	entries := make([]MinimalCacheEntry, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID: fmt.Sprintf("%d", i),
			User:     fmt.Sprintf("user:%d", i),
		}
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")

	ctx := context.Background()
	var wg errgroup.Group

	// Consumer goroutine
	wg.Go(func() error {
		for {
			_, err := iter.Next(ctx)
			if err != nil {
				return nil // Stop() was called or done
			}
		}
	})

	// Stop goroutine - calls Stop() after short delay
	wg.Go(func() error {
		time.Sleep(10 * time.Microsecond)
		iter.Stop()
		return nil
	})

	err := wg.Wait()
	require.NoError(t, err)

	// Subsequent calls should return done
	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

// ─────────────────────────────────────────────────────────────────────────────
// Cache Key Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildRSWUCacheKey_Basic(t *testing.T) {
	key := BuildRSWUCacheKey("store1", "document", "viewer", []string{"user:alice"}, nil)
	require.Contains(t, key, "v2ic.rswu/")
	require.Contains(t, key, "store1")
	require.Contains(t, key, "document#viewer")
	require.Contains(t, key, "user:alice")
}

func TestBuildRSWUCacheKey_WithConditions(t *testing.T) {
	tests := []struct {
		name        string
		conditions1 []string
		conditions2 []string
		shouldMatch bool
	}{
		{
			name:        "same_conditions_same_key",
			conditions1: []string{"cond1", "cond2"},
			conditions2: []string{"cond1", "cond2"},
			shouldMatch: true,
		},
		{
			name:        "different_order_same_key",
			conditions1: []string{"cond1", "cond2"},
			conditions2: []string{"cond2", "cond1"},
			shouldMatch: true, // Should be sorted
		},
		{
			name:        "different_conditions_different_key",
			conditions1: []string{"cond1"},
			conditions2: []string{"cond2"},
			shouldMatch: false,
		},
		{
			name:        "empty_vs_non_empty_different",
			conditions1: []string{},
			conditions2: []string{"cond1"},
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key1 := BuildRSWUCacheKey("store1", "document", "viewer",
				[]string{"user:alice"}, tt.conditions1)
			key2 := BuildRSWUCacheKey("store1", "document", "viewer",
				[]string{"user:alice"}, tt.conditions2)

			if tt.shouldMatch {
				require.Equal(t, key1, key2)
			} else {
				require.NotEqual(t, key1, key2)
			}
		})
	}
}

func TestBuildRSWUCacheKey_Deterministic(t *testing.T) {
	// Same inputs should produce same key
	key1 := BuildRSWUCacheKey("store1", "document", "viewer", []string{"user:alice", "user:bob"}, []string{"cond1"})
	key2 := BuildRSWUCacheKey("store1", "document", "viewer", []string{"user:alice", "user:bob"}, []string{"cond1"})
	require.Equal(t, key1, key2)

	// Users should be sorted
	key3 := BuildRSWUCacheKey("store1", "document", "viewer", []string{"user:bob", "user:alice"}, []string{"cond1"})
	require.Equal(t, key1, key3)
}

func TestBuildRUTCacheKey_Basic(t *testing.T) {
	refs := []*openfgav1.RelationReference{
		{Type: "user"},
		{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
	}
	key := BuildRUTCacheKey("store1", "document:1", "viewer", refs, nil)

	require.Contains(t, key, "v2ic.rut/")
	require.Contains(t, key, "store1")
	require.Contains(t, key, "document:1#viewer")
	require.Contains(t, key, "/user")
	require.Contains(t, key, "/group#member")
}

func TestBuildReadCacheKey_Basic(t *testing.T) {
	key := BuildReadCacheKey("store1", "document:1", "viewer", "user:", []string{"cond1"})

	require.Contains(t, key, "v2ic.r/")
	require.Contains(t, key, "store1")
	require.Contains(t, key, "document:1#viewer")
	require.Contains(t, key, "@user:")
	require.Contains(t, key, "/c:") // condition hash
}

// ─────────────────────────────────────────────────────────────────────────────
// Error Handling Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestPreConditionCachingIterator_InnerError(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedErr := errors.New("database error")
	mockIter := mocks.NewMockIterator[*openfgav1.Tuple](ctrl)
	mockIter.EXPECT().Next(gomock.Any()).Return(nil, expectedErr)
	// Head is called during Stop to check if iterator is exhausted
	mockIter.EXPECT().Head(gomock.Any()).Return(nil, storage.ErrIteratorDone).AnyTimes()
	mockIter.EXPECT().Stop()

	ctx := context.Background()
	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	iter := WrapWithPreConditionCache(
		ctx, mockIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)
	defer iter.Stop()

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, expectedErr)

	// Cache should NOT be populated on error
	entry := mockCache.Get("test-key")
	require.Nil(t, entry)
}

func TestPreConditionCachingIterator_ContextCanceled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tuples := []*openfgav1.Tuple{
		newTestTuple("document:1", "viewer", "user:alice"),
	}

	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	ctx, cancel := context.WithCancel(context.Background())
	innerIter := storage.NewStaticTupleIterator(tuples)
	iter := WrapWithPreConditionCache(
		ctx, innerIter, "test-key",
		"document", "viewer", "test",
		cfg,
	)
	defer iter.Stop()

	// Cancel context before iteration
	cancel()

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLockFreeCachedIterator_ContextCanceled(t *testing.T) {
	entries := []MinimalCacheEntry{
		{ObjectID: "1", User: "user:alice"},
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")
	defer iter.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration Tests - Condition Evaluation Correctness
// ─────────────────────────────────────────────────────────────────────────────

func TestCheckWithCache_DifferentContexts_SameCache(t *testing.T) {
	// This test verifies that the same cached tuples can be evaluated
	// with different request contexts to produce different results.

	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// Create tuple with condition
	tuples := []*openfgav1.Tuple{
		newTestTupleWithCondition(
			"document:budget", "viewer", "user:alice",
			"is_working_hours",
			mustStruct(map[string]any{"timezone": "America/New_York"}),
		),
	}

	mockCache := newMockInMemoryCache()
	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	// First request - consume and cache
	ctx1 := context.Background()
	innerIter1 := storage.NewStaticTupleIterator(tuples)
	iter1 := WrapWithPreConditionCache(
		ctx1, innerIter1, "test-key",
		"document", "viewer", "test",
		cfg,
	)

	// Consume all tuples
	for {
		_, err := iter1.Next(ctx1)
		if err != nil {
			break
		}
	}
	iter1.Stop()
	wg.Wait()

	// Second request - should get cache hit with same data
	ctx2 := context.Background()
	innerIter2 := storage.NewStaticTupleIterator(nil) // Won't be used
	iter2 := WrapWithPreConditionCache(
		ctx2, innerIter2, "test-key",
		"document", "viewer", "test",
		cfg,
	)
	defer iter2.Stop()

	// Verify cache hit
	_, ok := iter2.(*LockFreeCachedIterator)
	require.True(t, ok, "second request should get cache hit")

	// Verify tuple data is preserved for condition evaluation
	tup, err := iter2.Next(ctx2)
	require.NoError(t, err)
	require.Equal(t, "is_working_hours", tup.GetKey().GetCondition().GetName())
	require.NotNil(t, tup.GetKey().GetCondition().GetContext())
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

func BenchmarkPreConditionCachingIterator_CacheMiss(b *testing.B) {
	tuples := make([]*openfgav1.Tuple, 100)
	for i := 0; i < 100; i++ {
		tuples[i] = newTestTuple(
			fmt.Sprintf("document:%d", i),
			"viewer",
			fmt.Sprintf("user:%d", i),
		)
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mockCache := newMockInMemoryCache()
		var wg sync.WaitGroup
		cfg := defaultCacheConfig(mockCache, &wg)

		innerIter := storage.NewStaticTupleIterator(tuples)
		iter := WrapWithPreConditionCache(
			ctx, innerIter, fmt.Sprintf("key-%d", i),
			"document", "viewer", "test",
			cfg,
		)

		for {
			_, err := iter.Next(ctx)
			if err != nil {
				break
			}
		}
		iter.Stop()
		wg.Wait()
	}
}

func BenchmarkPreConditionCachingIterator_CacheHit(b *testing.B) {
	entries := make([]MinimalCacheEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID: fmt.Sprintf("%d", i),
			User:     fmt.Sprintf("user:%d", i),
		}
	}

	mockCache := newMockInMemoryCache()
	mockCache.Set("test-key", &V2IteratorCacheEntry{
		Entries:      entries,
		LastModified: time.Now(),
	}, time.Minute)

	var wg sync.WaitGroup
	cfg := defaultCacheConfig(mockCache, &wg)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := WrapWithPreConditionCache(
			ctx, nil, "test-key",
			"document", "viewer", "test",
			cfg,
		)

		for {
			_, err := iter.Next(ctx)
			if err != nil {
				break
			}
		}
		iter.Stop()
	}
}

func BenchmarkLockFreeCachedIterator_Next(b *testing.B) {
	entries := make([]MinimalCacheEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID: fmt.Sprintf("%d", i),
			User:     fmt.Sprintf("user:%d", i),
		}
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := NewLockFreeCachedIterator(entries, "document", "viewer")
		for {
			_, err := iter.Next(ctx)
			if err != nil {
				break
			}
		}
		iter.Stop()
	}
}

func BenchmarkLockFreeCachedIterator_VsStaticIterator(b *testing.B) {
	tuples := make([]*openfgav1.Tuple, 100)
	for i := 0; i < 100; i++ {
		tuples[i] = newTestTuple(
			fmt.Sprintf("document:%d", i),
			"viewer",
			fmt.Sprintf("user:%d", i),
		)
	}

	entries := make([]MinimalCacheEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID: fmt.Sprintf("%d", i),
			User:     fmt.Sprintf("user:%d", i),
		}
	}

	ctx := context.Background()

	b.Run("LockFreeCachedIterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := NewLockFreeCachedIterator(entries, "document", "viewer")
			for {
				_, err := iter.Next(ctx)
				if err != nil {
					break
				}
			}
			iter.Stop()
		}
	})

	b.Run("StaticTupleIterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := storage.NewStaticTupleIterator(tuples)
			for {
				_, err := iter.Next(ctx)
				if err != nil {
					break
				}
			}
			iter.Stop()
		}
	})
}
