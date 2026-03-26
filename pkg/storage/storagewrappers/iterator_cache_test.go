package storagewrappers

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

// ─────────────────────────────────────────────────────────────────────────────
// LockFreeCachedIterator Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestLockFreeCachedIterator_Next_Basic(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	entries := []MinimalCacheEntry{
		{ObjectID: "1", User: "user:alice"},
		{ObjectID: "2", User: "user:bob"},
		{ObjectID: "3", User: "user:charlie"},
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")

	ctx := context.Background()

	// Verify all entries can be retrieved
	t1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", t1.GetKey().GetObject())
	require.Equal(t, "viewer", t1.GetKey().GetRelation())
	require.Equal(t, "user:alice", t1.GetKey().GetUser())

	t2, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:2", t2.GetKey().GetObject())
	require.Equal(t, "user:bob", t2.GetKey().GetUser())

	t3, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:3", t3.GetKey().GetObject())
	require.Equal(t, "user:charlie", t3.GetKey().GetUser())

	// Should return ErrIteratorDone after exhaustion
	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestLockFreeCachedIterator_Next_Empty(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	iter := NewLockFreeCachedIterator([]MinimalCacheEntry{}, "document", "viewer")

	ctx := context.Background()
	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestLockFreeCachedIterator_Next_WithCondition(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	condCtx, _ := structpb.NewStruct(map[string]interface{}{"region": "us-east"})
	entries := []MinimalCacheEntry{
		{
			ObjectID:         "1",
			User:             "user:alice",
			ConditionName:    "is_allowed",
			ConditionContext: condCtx,
		},
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")

	ctx := context.Background()
	t1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "is_allowed", t1.GetKey().GetCondition().GetName())
	require.Equal(t, condCtx, t1.GetKey().GetCondition().GetContext())
}

func TestLockFreeCachedIterator_Head_Basic(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	entries := []MinimalCacheEntry{
		{ObjectID: "1", User: "user:alice"},
		{ObjectID: "2", User: "user:bob"},
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")

	ctx := context.Background()

	// Head should return first element without advancing
	t1, err := iter.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", t1.GetKey().GetObject())

	// Second Head call should return the same element
	t2, err := iter.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", t2.GetKey().GetObject())

	// Next should also return the same (first) element
	t3, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", t3.GetKey().GetObject())

	// Now Next should advance to second element
	t4, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:2", t4.GetKey().GetObject())
}

func TestLockFreeCachedIterator_Concurrent_Next(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	entries := make([]MinimalCacheEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID: string(rune('a' + i%26)),
			User:     "user:test",
		}
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")

	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan *openfgav1.Tuple, 200)

	// Start multiple goroutines calling Next concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				t, err := iter.Next(ctx)
				if err != nil {
					return
				}
				results <- t
			}
		}()
	}

	wg.Wait()
	close(results)

	// Count results - should be exactly 100 (no duplicates, no misses)
	count := 0
	for range results {
		count++
	}
	require.Equal(t, 100, count)
}

func TestLockFreeCachedIterator_Concurrent_Stop(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	entries := make([]MinimalCacheEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MinimalCacheEntry{ObjectID: "1", User: "user:test"}
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")

	ctx := context.Background()
	var wg sync.WaitGroup

	// Start goroutines calling Next
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, err := iter.Next(ctx)
				if err != nil {
					return
				}
			}
		}()
	}

	// Stop from another goroutine
	time.Sleep(10 * time.Millisecond)
	iter.Stop()

	wg.Wait()

	// After Stop, Next should return ErrIteratorDone
	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestLockFreeCachedIterator_ContextCanceled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	entries := []MinimalCacheEntry{
		{ObjectID: "1", User: "user:alice"},
	}

	iter := NewLockFreeCachedIterator(entries, "document", "viewer")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)

	_, err = iter.Head(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// ─────────────────────────────────────────────────────────────────────────────
// CachingIterator Tests - Using Static Iterators
// ─────────────────────────────────────────────────────────────────────────────

func TestCachingIterator_Next_Basic(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)

	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:alice")},
		{Key: tuple.NewTupleKey("document:2", "viewer", "user:bob")},
	}

	// Use static iterator
	innerIter := storage.NewStaticTupleIterator(tuples)

	// Expect cache.Get to check if already cached (optimization 1), return nil (not cached)
	mockCache.EXPECT().Get("test-key").Return(nil).Times(1)
	// Expect cache.Set when Stop() is called after iterator is exhausted
	mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	iter := newCachingIterator(
		innerIter, mockCache, "test-key", 1000, time.Hour, 30*time.Second,
		sf, wg, "document", "viewer", "ReadUsersetTuples",
	)

	t1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", t1.GetKey().GetObject())

	t2, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:2", t2.GetKey().GetObject())

	// Iterator done
	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)

	// Stop triggers flush to cache (now async via goroutine)
	iter.Stop()

	// Wait for background goroutine to complete
	wg.Wait()
}

func TestCachingIterator_State_Abandoned_OnMaxSize(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)

	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	maxSize := 3 // Small max size to trigger abandonment

	// Create more tuples than maxSize
	tuples := make([]*openfgav1.Tuple, maxSize+1)
	for i := 0; i < maxSize+1; i++ {
		tuples[i] = &openfgav1.Tuple{Key: tuple.NewTupleKey("document:"+string(rune('1'+i)), "viewer", "user:test")}
	}

	innerIter := storage.NewStaticTupleIterator(tuples)

	// No cache.Set expected because we exceed maxSize

	iter := newCachingIterator(
		innerIter, mockCache, "test-key", maxSize, time.Hour, 30*time.Second,
		sf, wg, "document", "viewer", "ReadUsersetTuples",
	)

	// Consume all tuples
	for i := 0; i < maxSize+1; i++ {
		_, err := iter.Next(ctx)
		require.NoError(t, err)
	}

	// Verify tuples is nil (abandoned due to exceeding maxSize)
	require.Nil(t, iter.tuples)
}

func TestCachingIterator_PopulatesCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)

	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	cacheKey := "test-key"
	ttl := time.Hour

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:alice")},
	}

	innerIter := storage.NewStaticTupleIterator(tuples)

	// Expect cache.Get to check if already cached (optimization 1), return nil (not cached)
	mockCache.EXPECT().Get(cacheKey).Return(nil).Times(1)
	// Capture the cache entry
	var capturedEntry *V2IteratorCacheEntry
	mockCache.EXPECT().Set(cacheKey, gomock.Any(), ttl).DoAndReturn(
		func(key string, value interface{}, duration time.Duration) {
			capturedEntry = value.(*V2IteratorCacheEntry)
		},
	)

	iter := newCachingIterator(
		innerIter, mockCache, cacheKey, 1000, ttl, 30*time.Second,
		sf, wg, "document", "viewer", "ReadUsersetTuples",
	)

	// Consume all tuples
	_, err := iter.Next(ctx)
	require.NoError(t, err)

	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)

	// Stop triggers flush to cache (now async via goroutine)
	iter.Stop()

	// Wait for background goroutine to complete
	wg.Wait()

	// Verify cache was populated correctly
	require.NotNil(t, capturedEntry)
	require.Len(t, capturedEntry.Entries, 1)
	require.Equal(t, "1", capturedEntry.Entries[0].ObjectID)
	require.Equal(t, "user:alice", capturedEntry.Entries[0].User)
}

func TestCachingIterator_InnerError(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)

	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	// Empty iterator returns ErrIteratorDone immediately
	innerIter := storage.NewStaticTupleIterator([]*openfgav1.Tuple{})

	// No cache.Set expected on empty iteration

	iter := newCachingIterator(
		innerIter, mockCache, "test-key", 1000, time.Hour, 30*time.Second,
		sf, wg, "document", "viewer", "ReadUsersetTuples",
	)

	_, err := iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestCachingIterator_CustomMaxSizeAbandoned(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)

	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	customMaxSize := 5

	// Create more tuples than customMaxSize
	tuples := make([]*openfgav1.Tuple, customMaxSize+1)
	for i := 0; i < customMaxSize+1; i++ {
		tuples[i] = &openfgav1.Tuple{Key: tuple.NewTupleKey("document:"+string(rune('1'+i)), "viewer", "user:test")}
	}

	innerIter := storage.NewStaticTupleIterator(tuples)

	iter := newCachingIterator(
		innerIter, mockCache, "test-key", customMaxSize, time.Hour, 30*time.Second,
		sf, wg, "document", "viewer", "ReadUsersetTuples",
	)

	// Consume all tuples
	for i := 0; i < customMaxSize+1; i++ {
		_, err := iter.Next(ctx)
		require.NoError(t, err)
	}

	// Verify tuples is nil (abandoned due to exceeding maxSize)
	require.Nil(t, iter.tuples)
}

// TestCachingIterator_BackgroundDrainIgnoresRequestContextCancellation verifies
// that the background drain completes and caches even when the original request
// context is canceled. This is important because:
// 1. Consumer calls Stop() after finding a result early
// 2. Original request completes and cancels its context
// 3. Background drain should still complete using context.Background().
func TestCachingIterator_BackgroundDrainIgnoresRequestContextCancellation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)

	// Create a request context that we'll cancel to simulate request completion
	requestCtx, cancelRequest := context.WithCancel(context.Background())

	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	// Create tuples - use StaticTupleIterator since background drain uses context.Background()
	tuples := make([]*openfgav1.Tuple, 10)
	for i := 0; i < 10; i++ {
		tuples[i] = &openfgav1.Tuple{Key: tuple.NewTupleKey("document:"+string(rune('1'+i)), "viewer", "user:test")}
	}

	innerIter := storage.NewStaticTupleIterator(tuples)

	// Expect cache.Get to check if already cached (optimization 1), return nil (not cached)
	mockCache.EXPECT().Get("test-key").Return(nil).Times(1)
	// Expect cache.Set because background drain uses context.Background() and should complete
	mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	iter := newCachingIterator(
		innerIter, mockCache, "test-key", 1000, time.Hour, 30*time.Second,
		sf, wg, "document", "viewer", "ReadUsersetTuples",
	)

	// Consume just a few tuples (simulating finding a result early)
	for i := 0; i < 3; i++ {
		_, err := iter.Next(requestCtx)
		require.NoError(t, err)
	}

	// Cancel the request context to simulate request completion
	cancelRequest()

	// Stop triggers background drain which should complete and cache
	// despite the request context being canceled
	iter.Stop()

	// Wait for background drain to complete
	wg.Wait()

	// Verify: cache.Set was called (checked by gomock)
}

func TestCachingIterator_BackgroundDrainCompletes_DoesCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)

	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	tuples := make([]*openfgav1.Tuple, 10)
	for i := 0; i < 10; i++ {
		tuples[i] = &openfgav1.Tuple{Key: tuple.NewTupleKey("document:"+string(rune('1'+i)), "viewer", "user:test")}
	}

	innerIter := storage.NewStaticTupleIterator(tuples)

	// Expect cache.Get to check if already cached (optimization 1), return nil (not cached)
	mockCache.EXPECT().Get("test-key").Return(nil).Times(1)
	// Expect cache.Set because drain will complete successfully
	mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	iter := newCachingIterator(
		innerIter, mockCache, "test-key", 1000, time.Hour, 30*time.Second,
		sf, wg, "document", "viewer", "ReadUsersetTuples",
	)

	// Consume just a few tuples (not all)
	for i := 0; i < 3; i++ {
		_, err := iter.Next(ctx)
		require.NoError(t, err)
	}

	// Stop triggers background drain which should complete and cache
	iter.Stop()

	// Wait for background drain to complete
	wg.Wait()

	// Verify: cache.Set was called once (checked by gomock)
}

// ─────────────────────────────────────────────────────────────────────────────
// Cache Key Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildReadUsersetTuplesCacheKey_Basic(t *testing.T) {
	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
		},
	}

	key := buildReadUsersetTuplesCacheKey("store123", filter)

	require.Contains(t, key, "v2ic.rut/")
	require.Contains(t, key, "store123")
	require.Contains(t, key, "document:1#viewer")
	require.Contains(t, key, "group#member")
}

func TestBuildReadUsersetTuplesCacheKey_WithConditions(t *testing.T) {
	filter := storage.ReadUsersetTuplesFilter{
		Object:     "document:1",
		Relation:   "viewer",
		Conditions: []string{"cond1", "cond2"},
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{Type: "user"},
		},
	}

	key := buildReadUsersetTuplesCacheKey("store123", filter)

	require.Contains(t, key, "v2ic.rut/")
	require.Contains(t, key, "/c:") // Conditions hash
}

func TestBuildReadUsersetTuplesCacheKey_Wildcard(t *testing.T) {
	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{Type: "user", RelationOrWildcard: &openfgav1.RelationReference_Wildcard{}},
		},
	}

	key := buildReadUsersetTuplesCacheKey("store123", filter)

	require.Contains(t, key, "user:*")
}

func TestBuildReadCacheKey_Basic(t *testing.T) {
	filter := storage.ReadFilter{
		Object:   "document:1",
		Relation: "parent",
		User:     "folder:",
	}

	key := buildReadCacheKey("store123", filter)

	require.Contains(t, key, "v2ic.r/")
	require.Contains(t, key, "store123")
	require.Contains(t, key, "document:1#parent")
	require.Contains(t, key, "folder:")
}

func TestBuildReadCacheKey_WithConditions(t *testing.T) {
	filter := storage.ReadFilter{
		Object:     "document:1",
		Relation:   "parent",
		User:       "folder:",
		Conditions: []string{"cond1"},
	}

	key := buildReadCacheKey("store123", filter)

	require.Contains(t, key, "/c:")
}

func TestBuildReadStartingWithUserCacheKey_Basic(t *testing.T) {
	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:alice"},
		},
	}

	key := buildReadStartingWithUserCacheKey("store123", filter)

	require.Contains(t, key, "v2ic.rswu/")
	require.Contains(t, key, "store123")
	require.Contains(t, key, "document#viewer")
	require.Contains(t, key, "user:alice")
}

func TestBuildReadStartingWithUserCacheKey_MultipleUsers(t *testing.T) {
	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:bob"},
			{Object: "user:alice"},
		},
	}

	key := buildReadStartingWithUserCacheKey("store123", filter)

	// Users should be sorted
	require.Contains(t, key, "user:alice,user:bob")
}

func TestBuildReadStartingWithUserCacheKey_WithRelation(t *testing.T) {
	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "group:eng", Relation: "member"},
		},
	}

	key := buildReadStartingWithUserCacheKey("store123", filter)

	require.Contains(t, key, "group:eng#member")
}

func TestCacheKey_Deterministic(t *testing.T) {
	filter := storage.ReadUsersetTuplesFilter{
		Object:     "document:1",
		Relation:   "viewer",
		Conditions: []string{"cond2", "cond1"}, // Unsorted
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
			{Type: "user"},
		},
	}

	// Generate key multiple times
	key1 := buildReadUsersetTuplesCacheKey("store123", filter)
	key2 := buildReadUsersetTuplesCacheKey("store123", filter)
	key3 := buildReadUsersetTuplesCacheKey("store123", filter)

	require.Equal(t, key1, key2)
	require.Equal(t, key2, key3)
}

func TestBuildUserTypeRestrictionsString(t *testing.T) {
	tests := []struct {
		name     string
		refs     []*openfgav1.RelationReference
		expected string
	}{
		{
			name:     "empty",
			refs:     nil,
			expected: "",
		},
		{
			name: "single_type",
			refs: []*openfgav1.RelationReference{
				{Type: "user"},
			},
			expected: "user",
		},
		{
			name: "wildcard",
			refs: []*openfgav1.RelationReference{
				{Type: "user", RelationOrWildcard: &openfgav1.RelationReference_Wildcard{}},
			},
			expected: "user:*",
		},
		{
			name: "relation",
			refs: []*openfgav1.RelationReference{
				{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
			},
			expected: "group#member",
		},
		{
			name: "multiple_sorted",
			refs: []*openfgav1.RelationReference{
				{Type: "user"},
				{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
			},
			expected: "group#member,user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildUserTypeRestrictionsString(tt.refs)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildUserFilterString(t *testing.T) {
	tests := []struct {
		name     string
		filters  []*openfgav1.ObjectRelation
		expected string
	}{
		{
			name:     "empty",
			filters:  nil,
			expected: "",
		},
		{
			name: "single_object",
			filters: []*openfgav1.ObjectRelation{
				{Object: "user:alice"},
			},
			expected: "user:alice",
		},
		{
			name: "with_relation",
			filters: []*openfgav1.ObjectRelation{
				{Object: "group:eng", Relation: "member"},
			},
			expected: "group:eng#member",
		},
		{
			name: "multiple_sorted",
			filters: []*openfgav1.ObjectRelation{
				{Object: "user:bob"},
				{Object: "user:alice"},
			},
			expected: "user:alice,user:bob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildUserFilterString(tt.filters)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestAppendConditionsHash(t *testing.T) {
	t.Run("empty_conditions", func(t *testing.T) {
		var b strings.Builder
		appendConditionsHash(&b, nil)
		require.Empty(t, b.String())
	})

	t.Run("empty_string_conditions", func(t *testing.T) {
		var b strings.Builder
		appendConditionsHash(&b, []string{""})
		require.Empty(t, b.String())
	})

	t.Run("with_conditions", func(t *testing.T) {
		var b strings.Builder
		appendConditionsHash(&b, []string{"cond1", "cond2"})
		require.Contains(t, b.String(), "/c:")
	})

	t.Run("deterministic", func(t *testing.T) {
		var b1, b2 strings.Builder
		appendConditionsHash(&b1, []string{"cond2", "cond1"})
		appendConditionsHash(&b2, []string{"cond1", "cond2"})
		require.Equal(t, b1.String(), b2.String())
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// extractObjectID Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestExtractObjectID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"document:123", "123"},
		{"folder:abc-def", "abc-def"},
		{"type:", ""},
		{"nocolon", "nocolon"},
		{"a:b:c", "b:c"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := extractObjectID(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkCachingIterator_CacheMiss benchmarks cache miss scenario:
// wrapping a database iterator and collecting tuples for caching.
func BenchmarkCachingIterator_CacheMiss(b *testing.B) {
	mockController := gomock.NewController(b)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	// Create 100 tuples for iteration
	tuples := make([]*openfgav1.Tuple, 100)
	for i := 0; i < 100; i++ {
		tuples[i] = &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:"+string(rune('a'+i%26)), "viewer", "user:test"),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		innerIter := storage.NewStaticTupleIterator(tuples)
		iter := newCachingIterator(
			innerIter, mockCache, "test-key", 1000, time.Hour, 30*time.Second,
			sf, wg, "document", "viewer", "benchmark",
		)

		// Consume all tuples
		for {
			_, err := iter.Next(ctx)
			if err != nil {
				break
			}
		}
	}
}

// BenchmarkCachingIterator_CacheHit benchmarks cache hit scenario:
// creating a LockFreeCachedIterator and iterating over cached entries.
func BenchmarkCachingIterator_CacheHit(b *testing.B) {
	// Create 100 cached entries
	entries := make([]MinimalCacheEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID: string(rune('a' + i%26)),
			User:     "user:test",
		}
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := NewLockFreeCachedIterator(entries, "document", "viewer")

		// Consume all entries
		for {
			_, err := iter.Next(ctx)
			if err != nil {
				break
			}
		}
	}
}

// BenchmarkLockFreeCachedIterator_Next benchmarks the Next() operation
// of the lock-free cached iterator.
func BenchmarkLockFreeCachedIterator_Next(b *testing.B) {
	// Create 100 cached entries
	entries := make([]MinimalCacheEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID:      string(rune('a' + i%26)),
			User:          "user:test",
			ConditionName: "test_condition",
		}
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := NewLockFreeCachedIterator(entries, "document", "viewer")

		// Consume all entries
		for {
			_, err := iter.Next(ctx)
			if err != nil {
				break
			}
		}
	}
}

// BenchmarkLockFreeCachedIterator_VsStaticIterator compares the lock-free
// cached iterator against the standard StaticTupleIterator.
func BenchmarkLockFreeCachedIterator_VsStaticIterator(b *testing.B) {
	// Create data for both iterators
	entries := make([]MinimalCacheEntry, 100)
	tuples := make([]*openfgav1.Tuple, 100)
	for i := 0; i < 100; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID: string(rune('a' + i%26)),
			User:     "user:test",
		}
		tuples[i] = &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:"+string(rune('a'+i%26)), "viewer", "user:test"),
		}
	}

	ctx := context.Background()

	b.Run("LockFreeCachedIterator", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			iter := NewLockFreeCachedIterator(entries, "document", "viewer")
			for {
				_, err := iter.Next(ctx)
				if err != nil {
					break
				}
			}
		}
	})

	b.Run("StaticTupleIterator", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			iter := storage.NewStaticTupleIterator(tuples)
			for {
				_, err := iter.Next(ctx)
				if err != nil {
					break
				}
			}
		}
	})
}

// BenchmarkMinimalCacheEntry_Memory benchmarks memory usage of MinimalCacheEntry
// vs full TupleRecord storage.
func BenchmarkMinimalCacheEntry_Memory(b *testing.B) {
	b.Run("MinimalCacheEntry_100", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			entries := make([]MinimalCacheEntry, 100)
			for j := 0; j < 100; j++ {
				entries[j] = MinimalCacheEntry{
					ObjectID: "object-" + string(rune('a'+j%26)),
					User:     "user:test-user",
				}
			}
			_ = entries
		}
	})

	b.Run("FullTuple_100", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tuples := make([]*openfgav1.Tuple, 100)
			for j := 0; j < 100; j++ {
				tuples[j] = &openfgav1.Tuple{
					Key: tuple.NewTupleKey(
						"document:object-"+string(rune('a'+j%26)),
						"viewer",
						"user:test-user",
					),
				}
			}
			_ = tuples
		}
	})
}

// BenchmarkCacheKeyGeneration benchmarks cache key generation performance.
func BenchmarkCacheKeyGeneration(b *testing.B) {
	b.Run("ReadUsersetTuples", func(b *testing.B) {
		filter := storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
				{Type: "user"},
			},
			Conditions: []string{"cond1", "cond2"},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = buildReadUsersetTuplesCacheKey("store123", filter)
		}
	})

	b.Run("Read", func(b *testing.B) {
		filter := storage.ReadFilter{
			Object:     "document:1",
			Relation:   "parent",
			User:       "folder:",
			Conditions: []string{"cond1"},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = buildReadCacheKey("store123", filter)
		}
	})

	b.Run("ReadStartingWithUser", func(b *testing.B) {
		filter := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:alice"},
				{Object: "user:bob"},
			},
			Conditions: []string{"cond1", "cond2"},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = buildReadStartingWithUserCacheKey("store123", filter)
		}
	})
}
