package storagewrappers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/tuple"
)

// ─────────────────────────────────────────────────────────────────────────────
// CachedTupleReader - ReadUsersetTuples Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCachedTupleReader_ReadUsersetTuples_CacheMiss(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{Type: "user"},
		},
	}
	opts := storage.ReadUsersetTuplesOptions{}

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:alice")},
	}

	cacheKey := storage.ReadUsersetTuplesKey(storeID, filter)

	// Cache miss
	mockCache.EXPECT().Get(cacheKey).Return(nil).Times(1)

	// Delegate to datastore (use gomock.Any() for ctx since tracing adds values)
	mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, filter, opts).Return(storage.NewStaticTupleIterator(tuples), nil).Times(1)

	iter, err := reader.ReadUsersetTuples(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.NotNil(t, iter)

	// Verify it returns a CachingIterator (not LockFreeCachedIterator)
	_, ok := iter.(*CachingIterator)
	require.True(t, ok, "Expected CachingIterator on cache miss")
}

func TestCachedTupleReader_ReadUsersetTuples_CacheHit(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{Type: "user"},
		},
	}
	opts := storage.ReadUsersetTuplesOptions{}

	cachedEntry := &V2IteratorCacheEntry{
		Entries: []MinimalCacheEntry{
			{ObjectID: "1", User: "user:alice"},
		},
		LastModified: time.Now(),
	}

	cacheKey := storage.ReadUsersetTuplesKey(storeID, filter)
	entityInvalidKey := storage.InvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "viewer")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Check invalidation keys
	mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil).Times(1)
	mockCache.EXPECT().Get(entityInvalidKey).Return(nil).Times(1)

	// NO delegate call expected on cache hit

	iter, err := reader.ReadUsersetTuples(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.NotNil(t, iter)

	// Verify it returns a LockFreeCachedIterator
	_, ok := iter.(*LockFreeCachedIterator)
	require.True(t, ok, "Expected LockFreeCachedIterator on cache hit")

	// Verify reconstructed tuple data
	t1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", t1.GetKey().GetObject())
	require.Equal(t, "viewer", t1.GetKey().GetRelation())
	require.Equal(t, "user:alice", t1.GetKey().GetUser())

	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestCachedTupleReader_ReadUsersetTuples_HigherConsistency(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
	}
	opts := storage.ReadUsersetTuplesOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		},
	}

	staticIter := storage.NewStaticTupleIterator([]*openfgav1.Tuple{})

	// NO cache.Get expected - bypasses cache entirely

	// Always calls delegate for higher consistency (use gomock.Any() for ctx since tracing adds values)
	mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, filter, opts).Return(staticIter, nil).Times(1)

	iter, err := reader.ReadUsersetTuples(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.NotNil(t, iter)
	require.Equal(t, staticIter, iter)
}

func TestCachedTupleReader_ReadUsersetTuples_StoreInvalidation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
	}
	opts := storage.ReadUsersetTuplesOptions{}

	cachedEntry := &V2IteratorCacheEntry{
		Entries:      []MinimalCacheEntry{{ObjectID: "1", User: "user:alice"}},
		LastModified: time.Now().Add(-time.Hour), // Cached 1 hour ago
	}

	invalidEntry := &storage.InvalidEntityCacheEntry{
		LastModified: time.Now(), // Invalidated just now (after cache entry)
	}

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:alice")},
	}

	cacheKey := storage.ReadUsersetTuplesKey(storeID, filter)

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - returns invalidation entry that is newer
	mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(invalidEntry).Times(1)

	// Cache entry should be deleted
	mockCache.EXPECT().Delete(cacheKey).Times(1)

	// Fallback to datastore (use gomock.Any() for ctx since tracing adds values)
	mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, filter, opts).Return(storage.NewStaticTupleIterator(tuples), nil).Times(1)

	iter, err := reader.ReadUsersetTuples(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.NotNil(t, iter)

	// Should return CachingIterator (not cached)
	_, ok := iter.(*CachingIterator)
	require.True(t, ok)
}

func TestCachedTupleReader_ReadUsersetTuples_EntityInvalidation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
	}
	opts := storage.ReadUsersetTuplesOptions{}

	cachedEntry := &V2IteratorCacheEntry{
		Entries:      []MinimalCacheEntry{{ObjectID: "1", User: "user:alice"}},
		LastModified: time.Now().Add(-time.Hour),
	}

	invalidEntry := &storage.InvalidEntityCacheEntry{
		LastModified: time.Now(), // Newer than cache entry
	}

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:alice")},
	}

	cacheKey := storage.ReadUsersetTuplesKey(storeID, filter)
	entityInvalidKey := storage.InvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "viewer")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - no store-level invalidation
	mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil).Times(1)

	// Entity invalidation check - entity was invalidated
	mockCache.EXPECT().Get(entityInvalidKey).Return(invalidEntry).Times(1)

	// Cache entry should be deleted
	mockCache.EXPECT().Delete(cacheKey).Times(1)

	// Fallback to datastore (use gomock.Any() for ctx since tracing adds values)
	mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, filter, opts).Return(storage.NewStaticTupleIterator(tuples), nil).Times(1)

	iter, err := reader.ReadUsersetTuples(ctx, storeID, filter, opts)
	require.NoError(t, err)

	_, ok := iter.(*CachingIterator)
	require.True(t, ok)
}

func TestCachedTupleReader_ReadUsersetTuples_InvalidationOlderThanCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
	}
	opts := storage.ReadUsersetTuplesOptions{}

	cachedEntry := &V2IteratorCacheEntry{
		Entries:      []MinimalCacheEntry{{ObjectID: "1", User: "user:alice"}},
		LastModified: time.Now(), // Cache entry is fresh
	}

	invalidEntry := &storage.InvalidEntityCacheEntry{
		LastModified: time.Now().Add(-time.Hour), // Invalidation is older than cache
	}

	cacheKey := storage.ReadUsersetTuplesKey(storeID, filter)
	entityInvalidKey := storage.InvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "viewer")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - invalidation is older than cache
	mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(invalidEntry).Times(1)

	// Entity invalidation check
	mockCache.EXPECT().Get(entityInvalidKey).Return(nil).Times(1)

	// No delete expected - cache entry is valid

	iter, err := reader.ReadUsersetTuples(ctx, storeID, filter, opts)
	require.NoError(t, err)

	// Should return LockFreeCachedIterator (cached)
	_, ok := iter.(*LockFreeCachedIterator)
	require.True(t, ok)
}

func TestCachedTupleReader_ReadUsersetTuples_DelegateError(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
	}
	opts := storage.ReadUsersetTuplesOptions{}

	testErr := storage.ErrInvalidContinuationToken
	cacheKey := storage.ReadUsersetTuplesKey(storeID, filter)

	// Cache miss
	mockCache.EXPECT().Get(cacheKey).Return(nil).Times(1)

	// Delegate returns error (use gomock.Any() for ctx since tracing adds values)
	mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, filter, opts).Return(nil, testErr).Times(1)

	_, err := reader.ReadUsersetTuples(ctx, storeID, filter, opts)
	require.ErrorIs(t, err, testErr)
}

// ─────────────────────────────────────────────────────────────────────────────
// CachedTupleReader - Read Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCachedTupleReader_Read_CacheMiss(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadFilter{
		Object:   "document:1",
		Relation: "parent",
		User:     "folder:",
	}
	opts := storage.ReadOptions{}

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "parent", "folder:a")},
	}

	cacheKey := storage.ReadKey(storeID, filter)

	// Cache miss
	mockCache.EXPECT().Get(cacheKey).Return(nil).Times(1)

	// Delegate to datastore (use gomock.Any() for ctx since tracing adds values)
	mockDatastore.EXPECT().Read(gomock.Any(), storeID, filter, opts).Return(storage.NewStaticTupleIterator(tuples), nil).Times(1)

	iter, err := reader.Read(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.NotNil(t, iter)

	// Verify it returns a CachingIterator
	_, ok := iter.(*CachingIterator)
	require.True(t, ok, "Expected CachingIterator on cache miss")
}

func TestCachedTupleReader_Read_CacheHit(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadFilter{
		Object:   "document:1",
		Relation: "parent",
		User:     "folder:",
	}
	opts := storage.ReadOptions{}

	cachedEntry := &V2IteratorCacheEntry{
		Entries: []MinimalCacheEntry{
			{ObjectID: "1", User: "folder:a"},
		},
		LastModified: time.Now(),
	}

	cacheKey := storage.ReadKey(storeID, filter)
	entityInvalidKey := storage.InvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "parent")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)
	mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil).Times(1)
	mockCache.EXPECT().Get(entityInvalidKey).Return(nil).Times(1)

	iter, err := reader.Read(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.NotNil(t, iter)

	_, ok := iter.(*LockFreeCachedIterator)
	require.True(t, ok, "Expected LockFreeCachedIterator on cache hit")

	// Verify reconstructed tuple data
	t1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", t1.GetKey().GetObject())
	require.Equal(t, "parent", t1.GetKey().GetRelation())
	require.Equal(t, "folder:a", t1.GetKey().GetUser())

	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestCachedTupleReader_Read_HigherConsistency(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadFilter{
		Object:   "document:1",
		Relation: "parent",
		User:     "folder:",
	}
	opts := storage.ReadOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		},
	}

	staticIter := storage.NewStaticTupleIterator([]*openfgav1.Tuple{})

	mockDatastore.EXPECT().Read(gomock.Any(), storeID, filter, opts).Return(staticIter, nil).Times(1)

	iter, err := reader.Read(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.Equal(t, staticIter, iter)
}

func TestCachedTupleReader_Read_EntityInvalidation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadFilter{
		Object:   "document:1",
		Relation: "parent",
		User:     "folder:",
	}
	opts := storage.ReadOptions{}

	cachedEntry := &V2IteratorCacheEntry{
		Entries:      []MinimalCacheEntry{{ObjectID: "1", User: "folder:a"}},
		LastModified: time.Now().Add(-time.Hour),
	}

	invalidEntry := &storage.InvalidEntityCacheEntry{
		LastModified: time.Now(), // Newer than cache entry
	}

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "parent", "folder:a")},
	}

	cacheKey := storage.ReadKey(storeID, filter)
	entityInvalidKey := storage.InvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "parent")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - no store-level invalidation
	mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil).Times(1)

	// Entity invalidation check - entity was invalidated
	mockCache.EXPECT().Get(entityInvalidKey).Return(invalidEntry).Times(1)

	// Cache entry should be deleted
	mockCache.EXPECT().Delete(cacheKey).Times(1)

	// Fallback to datastore
	mockDatastore.EXPECT().Read(gomock.Any(), storeID, filter, opts).Return(storage.NewStaticTupleIterator(tuples), nil).Times(1)

	iter, err := reader.Read(ctx, storeID, filter, opts)
	require.NoError(t, err)

	_, ok := iter.(*CachingIterator)
	require.True(t, ok, "Expected CachingIterator after invalidation")
}

// ─────────────────────────────────────────────────────────────────────────────
// CachedTupleReader - ReadStartingWithUser Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCachedTupleReader_ReadStartingWithUser_CacheMiss(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:alice"},
		},
	}
	opts := storage.ReadStartingWithUserOptions{}

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:alice")},
	}

	cacheKey := storage.ReadStartingWithUserKey(storeID, filter)

	// Cache miss
	mockCache.EXPECT().Get(cacheKey).Return(nil).Times(1)

	// Delegate to datastore (use gomock.Any() for ctx since tracing adds values)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, filter, opts).Return(storage.NewStaticTupleIterator(tuples), nil).Times(1)

	iter, err := reader.ReadStartingWithUser(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.NotNil(t, iter)

	_, ok := iter.(*CachingIterator)
	require.True(t, ok, "Expected CachingIterator on cache miss")
}

func TestCachedTupleReader_ReadStartingWithUser_CacheHit(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:alice"},
		},
	}
	opts := storage.ReadStartingWithUserOptions{}

	cachedEntry := &V2IteratorCacheEntry{
		Entries: []MinimalCacheEntry{
			{ObjectID: "1", User: "user:alice"},
		},
		LastModified: time.Now(),
	}

	cacheKey := storage.ReadStartingWithUserKey(storeID, filter)
	userInvalidKeys := []keys.Key{storage.InvalidIteratorByUserObjectTypeCacheKey(storeID, "user:alice", "document")}

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)
	mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil).Times(1)
	mockCache.EXPECT().Get(userInvalidKeys[0]).Return(nil).Times(1)

	iter, err := reader.ReadStartingWithUser(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.NotNil(t, iter)

	_, ok := iter.(*LockFreeCachedIterator)
	require.True(t, ok, "Expected LockFreeCachedIterator on cache hit")

	// Verify reconstructed tuple data
	t1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", t1.GetKey().GetObject())
	require.Equal(t, "viewer", t1.GetKey().GetRelation())
	require.Equal(t, "user:alice", t1.GetKey().GetUser())

	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)
}

func TestCachedTupleReader_ReadStartingWithUser_CacheHit_UsersetFilter(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "group:eng", Relation: "member"},
		},
	}
	opts := storage.ReadStartingWithUserOptions{}

	cachedEntry := &V2IteratorCacheEntry{
		Entries: []MinimalCacheEntry{
			{ObjectID: "1", User: "group:eng#member"},
		},
		LastModified: time.Now(),
	}

	cacheKey := storage.ReadStartingWithUserKey(storeID, filter)
	userInvalidKeys := []keys.Key{storage.InvalidIteratorByUserObjectTypeCacheKey(storeID, "group:eng#member", "document")}

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Check invalidation keys
	mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil).Times(1)
	mockCache.EXPECT().Get(userInvalidKeys[0]).Return(nil).Times(1)

	iter, err := reader.ReadStartingWithUser(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.NotNil(t, iter)

	_, ok := iter.(*LockFreeCachedIterator)
	require.True(t, ok, "Expected LockFreeCachedIterator on cache hit")

	// Verify reconstructed tuple data
	t1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", t1.GetKey().GetObject())
	require.Equal(t, "viewer", t1.GetKey().GetRelation())
	require.Equal(t, "group:eng#member", t1.GetKey().GetUser())
}

func TestCachedTupleReader_ReadStartingWithUser_HigherConsistency(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:alice"},
		},
	}
	opts := storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		},
	}

	staticIter := storage.NewStaticTupleIterator([]*openfgav1.Tuple{})

	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, filter, opts).Return(staticIter, nil).Times(1)

	iter, err := reader.ReadStartingWithUser(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.Equal(t, staticIter, iter)
}

func TestCachedTupleReader_ReadStartingWithUser_EntityInvalidation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:alice"},
		},
	}
	opts := storage.ReadStartingWithUserOptions{}

	cachedEntry := &V2IteratorCacheEntry{
		Entries:      []MinimalCacheEntry{{ObjectID: "1", User: "user:alice"}},
		LastModified: time.Now().Add(-time.Hour),
	}

	invalidEntry := &storage.InvalidEntityCacheEntry{
		LastModified: time.Now(), // Newer than cache entry
	}

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:alice")},
	}

	cacheKey := storage.ReadStartingWithUserKey(storeID, filter)
	userInvalidKeys := []keys.Key{storage.InvalidIteratorByUserObjectTypeCacheKey(storeID, "user:alice", "document")}

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - no store-level invalidation
	mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil).Times(1)

	// User entity invalidation check - entity was invalidated
	mockCache.EXPECT().Get(userInvalidKeys[0]).Return(invalidEntry).Times(1)

	// Cache entry should be deleted
	mockCache.EXPECT().Delete(cacheKey).Times(1)

	// Fallback to datastore
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, filter, opts).Return(storage.NewStaticTupleIterator(tuples), nil).Times(1)

	iter, err := reader.ReadStartingWithUser(ctx, storeID, filter, opts)
	require.NoError(t, err)

	_, ok := iter.(*CachingIterator)
	require.True(t, ok, "Expected CachingIterator after invalidation")
}

// ─────────────────────────────────────────────────────────────────────────────
// CachedTupleReader - Delegate Methods Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCachedTupleReader_ReadUserTuple_Delegates(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadUserTupleFilter{
		Object:   "document:1",
		Relation: "viewer",
		User:     "user:alice",
	}
	opts := storage.ReadUserTupleOptions{}

	expectedTuple := &openfgav1.Tuple{
		Key: tuple.NewTupleKey("document:1", "viewer", "user:alice"),
	}

	// ReadUserTuple should delegate directly (no caching)
	mockDatastore.EXPECT().ReadUserTuple(ctx, storeID, filter, opts).Return(expectedTuple, nil).Times(1)

	result, err := reader.ReadUserTuple(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.Equal(t, expectedTuple, result)
}

func TestCachedTupleReader_ReadPage_Delegates(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadFilter{
		Object: "document:1",
	}
	opts := storage.ReadPageOptions{
		Pagination: storage.PaginationOptions{
			PageSize: 10,
		},
	}

	expectedTuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:alice")},
	}
	expectedToken := "next-token"

	// ReadPage should delegate directly (no caching)
	mockDatastore.EXPECT().ReadPage(ctx, storeID, filter, opts).Return(expectedTuples, expectedToken, nil).Times(1)

	tuples, token, err := reader.ReadPage(ctx, storeID, filter, opts)
	require.NoError(t, err)
	require.Equal(t, expectedTuples, tuples)
	require.Equal(t, expectedToken, token)
}

// ─────────────────────────────────────────────────────────────────────────────
// CachedTupleReader - Configuration Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCachedTupleReader_DefaultMaxSize(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	// Pass 0 for maxSize - should default to 1000
	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, 0, time.Hour, sf, wg, 30*time.Second)

	require.Equal(t, maxCachedElements, reader.maxSize)
}

func TestCachedTupleReader_CustomMaxSize(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	customMaxSize := 500
	reader := NewCachedTupleReader(ctx, mockDatastore, mockCache, customMaxSize, time.Hour, sf, wg, 30*time.Second)

	require.Equal(t, customMaxSize, reader.maxSize)
}

// ─────────────────────────────────────────────────────────────────────────────
// CachedTupleReader - Cache Key Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCachedTupleReader_CacheKeyUniqueness(t *testing.T) {
	filter1 := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
	}
	filter2 := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "editor", // Different relation
	}
	filter3 := storage.ReadUsersetTuplesFilter{
		Object:   "document:2", // Different object
		Relation: "viewer",
	}

	key1 := storage.ReadUsersetTuplesKey("store", filter1)
	key2 := storage.ReadUsersetTuplesKey("store", filter2)
	key3 := storage.ReadUsersetTuplesKey("store", filter3)

	require.NotEqual(t, key1, key2, "Different relations should produce different keys")
	require.NotEqual(t, key1, key3, "Different objects should produce different keys")
	require.NotEqual(t, key2, key3, "All keys should be unique")
}

func TestCachedTupleReader_ConditionsInCacheKey(t *testing.T) {
	filterWithoutCond := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
	}
	filterWithCond := storage.ReadUsersetTuplesFilter{
		Object:     "document:1",
		Relation:   "viewer",
		Conditions: []string{"cond1"},
	}

	keyWithout := storage.ReadUsersetTuplesKey("store", filterWithoutCond)
	keyWith := storage.ReadUsersetTuplesKey("store", filterWithCond)

	require.NotEqual(t, keyWithout, keyWith, "Conditions should affect cache key")
}

// ─────────────────────────────────────────────────────────────────────────────
// CachedTupleReader - End-to-End Cache Round-Trip Test
// ─────────────────────────────────────────────────────────────────────────────

func TestCachedTupleReader_CacheRoundTrip(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	// Use a REAL cache for round-trip testing
	realCache, err := storage.NewInMemoryLRUCache[any]()
	require.NoError(t, err)
	defer realCache.Stop()

	reader := NewCachedTupleReader(ctx, mockDatastore, realCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{Type: "user"},
		},
	}
	opts := storage.ReadUsersetTuplesOptions{}

	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:alice")},
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:bob")},
	}

	// Datastore should only be called ONCE (the cache miss). Second call uses cache.
	mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, filter, opts).
		Return(storage.NewStaticTupleIterator(tuples), nil).Times(1)

	// --- First call: cache miss ---
	iter, err := reader.ReadUsersetTuples(ctx, storeID, filter, opts)
	require.NoError(t, err)

	_, ok := iter.(*CachingIterator)
	require.True(t, ok, "First call should return CachingIterator (cache miss)")

	// Fully consume the iterator
	t1, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:alice", t1.GetKey().GetUser())

	t2, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:bob", t2.GetKey().GetUser())

	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)

	iter.Stop()

	// Wait for background drain/flush to populate cache
	cacheKey := storage.ReadUsersetTuplesKey(storeID, filter)
	require.Eventually(t, func() bool {
		return realCache.Get(cacheKey) != nil
	}, 5*time.Second, 10*time.Millisecond, "Cache should be populated after Stop()")

	// --- Second call: cache hit ---
	iter2, err := reader.ReadUsersetTuples(ctx, storeID, filter, opts)
	require.NoError(t, err)

	_, ok = iter2.(*LockFreeCachedIterator)
	require.True(t, ok, "Second call should return LockFreeCachedIterator (cache hit)")

	// Verify all reconstructed tuples match original data
	rt1, err := iter2.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", rt1.GetKey().GetObject())
	require.Equal(t, "viewer", rt1.GetKey().GetRelation())
	require.Equal(t, "user:alice", rt1.GetKey().GetUser())

	rt2, err := iter2.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", rt2.GetKey().GetObject())
	require.Equal(t, "viewer", rt2.GetKey().GetRelation())
	require.Equal(t, "user:bob", rt2.GetKey().GetUser())

	_, err = iter2.Next(ctx)
	require.ErrorIs(t, err, storage.ErrIteratorDone)

	iter2.Stop()
}

// ─────────────────────────────────────────────────────────────────────────────
// CachedTupleReader - Concurrent read coalescing
// ─────────────────────────────────────────────────────────────────────────────

// TestCachedTupleReader_Read_ConcurrentMiss_CoalescesToSingleRead verifies the
// intra-request read-coalescing behavior: when many callers issue the SAME read
// concurrently and all miss the not-yet-populated cache, only the first ("leader")
// hits the datastore. The others ("followers") block on the leader's completion
// and serve from the cache it populates, so N concurrent identical reads collapse
// to a single datastore read. This is the read amplification the recursive check
// resolver would otherwise cause by fanning out identical reads of the same
// tupleset (e.g. workgroup:X#child) within one Check.
//
// The datastore mock is gated: the leader's read is held in flight (via release)
// until it is confirmed in flight, so the other callers coalesce onto it rather
// than each starting their own read. The mock's Times(1) expectation is itself
// the coalescing assertion — a second datastore read (e.g. a straggler racing
// cache population) would trip an unexpected-call failure.
func TestCachedTupleReader_Read_ConcurrentMiss_CoalescesToSingleRead(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockRelationshipTupleReader(mockController)

	ctx := context.Background()
	storeID := ulid.Make().String()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	realCache, err := storage.NewInMemoryLRUCache[any]()
	require.NoError(t, err)
	defer realCache.Stop()

	reader := NewCachedTupleReader(ctx, mockDatastore, realCache, 1000, time.Hour, sf, wg, 30*time.Second)

	filter := storage.ReadFilter{Object: "workgroup:13110", Relation: "child"}
	opts := storage.ReadOptions{}
	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("workgroup:13110", "child", "workgroup:a")},
		{Key: tuple.NewTupleKey("workgroup:13110", "child", "workgroup:b")},
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once

	mockDatastore.EXPECT().Read(gomock.Any(), storeID, filter, opts).DoAndReturn(
		func(_ context.Context, _ string, _ storage.ReadFilter, _ storage.ReadOptions) (storage.TupleIterator, error) {
			startOnce.Do(func() { close(started) })
			<-release
			return storage.NewStaticTupleIterator(tuples), nil
		},
	).Times(1)

	const n = 8
	var done sync.WaitGroup
	done.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer done.Done()
			iter, err := reader.Read(ctx, storeID, filter, opts)
			if err != nil {
				t.Errorf("concurrent Read failed: %v", err)
				return
			}
			defer iter.Stop()
			for {
				if _, err := iter.Next(ctx); err != nil {
					if !errors.Is(err, storage.ErrIteratorDone) {
						t.Errorf("Next failed: %v", err)
					}
					return
				}
			}
		}()
	}

	// Release the leader only once it is in flight, guaranteeing the other callers
	// coalesce onto it rather than each starting their own read.
	<-started
	close(release)

	done.Wait()
	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: V2 read coalescing (CachedTupleReader.missRead)
// ─────────────────────────────────────────────────────────────────────────────
//
// These benchmarks characterize the CPU / allocation / latency cost of the
// intra-request read-coalescing fix in CachedTupleReader. The recursive check
// resolver fans out many identical reads of the same tupleset (e.g.
// workgroup:X#child) concurrently within a single Check; without coalescing each
// races the not-yet-populated cache and issues its own datastore read. missRead
// makes the first caller ("leader") stream the read while concurrent callers
// ("followers") block on the leader's completion and then serve from the cache it
// populated — collapsing N reads to 1.
//
// Methodology mirrors sharediterator.BenchmarkIteratorDatastoreReadLatencyWith-
// DifferentLoads (and the "Shared Iterator Optimization" benchmark study): a
// fixed slow backend call (5ms) makes the number of REAL datastore reads
// (db_calls) the coalescing signal, and per-call latency percentiles are
// reported alongside. Unlike the shared iterator — which keeps iterators live to
// hand out per-consumer clones (copying, reference counting, a TTL'd admission
// cache with idle eviction, all of which run continuously) — coalescing here
// adds only a per-miss channel + a request-scoped sync.Map entry, and followers
// read the ordinary cache entry rather than a cloned iterator. These benchmarks
// quantify that difference.
//
// Run with:
//   go test -bench=BenchmarkCachedTupleReader -benchmem ./pkg/storage/storagewrappers/...

const (
	coalesceBenchDelay = 5 * time.Millisecond // simulated slow backend, as in the study
	coalesceBenchStore = "01JP560HBHDCK4SK3K14CFAK4G"
)

// benchCoalesceReader is a minimal RelationshipTupleReader that counts datastore
// reads and optionally simulates backend latency. It stands in for the delegate
// so db_calls reflects exactly how many reads reached "storage".
type benchCoalesceReader struct {
	tuples  []*openfgav1.Tuple
	delay   time.Duration
	dbCalls atomic.Int64
}

func (r *benchCoalesceReader) Read(_ context.Context, _ string, _ storage.ReadFilter, _ storage.ReadOptions) (storage.TupleIterator, error) {
	r.dbCalls.Add(1)
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	items := make([]*openfgav1.Tuple, len(r.tuples))
	copy(items, r.tuples)
	return storage.NewStaticTupleIterator(items), nil
}

func (r *benchCoalesceReader) ReadUserTuple(context.Context, string, storage.ReadUserTupleFilter, storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	return nil, errors.New("not implemented")
}

func (r *benchCoalesceReader) ReadPage(context.Context, string, storage.ReadFilter, storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	return nil, "", errors.New("not implemented")
}

func (r *benchCoalesceReader) ReadUsersetTuples(context.Context, string, storage.ReadUsersetTuplesFilter, storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	return nil, errors.New("not implemented")
}

func (r *benchCoalesceReader) ReadStartingWithUser(context.Context, string, storage.ReadStartingWithUserFilter, storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	return nil, errors.New("not implemented")
}

// drainAndStop fully consumes an iterator (so the leader's eager-flush settle
// path fires) and stops it.
func drainAndStop(ctx context.Context, iter storage.TupleIterator) error {
	defer iter.Stop()
	for {
		if _, err := iter.Next(ctx); err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				return nil
			}
			return err
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Coalescing effectiveness + latency under concurrent identical reads
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkCachedTupleReaderReadCoalescingWithDifferentLoads measures, per
// concurrency level, how many datastore reads actually happen (db_calls) and the
// per-call latency distribution when N goroutines issue the SAME read against a
// single request-scoped CachedTupleReader.
//
//   - Coalesced (default consistency): all N callers share one datastore read.
//     db_calls should track the iteration count (≈1 per iteration) regardless of
//     concurrency — the "traditional single-flight" ideal from the study.
//   - HigherConsistency: the cache (and therefore coalescing) is bypassed, so
//     every caller reads the backend. This is the "no coalescing" baseline; its
//     db_calls scale with concurrency.
//
// -benchmem additionally reports B/op and allocs/op so the per-request overhead
// of the coalescing machinery is visible and can be seen to grow gently (a
// channel + a sync.Map entry per follower), not with per-consumer iterator
// clones.
func BenchmarkCachedTupleReaderReadCoalescingWithDifferentLoads(b *testing.B) {
	concurrencyLevels := []int{1, 10, 50, 100, 200, 500}
	tuples := createTestTuples(50)

	for _, higherConsistency := range []bool{false, true} {
		mode := "Coalesced"
		if higherConsistency {
			mode = "HigherConsistency"
		}
		for _, concurrency := range concurrencyLevels {
			b.Run(fmt.Sprintf("%s/Concurrency_%d", mode, concurrency), func(b *testing.B) {
				runCoalesceLoad(b, tuples, concurrency, higherConsistency)
			})
		}
	}
}

func runCoalesceLoad(b *testing.B, tuples []*openfgav1.Tuple, concurrency int, higherConsistency bool) {
	b.Helper()
	ctx := context.Background()

	// Shared server-level resources (as in production: shared across requests).
	cache, err := storage.NewInMemoryLRUCache[any]()
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Stop()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	backend := &benchCoalesceReader{tuples: tuples, delay: coalesceBenchDelay}

	opts := storage.ReadOptions{}
	if higherConsistency {
		opts.Consistency = storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		}
	}

	var latencies []time.Duration
	var mu sync.Mutex

	b.ReportAllocs()
	b.ResetTimer()

	iterKey := 0
	for b.Loop() {
		iterKey++
		// One CachedTupleReader per iteration models one Check request (a fresh
		// inflight map). A unique object per iteration guarantees every iteration
		// is a genuine cache miss that must coalesce, rather than a trivial hit on
		// the entry a previous iteration left behind.
		reader := NewCachedTupleReader(
			ctx, backend, cache, 10000, 10*time.Second, sf, wg, DefaultDrainTimeout,
			WithMethod(apimethod.Check.String()),
		)
		filter := storage.ReadFilter{Object: fmt.Sprintf("workgroup:%d", iterKey), Relation: "child"}

		var callers sync.WaitGroup
		callers.Add(concurrency)
		for j := 0; j < concurrency; j++ {
			go func() {
				defer callers.Done()
				start := time.Now()
				iter, err := reader.Read(ctx, coalesceBenchStore, filter, opts)
				elapsed := time.Since(start)
				if err != nil {
					b.Errorf("Read failed: %v", err)
					return
				}
				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()
				if err := drainAndStop(ctx, iter); err != nil {
					b.Errorf("drain failed: %v", err)
				}
			}()
		}
		callers.Wait()
	}

	b.StopTimer()
	wg.Wait() // let any background drains finish before reporting

	b.ReportMetric(float64(backend.dbCalls.Load()), "db_calls")
	reportLatencies(b, latencies)
}

// reportLatencies reports avg/min/max/p95/p99 in microseconds, matching the
// sharediterator load benchmark's output.
func reportLatencies(b *testing.B, latencies []time.Duration) {
	b.Helper()
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	avg := total / time.Duration(len(latencies))
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(avg.Microseconds()), "avg_latency_us")
	b.ReportMetric(float64(p95.Microseconds()), "p95_latency_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_latency_us")
	b.ReportMetric(float64(latencies[len(latencies)-1].Microseconds()), "max_latency_us")
	b.ReportMetric(float64(latencies[0].Microseconds()), "min_latency_us")
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-miss CPU / allocation overhead of the coalescing path
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkCachedTupleReaderMissOverhead isolates the CPU/allocation cost of a
// single (uncontended) cache-miss read, with no simulated backend latency, so
// the coalescing bookkeeping is not hidden behind I/O. Three baselines let a
// reviewer attribute cost precisely:
//
//   - CoalescedMiss:      reader.Read on a miss — cache lookup + inflight
//     bookkeeping (channel + sync.Map entry) + CachingIterator.
//   - RawCachingIterator: newCachingIterator consumed directly — the pre-existing
//     miss path WITHOUT the cache lookup or inflight machinery.
//   - HigherConsistency:  reader.Read that bypasses the cache entirely — a bare
//     delegate read with no caching/coalescing at all.
//
// (CoalescedMiss − RawCachingIterator) is the marginal cost the coalescing fix
// adds per miss: one cache Get plus the inflight channel + map entry.
func BenchmarkCachedTupleReaderMissOverhead(b *testing.B) {
	ctx := context.Background()
	tuples := createTestTuples(50)
	backend := &benchCoalesceReader{tuples: tuples} // no delay: measure pure CPU

	b.Run("CoalescedMiss", func(b *testing.B) {
		cache, err := storage.NewInMemoryLRUCache[any]()
		if err != nil {
			b.Fatal(err)
		}
		defer cache.Stop()
		sf := &singleflight.Group{}
		wg := &sync.WaitGroup{}
		reader := NewCachedTupleReader(
			ctx, backend, cache, 10000, 10*time.Second, sf, wg, DefaultDrainTimeout,
			WithMethod(apimethod.Check.String()),
		)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Unique object per op ⇒ every op is a genuine miss through the full
			// coalescing path (LoadOrStore of a fresh inflight entry, etc.).
			filter := storage.ReadFilter{Object: fmt.Sprintf("workgroup:%d", i), Relation: "child"}
			iter, err := reader.Read(ctx, coalesceBenchStore, filter, storage.ReadOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if err := drainAndStop(ctx, iter); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		wg.Wait()
	})

	b.Run("RawCachingIterator", func(b *testing.B) {
		cache, err := storage.NewInMemoryLRUCache[any]()
		if err != nil {
			b.Fatal(err)
		}
		defer cache.Stop()
		sf := &singleflight.Group{}
		wg := &sync.WaitGroup{}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inner := storage.NewStaticTupleIterator(tuples)
			iter := newCachingIterator(
				inner, cache, testCacheKey(fmt.Sprintf("workgroup:%d", i)), 10000, 10*time.Second,
				DefaultDrainTimeout, sf, wg, "workgroup", "child", "Read", apimethod.Check.String(),
			)
			if err := drainAndStop(ctx, iter); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		wg.Wait()
	})

	b.Run("HigherConsistency", func(b *testing.B) {
		cache, err := storage.NewInMemoryLRUCache[any]()
		if err != nil {
			b.Fatal(err)
		}
		defer cache.Stop()
		sf := &singleflight.Group{}
		wg := &sync.WaitGroup{}
		reader := NewCachedTupleReader(
			ctx, backend, cache, 10000, 10*time.Second, sf, wg, DefaultDrainTimeout,
			WithMethod(apimethod.Check.String()),
		)
		opts := storage.ReadOptions{Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		}}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			filter := storage.ReadFilter{Object: fmt.Sprintf("workgroup:%d", i), Relation: "child"}
			iter, err := reader.Read(ctx, coalesceBenchStore, filter, opts)
			if err != nil {
				b.Fatal(err)
			}
			if err := drainAndStop(ctx, iter); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		wg.Wait()
	})
}
