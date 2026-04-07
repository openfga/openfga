package storagewrappers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
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

	cacheKey := buildReadUsersetTuplesCacheKey(storeID, filter)

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

	cacheKey := buildReadUsersetTuplesCacheKey(storeID, filter)
	entityInvalidKey := storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "viewer")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Check invalidation keys
	mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil).Times(1)
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

	cacheKey := buildReadUsersetTuplesCacheKey(storeID, filter)

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - returns invalidation entry that is newer
	mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(invalidEntry).Times(1)

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

	cacheKey := buildReadUsersetTuplesCacheKey(storeID, filter)
	entityInvalidKey := storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "viewer")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - no store-level invalidation
	mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil).Times(1)

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

	cacheKey := buildReadUsersetTuplesCacheKey(storeID, filter)
	entityInvalidKey := storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "viewer")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - invalidation is older than cache
	mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(invalidEntry).Times(1)

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
	cacheKey := buildReadUsersetTuplesCacheKey(storeID, filter)

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

	cacheKey := buildReadCacheKey(storeID, filter)

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

	cacheKey := buildReadCacheKey(storeID, filter)
	entityInvalidKey := storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "parent")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)
	mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil).Times(1)
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

	cacheKey := buildReadCacheKey(storeID, filter)
	entityInvalidKey := storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, "document:1", "parent")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - no store-level invalidation
	mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil).Times(1)

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

	cacheKey := buildReadStartingWithUserCacheKey(storeID, filter)

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

	cacheKey := buildReadStartingWithUserCacheKey(storeID, filter)
	userInvalidKeys := storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{"user:alice"}, "document")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)
	mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil).Times(1)
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

	cacheKey := buildReadStartingWithUserCacheKey(storeID, filter)

	// NOTE: buildInvalidationKeysForUser currently uses f.GetObject() which drops
	// the "#member" relation. This means the invalidation key uses "group:eng"
	// instead of "group:eng#member". This is a known bug — when fixed, the
	// expected invalidation key below should change to use "group:eng#member".
	userInvalidKeys := storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{"group:eng"}, "document")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Check invalidation keys
	mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil).Times(1)
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

	cacheKey := buildReadStartingWithUserCacheKey(storeID, filter)
	userInvalidKeys := storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{"user:alice"}, "document")

	// Cache hit
	mockCache.EXPECT().Get(cacheKey).Return(cachedEntry).Times(1)

	// Store invalidation check - no store-level invalidation
	mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil).Times(1)

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

	key1 := buildReadUsersetTuplesCacheKey("store", filter1)
	key2 := buildReadUsersetTuplesCacheKey("store", filter2)
	key3 := buildReadUsersetTuplesCacheKey("store", filter3)

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

	keyWithout := buildReadUsersetTuplesCacheKey("store", filterWithoutCond)
	keyWith := buildReadUsersetTuplesCacheKey("store", filterWithCond)

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
	cacheKey := buildReadUsersetTuplesCacheKey(storeID, filter)
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
