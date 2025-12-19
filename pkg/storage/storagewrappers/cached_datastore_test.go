package storagewrappers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers/storagewrappersutil"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestFindInCache(t *testing.T) {
	ctx := context.Background()

	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()
	key := "key"
	invalidEntityKeys := []string{storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, "object", "relation")}

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).Return(nil),
		)
		_, ok := findInCache(ds.cache, key, storage.GetInvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_no_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
		)
		_, ok := findInCache(ds.cache, key, storage.GetInvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_bad_result", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).Return("invalid"),
		)
		_, ok := findInCache(ds.cache, key, storage.GetInvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(5 * time.Second)}),
			mockCache.EXPECT().Delete(key),
		)
		_, ok := findInCache(ds.cache, key, storage.GetInvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_stale_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(-5 * time.Second)}),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
		)
		_, ok := findInCache(ds.cache, key, storage.GetInvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_invalidation_incorrect_type", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).
				Return("invalid"),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
		)
		_, ok := findInCache(ds.cache, key, storage.GetInvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_invalid_entity", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(5 * time.Second)}),
			mockCache.EXPECT().Delete(key),
		)
		_, ok := findInCache(ds.cache, key, storage.GetInvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_invalid_entity_stale", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(-5 * time.Second)}),
		)
		_, ok := findInCache(ds.cache, key, storage.GetInvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_invalid_entity_stale_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).
				Return("invalid"),
		)
		_, ok := findInCache(ds.cache, key, storage.GetInvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
}

func TestReadStartingWithUser(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:*"),
	}
	var tuples []*openfgav1.Tuple
	var cachedTuples []*storage.TupleRecord
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
		_, objectID := tuple.SplitObject(tk.GetObject())
		_, userObjectID, userRelation := tuple.ToUserParts(tk.GetUser())
		cachedTuples = append(cachedTuples, &storage.TupleRecord{
			ObjectID:       objectID,
			Relation:       tk.GetRelation(),
			UserObjectType: "",
			UserObjectID:   userObjectID,
			UserRelation:   userRelation,
			InsertedAt:     ts.AsTime(),
		})
	}

	options := storage.ReadStartingWithUserOptions{}
	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:5"},
			{Object: "user:*"},
		},
		ObjectIDs: storage.NewSortedSet("1"),
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	invalidEntityKeys := storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{"user:5", "user:*"}, filter.ObjectType)

	t.Run("cache_miss", func(t *testing.T) {
		cacheKey, err := storagewrappersutil.ReadStartingWithUserKey(storeID, filter) // first find to determine cache miss
		require.NoError(t, err)
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),                                    // find while stopping
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil), // check if store invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),                        // check if entity invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKeys[1]).Return(nil),                        // check if entity invalidated before writing
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			}),
			mockCache.EXPECT().Delete(invalidEntityKeys[0]),
			mockCache.EXPECT().Delete(invalidEntityKeys[1]),
		)

		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_hit", func(t *testing.T) {
		t.Run("without_user_filter_relation", func(t *testing.T) {
			gomock.InOrder(
				mockCache.EXPECT().Get(gomock.Any()).Return(&storage.TupleIteratorCacheEntry{Tuples: cachedTuples}),
				mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
				mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
				mockCache.EXPECT().Get(invalidEntityKeys[1]).Return(nil),
			)

			iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
			require.NoError(t, err)

			var actual []*openfgav1.Tuple

			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					require.Fail(t, "no error was expected")
					break
				}
				actual = append(actual, tuple)
			}

			if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("with_user_filter_relation", func(t *testing.T) {
			filterWithUserRelation := storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "viewer",
				UserFilter: []*openfgav1.ObjectRelation{
					{Object: "user:5", Relation: "viewer"}, // one with Relation
					{Object: "user:*"},
				},
				ObjectIDs: storage.NewSortedSet("1"),
			}
			invalidEntityKeysWithRelation := storage.GetInvalidIteratorByUserObjectTypeCacheKeys(
				storeID, []string{"user:5#viewer", "user:*"}, filterWithUserRelation.ObjectType,
			)
			gomock.InOrder(
				mockCache.EXPECT().Get(gomock.Any()).Return(&storage.TupleIteratorCacheEntry{Tuples: cachedTuples}),
				mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),

				// These should not be found, the cache_controller does not include relations
				// in the invalidation record keys when calling invalidateIteratorCacheByUserAndObjectType
				mockCache.EXPECT().Get(invalidEntityKeysWithRelation[0]).Return(nil),
				mockCache.EXPECT().Get(invalidEntityKeysWithRelation[1]).Return(nil),
			)

			iter, err := ds.ReadStartingWithUser(ctx, storeID, filterWithUserRelation, options)
			require.NoError(t, err)

			var actual []*openfgav1.Tuple

			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					require.Fail(t, "no error was expected")
					break
				}
				actual = append(actual, tuple)
			}

			if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	})

	t.Run("cache_empty_response", func(t *testing.T) {
		cacheKey, err := storagewrappersutil.ReadStartingWithUserKey(storeID, filter) // first find to determine cache miss
		require.NoError(t, err)
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey),
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),                                    // find while stopping
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil), // check if store invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),                        // check if entity invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKeys[1]).Return(nil),                        // check if entity invalidated before writing
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				require.Empty(t, entry.Tuples)
			}),
			mockCache.EXPECT().Delete(invalidEntityKeys[0]),
			mockCache.EXPECT().Delete(invalidEntityKeys[1]),
		)

		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		require.Empty(t, actual)
	})

	t.Run("higher_consistency", func(t *testing.T) {
		opts := storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}

		gomock.InOrder(
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, filter, opts).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, opts)
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestReadUsersetTuples(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:1", "viewer", "user:2"),
		tuple.NewTupleKey("document:1", "viewer", "user:3"),
		tuple.NewTupleKey("document:1", "viewer", "user:4"),
		tuple.NewTupleKey("document:1", "viewer", "user:*"),
		tuple.NewTupleKey("document:1", "viewer", "company:1#viewer"),
	}
	var tuples []*openfgav1.Tuple
	var cachedTuples []*storage.TupleRecord
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
		userObjectType, userObjectID, userRelation := tuple.ToUserParts(tk.GetUser())
		cachedTuples = append(cachedTuples, &storage.TupleRecord{
			UserObjectType: userObjectType,
			UserObjectID:   userObjectID,
			UserRelation:   userRelation,
			InsertedAt:     ts.AsTime(),
		})
	}

	options := storage.ReadUsersetTuplesOptions{}
	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			typesystem.DirectRelationReference("company", "viewer"),
			typesystem.WildcardRelationReference("user"),
		},
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	invalidEntityKey := storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, filter.Object, filter.Relation)
	cacheKey := storagewrappersutil.ReadUsersetTuplesKey(storeID, filter)

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()),
			mockDatastore.EXPECT().
				ReadUsersetTuples(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),                                    // find while stopping
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil), // check if store invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),                            // check if entity invalidated before writing
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			}),
			mockCache.EXPECT().Delete(invalidEntityKey),
		)

		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_hit", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(&storage.TupleIteratorCacheEntry{Tuples: cachedTuples}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
		)

		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_empty_response", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockDatastore.EXPECT().
				ReadUsersetTuples(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				require.Empty(t, entry.Tuples)
			}),
			mockCache.EXPECT().Delete(invalidEntityKey),
		)

		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		require.Empty(t, actual)
	})

	t.Run("higher_consistency", func(t *testing.T) {
		opts := storage.ReadUsersetTuplesOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}

		gomock.InOrder(
			mockDatastore.EXPECT().
				ReadUsersetTuples(gomock.Any(), storeID, filter, opts).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, opts)
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("license:1", "owner", "company:1"),
		tuple.NewTupleKey("license:1", "owner", "company:2"),
	}
	var tuples []*openfgav1.Tuple
	var cachedTuples []*storage.TupleRecord
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
		userObjectType, userObjectID, userRelation := tuple.ToUserParts(tk.GetUser())
		cachedTuples = append(cachedTuples, &storage.TupleRecord{
			UserObjectType: userObjectType,
			UserObjectID:   userObjectID,
			UserRelation:   userRelation,
			InsertedAt:     ts.AsTime(),
		})
	}

	tk := tuple.NewTupleKey("license:1", "owner", "")
	filter := storage.ReadFilter{
		Object:   "license:1",
		Relation: "owner",
		User:     "",
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	invalidEntityKey := storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, tk.GetObject(), tk.GetRelation())
	cacheKey := storagewrappersutil.ReadKey(storeID, tk)

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, filter, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			}),
			mockCache.EXPECT().Delete(invalidEntityKey),
		)

		iter, err := ds.Read(ctx, storeID, filter, storage.ReadOptions{})
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_hit", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(&storage.TupleIteratorCacheEntry{Tuples: cachedTuples}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
		)

		iter, err := ds.Read(ctx, storeID, filter, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_empty_response", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey),
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, filter, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil),
			mockCache.EXPECT().Get(cacheKey),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				require.Empty(t, entry.Tuples)
			}),
			mockCache.EXPECT().Delete(invalidEntityKey),
		)

		iter, err := ds.Read(ctx, storeID, filter, storage.ReadOptions{})
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		require.Empty(t, actual)
	})

	t.Run("higher_consistency", func(t *testing.T) {
		opts := storage.ReadOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}

		gomock.InOrder(
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, filter, opts).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.Read(ctx, storeID, filter, opts)
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("tuple_key_is_not_from_ttu", func(t *testing.T) {
		invalidObjectFilter := storage.ReadFilter{
			Object:   "invalid",
			Relation: filter.Relation,
			User:     filter.User,
		}

		gomock.InOrder(
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, invalidObjectFilter, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.Read(ctx, storeID, invalidObjectFilter, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestDatastoreIteratorError(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()

	filter := storage.ReadFilter{
		Object:   "license:1",
		Relation: "owner",
		User:     "",
	}

	gomock.InOrder(
		mockCache.EXPECT().Get(gomock.Any()),
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, filter, storage.ReadOptions{}).
			Return(nil, storage.ErrNotFound),
	)

	_, err := ds.Read(ctx, storeID, filter, storage.ReadOptions{})
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestCachedIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctx := context.Background()

	tuples := []*openfgav1.Tuple{
		{
			Key:       tuple.NewTupleKey("document:doc1", "viewer", "bill"),
			Timestamp: timestamppb.New(time.Now()),
		},
		{
			Key:       tuple.NewTupleKey("document:doc2", "editor", "bob"),
			Timestamp: timestamppb.New(time.Now()),
		},
	}

	cachedTuples := []*storage.TupleRecord{
		{
			ObjectID:       "doc1",
			ObjectType:     "document",
			Relation:       "viewer",
			UserObjectType: "",
			UserObjectID:   "bill",
			UserRelation:   "",
			InsertedAt:     tuples[0].GetTimestamp().AsTime(),
		},
		{
			ObjectID:       "doc2",
			ObjectType:     "document",
			Relation:       "editor",
			UserObjectType: "",
			UserObjectID:   "bob",
			UserRelation:   "",
			InsertedAt:     tuples[1].GetTimestamp().AsTime(),
		},
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	t.Run("next_yielding_error_discards_results", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              mocks.NewErrorTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []string{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		_, err = iter.Next(ctx)
		require.NoError(t, err)

		_, err = iter.Next(ctx)
		require.Error(t, err)

		iter.Stop()
		require.Nil(t, iter.tuples)
		cachedResults := cache.Get(cacheKey)
		require.Nil(t, cachedResults)
	})

	t.Run("next_at_max_discards_results", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []string{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		_, err = iter.Next(ctx)
		require.NoError(t, err)

		require.Nil(t, iter.tuples)
	})

	t.Run("calling_stop_doesnt_cache_due_to_size_foreground", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []string{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		var actual []*openfgav1.Tuple

		for {
			tk, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tk)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.Nil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)
	})

	t.Run("calling_stop_doesnt_cache_due_to_size_background", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              mocks.NewErrorTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []string{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.Nil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)
	})

	t.Run("calling_stop_caches_in_foreground", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []string{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		var actual []*openfgav1.Tuple

		for {
			tk, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tk)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)

		entry := cachedResults.(*storage.TupleIteratorCacheEntry)

		if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("calling_stop_caches_in_background", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []string{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)

		entry := cachedResults.(*storage.TupleIteratorCacheEntry)

		if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("parent_context_cancelled_still_caches_in_background", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []string{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = iter.Next(cancelledCtx)
		require.ErrorIs(t, err, context.Canceled)

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)

		entry := cachedResults.(*storage.TupleIteratorCacheEntry)

		if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("prevent_draining_if_already_cached", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		store := ulid.Make().String()
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](mockController)
		tupleRecord := &storage.TupleIteratorCacheEntry{
			Tuples:       cachedTuples,
			LastModified: time.Now().Add(-1 * time.Second),
		}
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(tupleRecord),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(store)).Return(nil),
		)

		var wg sync.WaitGroup

		mockedIter := &mockCalledTupleIterator{
			iter: storage.NewStaticTupleIterator(tuples),
		}

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              mockedIter,
			store:             store,
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidStoreKey:   storage.GetInvalidIteratorCacheKey(store),
			invalidEntityKeys: []string{},
			cache:             mockCache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			iter.Stop()
		}()

		wg.Wait()
		iter.wg.Wait()

		require.Zero(t, mockedIter.nextCalled)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)
	})

	t.Run("prevent_draining_if_queried_before_invalidation_time", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		store := ulid.Make().String()
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](mockController)

		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(store)).Return(&storage.InvalidEntityCacheEntry{
				LastModified: time.Now().Add(1 * time.Minute),
			}),
		)

		var wg sync.WaitGroup

		mockedIter := &mockCalledTupleIterator{
			iter: storage.NewStaticTupleIterator(tuples),
		}

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              mockedIter,
			store:             store,
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidStoreKey:   storage.GetInvalidIteratorCacheKey(store),
			invalidEntityKeys: []string{},
			cache:             mockCache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			initializedAt:     time.Now(),
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			iter.Stop()
		}()

		wg.Wait()
		iter.wg.Wait()

		require.Zero(t, mockedIter.nextCalled)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)
	})

	t.Run("prevent_draining_on_the_same_iterator_across_concurrent_requests", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		store := ulid.Make().String()
		for i := 0; i < 100; i++ {
			mockController := gomock.NewController(t)
			defer mockController.Finish()

			mockCache := mocks.NewMockInMemoryCache[any](mockController)

			mockCache.EXPECT().Get(cacheKey).AnyTimes().Return(nil)
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(store)).AnyTimes().Return(nil)
			mockCache.EXPECT().Set(cacheKey, gomock.Any(), ttl).AnyTimes()
			mockCache.EXPECT().Delete(gomock.Any()).AnyTimes()

			sf := &singleflight.Group{}

			var wg sync.WaitGroup

			mockedIter1 := &mockCalledTupleIterator{
				iter: storage.NewStaticTupleIterator(tuples),
			}

			iter1 := &cachedIterator{
				ctx:               ctx,
				iter:              mockedIter1,
				operation:         "operation",
				tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
				cacheKey:          cacheKey,
				invalidStoreKey:   storage.GetInvalidIteratorCacheKey(store),
				invalidEntityKeys: []string{},
				cache:             mockCache,
				maxResultSize:     maxCacheSize,
				ttl:               ttl,
				sf:                sf,
				wg:                &sync.WaitGroup{},
				objectType:        "",
				objectID:          "",
				relation:          "",
				userType:          "",
				logger:            logger.NewNoopLogger(),
			}

			mockedIter2 := &mockCalledTupleIterator{
				iter: storage.NewStaticTupleIterator(tuples),
			}

			iter2 := &cachedIterator{
				ctx:               ctx,
				iter:              mockedIter2,
				operation:         "operation",
				tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
				cacheKey:          cacheKey,
				invalidStoreKey:   storage.GetInvalidIteratorCacheKey(store),
				invalidEntityKeys: []string{},
				cache:             mockCache,
				maxResultSize:     maxCacheSize,
				ttl:               ttl,
				sf:                sf,
				wg:                &sync.WaitGroup{},
				objectType:        "",
				objectID:          "",
				relation:          "",
				userType:          "",
				logger:            logger.NewNoopLogger(),
			}

			wg.Add(2)

			go func() {
				defer wg.Done()

				iter1.Stop()
				iter1.wg.Wait()
			}()

			go func() {
				defer wg.Done()

				iter2.Stop()
				iter2.wg.Wait()
			}()

			wg.Wait()

			require.GreaterOrEqual(t, mockedIter1.nextCalled, 0)
			require.GreaterOrEqual(t, mockedIter2.nextCalled, 0)
			require.GreaterOrEqual(t, mockedIter1.nextCalled+mockedIter2.nextCalled, 3)
		}
	})
}

type mockCalledTupleIterator struct {
	iter        storage.TupleIterator
	nextCalled  int
	headCalled  int
	closeCalled int
}

func (s *mockCalledTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	s.nextCalled++
	return s.iter.Next(ctx)
}

func (s *mockCalledTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	s.headCalled++
	return s.iter.Head(ctx)
}

func (s *mockCalledTupleIterator) Stop() {
	s.closeCalled++
	s.iter.Stop()
}
