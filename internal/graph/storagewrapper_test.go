package graph

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestFindInCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	ds := NewCachedDatastore(mockDatastore, mockCache, maxSize, ttl)

	storeID := ulid.Make().String()
	key := "key"
	invalidEntityKeys := []string{"invalid_key"}

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).Return(nil),
		)
		_, ok := ds.findInCache(storeID, key, invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_no_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*openfgav1.Tuple{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
		)
		_, ok := ds.findInCache(storeID, key, invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*openfgav1.Tuple{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(5 * time.Second)}),
		)
		_, ok := ds.findInCache(storeID, key, invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_stale_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*openfgav1.Tuple{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(-5 * time.Second)}),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
		)
		_, ok := ds.findInCache(storeID, key, invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_invalid_entity", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*openfgav1.Tuple{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(5 * time.Second)}),
		)
		_, ok := ds.findInCache(storeID, key, invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_invalid_entity_stale", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*openfgav1.Tuple{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(-5 * time.Second)}),
		)
		_, ok := ds.findInCache(storeID, key, invalidEntityKeys)
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
	ds := NewCachedDatastore(mockDatastore, mockCache, maxSize, ttl)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("company:1", "viewer", "user:1"),
		tuple.NewTupleKey("company:1", "viewer", "user:2"),
		tuple.NewTupleKey("company:1", "viewer", "user:3"),
		tuple.NewTupleKey("company:1", "viewer", "user:4"),
		tuple.NewTupleKey("company:1", "viewer", "user:5"),
		tuple.NewTupleKey("license:1", "viewer", "company:1#viewer"),
	}
	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		tuples = append(tuples, &openfgav1.Tuple{Key: tk})
	}

	options := storage.ReadStartingWithUserOptions{}
	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:5"},
		},
		ObjectIDs: storage.NewSortedSet("1"),
	}

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()),
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				require.Equal(t, tuples, entry.Tuples)
			}),
			mockCache.EXPECT().Delete(storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{"user:5#"}, filter.ObjectType)[0]),
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

		require.Equal(t, tuples, actual)
	})

	t.Run("cache_hit", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()).Return(&storage.TupleIteratorCacheEntry{Tuples: tuples}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{"user:5#"}, filter.ObjectType)[0]).Return(nil),
		)

		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
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

		require.Equal(t, tuples, actual)
	})

	t.Run("cache_empty_response", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()),
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				require.Equal(t, []*openfgav1.Tuple{}, entry.Tuples)
			}),
			mockCache.EXPECT().Delete(storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{"user:5#"}, filter.ObjectType)[0]),
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

		require.Equal(t, tuples, actual)
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
	ds := NewCachedDatastore(mockDatastore, mockCache, maxSize, ttl)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("company:1", "viewer", "user:1"),
		tuple.NewTupleKey("company:1", "viewer", "user:2"),
		tuple.NewTupleKey("company:1", "viewer", "user:3"),
		tuple.NewTupleKey("company:1", "viewer", "user:4"),
		tuple.NewTupleKey("company:1", "viewer", "user:5"),
		tuple.NewTupleKey("license:1", "viewer", "company:1#viewer"),
	}
	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		tuples = append(tuples, &openfgav1.Tuple{Key: tk})
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

	mockDatastore.EXPECT().
		Write(gomock.Any(), storeID, nil, tks).Return(nil)
	err := ds.Write(ctx, storeID, nil, tks)
	require.NoError(t, err)

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()),
			mockDatastore.EXPECT().
				ReadUsersetTuples(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				require.Equal(t, tuples, entry.Tuples)
			}),
			mockCache.EXPECT().Delete(storage.GetInvalidIteratorByObjectRelationCacheKeys(storeID, filter.Object, filter.Relation)[0]),
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

		require.Equal(t, tuples, actual)
	})

	t.Run("cache_hit", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()).Return(&storage.TupleIteratorCacheEntry{Tuples: tuples}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorByObjectRelationCacheKeys(storeID, filter.Object, filter.Relation)[0]).Return(nil),
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

		require.Equal(t, tuples, actual)
	})

	t.Run("cache_empty_response", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()),
			mockDatastore.EXPECT().
				ReadUsersetTuples(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				require.Equal(t, []*openfgav1.Tuple{}, entry.Tuples)
			}),
			mockCache.EXPECT().Delete(storage.GetInvalidIteratorByObjectRelationCacheKeys(storeID, filter.Object, filter.Relation)[0]),
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

		require.Equal(t, tuples, actual)
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
	ds := NewCachedDatastore(mockDatastore, mockCache, maxSize, ttl)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("company:1", "viewer", "user:1"),
		tuple.NewTupleKey("license:1", "owner", "company:1"),
		tuple.NewTupleKey("company:1", "viewer", "user:3"),
		tuple.NewTupleKey("company:1", "viewer", "user:4"),
		tuple.NewTupleKey("company:1", "viewer", "user:5"),
	}
	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		tuples = append(tuples, &openfgav1.Tuple{Key: tk})
	}

	tk := tuple.NewTupleKey("license:1", "owner", "")

	mockDatastore.EXPECT().
		Write(gomock.Any(), storeID, nil, tks).Return(nil)

	err := ds.Write(ctx, storeID, nil, tks)
	require.NoError(t, err)

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()),
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				require.Equal(t, tuples, entry.Tuples)
			}),
			mockCache.EXPECT().Delete(storage.GetInvalidIteratorByObjectRelationCacheKeys(storeID, tk.GetObject(), tk.GetRelation())[0]),
		)

		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
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

		require.Equal(t, tuples, actual)
	})

	t.Run("cache_hit", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()).Return(&storage.TupleIteratorCacheEntry{Tuples: tuples}),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(storage.GetInvalidIteratorByObjectRelationCacheKeys(storeID, tk.GetObject(), tk.GetRelation())[0]),
		)

		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
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

		require.Equal(t, tuples, actual)
	})

	t.Run("cache_empty_response", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()),
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(k string, entry *storage.TupleIteratorCacheEntry, ttl time.Duration) {
				require.Equal(t, []*openfgav1.Tuple{}, entry.Tuples)
			}),
			mockCache.EXPECT().Delete(storage.GetInvalidIteratorByObjectRelationCacheKeys(storeID, tk.GetObject(), tk.GetRelation())[0]),
		)

		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
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
				Read(gomock.Any(), storeID, tk, opts).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.Read(ctx, storeID, tk, opts)
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

		require.Equal(t, tuples, actual)
	})

	t.Run("tuple_key_is_not_from_ttu", func(t *testing.T) {
		tupleKey := &openfgav1.TupleKey{
			Relation: tk.GetRelation(),
			Object:   "invalid",
		}
		gomock.InOrder(
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, tupleKey, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.Read(ctx, storeID, tupleKey, storage.ReadOptions{})
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

		require.Equal(t, tuples, actual)
	})
}

func TestCloseDatastore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().Close().Times(1).Return()

	maxSize := 10
	ttl := 5 * time.Hour
	ds := NewCachedDatastore(mockDatastore, mockCache, maxSize, ttl)
	ds.Close()
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
	ds := NewCachedDatastore(mockDatastore, mockCache, maxSize, ttl)

	storeID := ulid.Make().String()

	tk := tuple.NewTupleKey("license:1", "owner", "")

	gomock.InOrder(
		mockCache.EXPECT().Get(gomock.Any()),
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(nil, storage.ErrNotFound),
	)

	_, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
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

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	t.Run("next_yielding_error_discards_results", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		defer cache.Stop()

		iter := &cachedIterator{
			iter:          mocks.NewErrorTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
			sf:            &singleflight.Group{},
		}

		_, err := iter.Next(ctx)
		require.NoError(t, err)

		_, err = iter.Next(ctx)
		require.Error(t, err)

		require.Nil(t, iter.tuples)

		iter.Stop()

		iter.wg.Wait()
		cachedResults := cache.Get(cacheKey)
		require.Nil(t, cachedResults)
	})

	t.Run("calling_stop_doesnt_cache_due_to_size_foreground", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		defer cache.Stop()

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
			sf:            &singleflight.Group{},
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

		require.Equal(t, tuples, actual)

		iter.Stop()

		iter.wg.Wait()
		cachedResults := cache.Get(cacheKey)
		require.Nil(t, cachedResults)
	})

	t.Run("calling_stop_doesnt_cache_due_to_size_background", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		defer cache.Stop()

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
			sf:            &singleflight.Group{},
		}

		iter.Stop()

		iter.wg.Wait()
		cachedResults := cache.Get(cacheKey)
		require.Nil(t, cachedResults)
	})

	t.Run("calling_stop_caches_in_foreground", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		defer cache.Stop()

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
			sf:            &singleflight.Group{},
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

		require.Equal(t, tuples, actual)

		iter.Stop()
		iter.wg.Wait()
		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)

		entry := cachedResults.(*storage.TupleIteratorCacheEntry)

		if diff := cmp.Diff(tuples, entry.Tuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("calling_stop_caches_in_background", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		defer cache.Stop()

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
			sf:            &singleflight.Group{},
		}

		iter.Stop()
		iter.wg.Wait()
		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)

		entry := cachedResults.(*storage.TupleIteratorCacheEntry)

		if diff := cmp.Diff(tuples, entry.Tuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("parent_context_cancelled_still_caches_in_background", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		defer cache.Stop()

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
			sf:            &singleflight.Group{},
		}
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := iter.Next(cancelledCtx)

		require.ErrorIs(t, err, context.Canceled)

		iter.Stop()
		iter.wg.Wait()
		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)

		entry := cachedResults.(*storage.TupleIteratorCacheEntry)

		if diff := cmp.Diff(tuples, entry.Tuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("prevent_draining_if_already_cached", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](mockController)
		mockCache.EXPECT().Get(gomock.Any()).Return(tuples)

		sf := &singleflight.Group{}

		var wg sync.WaitGroup

		mockedIter1 := &mockCalledTupleIterator{
			iter: storage.NewStaticTupleIterator(tuples),
		}

		iter1 := &cachedIterator{
			iter:          mockedIter1,
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         mockCache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
			sf:            sf,
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			iter1.Stop()
			iter1.wg.Wait()
		}()

		wg.Wait()

		require.Zero(t, mockedIter1.nextCalled)
	})

	t.Run("prevent_draining_on_the_same_iterator_across_concurrent_requests", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			maxCacheSize := 10
			cacheKey := "cache-key"
			ttl := 5 * time.Hour
			mockController := gomock.NewController(t)
			defer mockController.Finish()

			mockCache := mocks.NewMockInMemoryCache[any](mockController)

			mockCache.EXPECT().Get(cacheKey).AnyTimes().Return(nil)
			mockCache.EXPECT().Set(cacheKey, gomock.Any(), ttl).AnyTimes()
			mockCache.EXPECT().Delete(gomock.Any()).AnyTimes()

			sf := &singleflight.Group{}

			var wg sync.WaitGroup

			mockedIter1 := &mockCalledTupleIterator{
				iter: storage.NewStaticTupleIterator(tuples),
			}

			iter1 := &cachedIterator{
				iter:          mockedIter1,
				tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
				cacheKey:      cacheKey,
				cache:         mockCache,
				maxResultSize: maxCacheSize,
				ttl:           ttl,
				sf:            sf,
			}

			mockedIter2 := &mockCalledTupleIterator{
				iter: storage.NewStaticTupleIterator(tuples),
			}

			iter2 := &cachedIterator{
				iter:          mockedIter2,
				tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
				cacheKey:      cacheKey,
				cache:         mockCache,
				maxResultSize: maxCacheSize,
				ttl:           ttl,
				sf:            sf,
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
