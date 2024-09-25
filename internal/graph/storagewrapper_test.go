package graph

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

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
			mockCache.EXPECT().Set(gomock.Any(), tuples, ttl),
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
			mockCache.EXPECT().Get(gomock.Any()).Return(&storage.CachedResult[any]{Value: tuples}),
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
	tuples := []*openfgav1.Tuple{}
	for _, tk := range tks {
		tuples = append(tuples, &openfgav1.Tuple{Key: tk})
	}

	tk := tuple.NewTupleKey("license:1", "owner", "")

	mockDatastore.EXPECT().
		Write(gomock.Any(), storeID, nil, tks).Return(nil)

	err := ds.Write(ctx, storeID, nil, tks)
	require.NoError(t, err)

	t.Run("cache miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()),
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Set(gomock.Any(), tuples, ttl),
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

	t.Run("cache hit", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()).
				Return(&storage.CachedResult[any]{Value: tuples}),
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

	t.Run("higher consistency", func(t *testing.T) {
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
}

func TestCachedIterator(t *testing.T) {
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

	t.Run("not_calling_stop_doesnt_cache", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := "cache-key"
		ttl := 5 * time.Hour
		cache := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
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

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
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

		iter.Stop()

		require.Equal(t, tuples, actual)

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

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
		}

		iter.Stop()

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

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
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
		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)

		if diff := cmp.Diff(tuples, cachedResults.Value.([]*openfgav1.Tuple), cmpOpts...); diff != "" {
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

		iter := &cachedIterator{
			iter:          storage.NewStaticTupleIterator(tuples),
			tuples:        make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:      cacheKey,
			cache:         cache,
			maxResultSize: maxCacheSize,
			ttl:           ttl,
		}

		iter.Stop()
		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)

		if diff := cmp.Diff(tuples, cachedResults.Value.([]*openfgav1.Tuple), cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})
}
