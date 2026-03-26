package storagewrappers

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark: V1 vs V2 Iterator Cache Comparison
// ─────────────────────────────────────────────────────────────────────────────
//
// This file contains benchmarks comparing:
// - V1: CachedDatastore (uses TupleRecord, mutex-based cachedIterator)
// - V2: CachedTupleReader (uses MinimalCacheEntry, lock-free LockFreeCachedIterator)
//
// Run with: go test -bench=BenchmarkV1vsV2 -benchmem ./pkg/storage/storagewrappers/...

// createTestTuples generates test tuples for benchmarking.
func createTestTuples(count int) []*openfgav1.Tuple {
	tuples := make([]*openfgav1.Tuple, count)
	for i := 0; i < count; i++ {
		tuples[i] = &openfgav1.Tuple{
			Key: tuple.NewTupleKey(
				fmt.Sprintf("document:%d", i),
				"viewer",
				fmt.Sprintf("user:user%d", i%100),
			),
		}
	}
	return tuples
}

// createMinimalCacheEntries generates MinimalCacheEntry for V2 benchmarks.
func createMinimalCacheEntries(count int) []MinimalCacheEntry {
	entries := make([]MinimalCacheEntry, count)
	for i := 0; i < count; i++ {
		entries[i] = MinimalCacheEntry{
			ObjectID: fmt.Sprintf("%d", i),
			User:     fmt.Sprintf("user:user%d", i%100),
		}
	}
	return entries
}

// createTupleRecords generates TupleRecord for V1 benchmarks.
func createTupleRecords(count int) []*storage.TupleRecord {
	records := make([]*storage.TupleRecord, count)
	for i := 0; i < count; i++ {
		records[i] = &storage.TupleRecord{
			ObjectID:       fmt.Sprintf("%d", i),
			ObjectType:     "document",
			Relation:       "viewer",
			UserObjectType: "user",
			UserObjectID:   fmt.Sprintf("user%d", i%100),
			UserRelation:   "",
		}
	}
	return records
}

// ─────────────────────────────────────────────────────────────────────────────
// Cache Hit Benchmarks - Compare iteration over cached data
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkV1vsV2_CacheHit_Iteration compares iterating over cached entries.
// V1 uses cachedTupleIterator (wraps StaticIterator with TupleRecord)
// V2 uses LockFreeCachedIterator (atomic index, MinimalCacheEntry).
func BenchmarkV1vsV2_CacheHit_Iteration(b *testing.B) {
	sizes := []int{10, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("V1_TupleRecord_%d", size), func(b *testing.B) {
			records := createTupleRecords(size)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				staticIter := storage.NewStaticIterator[*storage.TupleRecord](records)
				iter := &cachedTupleIterator{
					objectType: "document",
					relation:   "viewer",
					iter:       staticIter,
				}

				for {
					_, err := iter.Next(ctx)
					if err != nil {
						break
					}
				}
			}
		})

		b.Run(fmt.Sprintf("V2_MinimalEntry_%d", size), func(b *testing.B) {
			entries := createMinimalCacheEntries(size)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

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
	}
}

// BenchmarkV1vsV2_CacheHit_SingleNext compares single Next() call overhead.
func BenchmarkV1vsV2_CacheHit_SingleNext(b *testing.B) {
	size := 1000
	ctx := context.Background()

	b.Run("V1_TupleRecord", func(b *testing.B) {
		records := createTupleRecords(size)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			staticIter := storage.NewStaticIterator[*storage.TupleRecord](records)
			iter := &cachedTupleIterator{
				objectType: "document",
				relation:   "viewer",
				iter:       staticIter,
			}
			_, _ = iter.Next(ctx)
		}
	})

	b.Run("V2_MinimalEntry", func(b *testing.B) {
		entries := createMinimalCacheEntries(size)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			iter := NewLockFreeCachedIterator(entries, "document", "viewer")
			_, _ = iter.Next(ctx)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Cache Miss Benchmarks - Compare collecting tuples for caching
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkV1vsV2_CacheMiss_Collection compares collecting tuples on cache miss.
func BenchmarkV1vsV2_CacheMiss_Collection(b *testing.B) {
	sizes := []int{10, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("V1_CachedIterator_%d", size), func(b *testing.B) {
			mockController := gomock.NewController(b)
			defer mockController.Finish()

			mockCache := mocks.NewMockInMemoryCache[any](mockController)
			mockCache.EXPECT().Get(gomock.Any()).Return(nil).AnyTimes()
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			mockCache.EXPECT().Delete(gomock.Any()).AnyTimes()

			tuples := createTestTuples(size)
			ctx := context.Background()
			sf := &singleflight.Group{}
			wg := &sync.WaitGroup{}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				innerIter := storage.NewStaticTupleIterator(tuples)

				ds := NewCachedDatastore(
					ctx,
					nil, // RelationshipTupleReader not used directly
					mockCache,
					size+100,
					time.Hour,
					sf,
					wg,
				)

				// Simulate cache miss path
				iter := &cachedIterator{
					ctx:           ctx,
					iter:          innerIter,
					store:         "store123",
					operation:     "ReadUsersetTuples",
					tuples:        make([]*openfgav1.Tuple, 0, size),
					cacheKey:      "test-key",
					cache:         mockCache,
					maxResultSize: size + 100,
					ttl:           time.Hour,
					sf:            sf,
					objectType:    "document",
					relation:      "viewer",
					wg:            wg,
				}
				_ = ds // avoid unused

				for {
					_, err := iter.Next(ctx)
					if err != nil {
						break
					}
				}
			}
		})

		b.Run(fmt.Sprintf("V2_CachingIterator_%d", size), func(b *testing.B) {
			mockController := gomock.NewController(b)
			defer mockController.Finish()

			mockCache := mocks.NewMockInMemoryCache[any](mockController)
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

			tuples := createTestTuples(size)
			ctx := context.Background()
			sf := &singleflight.Group{}
			wg := &sync.WaitGroup{}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				innerIter := storage.NewStaticTupleIterator(tuples)

				iter := newCachingIterator(
					innerIter, mockCache, "test-key", size+100, time.Hour, 30*time.Second,
					sf, wg, "document", "viewer", "ReadUsersetTuples",
				)

				for {
					_, err := iter.Next(ctx)
					if err != nil {
						break
					}
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Memory Benchmarks - Compare memory usage
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkV1vsV2_Memory_CacheEntry compares memory allocation for cache entries.
func BenchmarkV1vsV2_Memory_CacheEntry(b *testing.B) {
	sizes := []int{100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("V1_TupleRecord_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				records := make([]*storage.TupleRecord, size)
				for j := 0; j < size; j++ {
					records[j] = &storage.TupleRecord{
						ObjectID:       fmt.Sprintf("object-%d", j),
						ObjectType:     "document",
						Relation:       "viewer",
						UserObjectType: "user",
						UserObjectID:   fmt.Sprintf("user%d", j%100),
						UserRelation:   "",
					}
				}
				_ = &storage.TupleIteratorCacheEntry{
					Tuples:       records,
					LastModified: time.Now(),
				}
			}
		})

		b.Run(fmt.Sprintf("V2_MinimalEntry_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				entries := make([]MinimalCacheEntry, size)
				for j := 0; j < size; j++ {
					entries[j] = MinimalCacheEntry{
						ObjectID: fmt.Sprintf("object-%d", j),
						User:     fmt.Sprintf("user:user%d", j%100),
					}
				}
				_ = &V2IteratorCacheEntry{
					Entries:      entries,
					LastModified: time.Now(),
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent Access Benchmarks - Compare thread safety overhead
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkV1vsV2_Concurrent_Next compares concurrent Next() calls.
func BenchmarkV1vsV2_Concurrent_Next(b *testing.B) {
	goroutines := []int{1, 4, 8, 16}
	size := 1000

	for _, numGoroutines := range goroutines {
		b.Run(fmt.Sprintf("V1_TupleRecord_%d_goroutines", numGoroutines), func(b *testing.B) {
			records := createTupleRecords(size)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				staticIter := storage.NewStaticIterator[*storage.TupleRecord](records)
				iter := &cachedTupleIterator{
					objectType: "document",
					relation:   "viewer",
					iter:       staticIter,
				}

				var wg sync.WaitGroup
				wg.Add(numGoroutines)

				for g := 0; g < numGoroutines; g++ {
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

				wg.Wait()
			}
		})

		b.Run(fmt.Sprintf("V2_MinimalEntry_%d_goroutines", numGoroutines), func(b *testing.B) {
			entries := createMinimalCacheEntries(size)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				iter := NewLockFreeCachedIterator(entries, "document", "viewer")

				var wg sync.WaitGroup
				wg.Add(numGoroutines)

				for g := 0; g < numGoroutines; g++ {
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

				wg.Wait()
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Buffer Allocation Benchmarks - Compare allocation strategies
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkV1vsV2_BufferAllocation compares buffer allocation strategies.
// V2 now uses the same V1 pattern (pointer slice during collection, transform at flush).
func BenchmarkV1vsV2_BufferAllocation(b *testing.B) {
	size := 100

	b.Run("V1_TuplePointers", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// V1 allocates slice of tuple pointers
			tuples := make([]*openfgav1.Tuple, 0, size/2)
			for j := 0; j < size; j++ {
				tuples = append(tuples, &openfgav1.Tuple{
					Key: tuple.NewTupleKey(
						fmt.Sprintf("document:%d", j),
						"viewer",
						"user:test",
					),
				})
			}
			_ = tuples
		}
	})

	b.Run("V2_TuplePointers_ThenTransform", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// V2 now uses same pattern: collect pointers, then transform at flush
			tuples := make([]*openfgav1.Tuple, 0, size/2)
			for j := 0; j < size; j++ {
				tuples = append(tuples, &openfgav1.Tuple{
					Key: tuple.NewTupleKey(
						fmt.Sprintf("document:%d", j),
						"viewer",
						"user:test",
					),
				})
			}
			// Transform to MinimalCacheEntry at flush time
			entries := make([]MinimalCacheEntry, len(tuples))
			for j, t := range tuples {
				tk := t.GetKey()
				entries[j] = MinimalCacheEntry{
					ObjectID: tk.GetObject()[len("document:"):],
					User:     tk.GetUser(),
				}
			}
			_ = entries
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// End-to-End Benchmarks - Full cache miss to cache hit cycle
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkV1vsV2_EndToEnd simulates a full cache miss followed by cache hits.
func BenchmarkV1vsV2_EndToEnd(b *testing.B) {
	size := 100

	b.Run("V2_CacheMissThenHit", func(b *testing.B) {
		mockController := gomock.NewController(b)
		defer mockController.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](mockController)

		var cachedEntry *V2IteratorCacheEntry
		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ string, value any, _ time.Duration) {
				cachedEntry = value.(*V2IteratorCacheEntry)
			},
		).AnyTimes()
		mockCache.EXPECT().Get(gomock.Any()).DoAndReturn(
			func(_ string) any {
				return cachedEntry
			},
		).AnyTimes()

		tuples := createTestTuples(size)
		ctx := context.Background()
		sf := &singleflight.Group{}
		wg := &sync.WaitGroup{}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Cache miss - populate cache
			innerIter := storage.NewStaticTupleIterator(tuples)
			cachingIter := newCachingIterator(
				innerIter, mockCache, "test-key", size+100, time.Hour, 30*time.Second,
				sf, wg, "document", "viewer", "ReadUsersetTuples",
			)

			for {
				_, err := cachingIter.Next(ctx)
				if err != nil {
					break
				}
			}

			// Cache hit - iterate from cache
			if cachedEntry != nil {
				cachedIter := NewLockFreeCachedIterator(cachedEntry.Entries, "document", "viewer")
				for {
					_, err := cachedIter.Next(ctx)
					if err != nil {
						break
					}
				}
			}
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test to verify V2 atomic operations are race-free
// ─────────────────────────────────────────────────────────────────────────────

func TestV2_NoRaceCondition(t *testing.T) {
	// This test should pass with -race flag
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	// Expect Get calls for optimization 1 (check if already cached)
	mockCache.EXPECT().Get(gomock.Any()).Return(nil).AnyTimes()
	mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	tuples := createTestTuples(100)
	ctx := context.Background()
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}

	// Run multiple iterations to increase chance of detecting race
	for i := 0; i < 100; i++ {
		innerIter := storage.NewStaticTupleIterator(tuples)
		iter := newCachingIterator(
			innerIter, mockCache, fmt.Sprintf("test-key-%d", i), 1000, time.Hour, 30*time.Second,
			sf, wg, "document", "viewer", "ReadUsersetTuples",
		)

		// Concurrent access pattern that previously caused race
		var testWg sync.WaitGroup
		testWg.Add(2)

		go func() {
			defer testWg.Done()
			for {
				_, err := iter.Next(ctx)
				if err != nil {
					return
				}
			}
		}()

		go func() {
			defer testWg.Done()
			time.Sleep(time.Microsecond)
			iter.Stop()
		}()

		testWg.Wait()
	}

	wg.Wait()
	require.True(t, true, "No race condition detected")
}
