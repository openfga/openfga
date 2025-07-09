package sharediterator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/sourcegraph/conc/pool"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func length(m *sync.Map) int {
	var i int
	m.Range(func(_, _ any) bool {
		i++
		return true
	})
	return i
}

type testIteratorInfo struct {
	iter storage.TupleIterator
	err  error
}

func BenchmarkSharedIteratorWithStaticIterator(b *testing.B) {
	ctx := context.Background()

	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:5"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Create a static iterator as the internal iterator
	staticIter := storage.NewStaticTupleIterator(tuples)

	// Create shared iterator with cleanup function
	sharedIter := newSharedIterator(staticIter, func() {
		// Cleanup function - no-op for benchmark
	})
	defer sharedIter.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Clone the shared iterator for each benchmark iteration
		clonedIter := sharedIter.clone()
		if clonedIter == nil {
			b.Fatal("Failed to clone shared iterator")
		}

		// Read all tuples from the cloned iterator
		for {
			_, err := clonedIter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				b.Fatalf("Unexpected error: %v", err)
			}
		}

		clonedIter.Stop()
	}
}

func BenchmarkSharedIteratorConcurrentAccess(b *testing.B) {
	ctx := context.Background()

	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:5"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Create a static iterator as the internal iterator
	staticIter := storage.NewStaticTupleIterator(tuples)

	// Create shared iterator with cleanup function
	sharedIter := newSharedIterator(staticIter, func() {
		// Cleanup function - no-op for benchmark
	})
	defer sharedIter.Stop()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Clone the shared iterator for each goroutine
			clonedIter := sharedIter.clone()
			if clonedIter == nil {
				b.Fatal("Failed to clone shared iterator")
			}

			// Read all tuples from the cloned iterator
			for {
				_, err := clonedIter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					b.Fatalf("Unexpected error: %v", err)
				}
			}

			clonedIter.Stop()
		}
	})
}

func BenchmarkSharedIteratorVsDirectAccess(b *testing.B) {
	ctx := context.Background()

	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:5"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	b.Run("SharedIterator", func(b *testing.B) {
		staticIter := storage.NewStaticTupleIterator(tuples)
		sharedIter := newSharedIterator(staticIter, func() {})
		defer sharedIter.Stop()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			clonedIter := sharedIter.clone()
			if clonedIter == nil {
				b.Fatal("Failed to clone shared iterator")
			}

			for {
				_, err := clonedIter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					b.Fatalf("Unexpected error: %v", err)
				}
			}

			clonedIter.Stop()
		}
	})

	b.Run("DirectStaticIterator", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			staticIter := storage.NewStaticTupleIterator(tuples)

			for {
				_, err := staticIter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					b.Fatalf("Unexpected error: %v", err)
				}
			}

			staticIter.Stop()
		}
	})
}

func BenchmarkSharedIteratorClone(b *testing.B) {
	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:5"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Create a static iterator as the internal iterator
	staticIter := storage.NewStaticTupleIterator(tuples)

	// Create shared iterator with cleanup function
	sharedIter := newSharedIterator(staticIter, func() {
		// Cleanup function - no-op for benchmark
	})
	defer sharedIter.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Clone the shared iterator
		clonedIter := sharedIter.clone()
		if clonedIter == nil {
			b.Fatal("Failed to clone shared iterator")
		}
		clonedIter.Stop()
	}
}

func BenchmarkSharedIteratorConcurrentCloning(b *testing.B) {
	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:5"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Create a static iterator as the internal iterator
	staticIter := storage.NewStaticTupleIterator(tuples)

	// Create shared iterator with cleanup function
	sharedIter := newSharedIterator(staticIter, func() {
		// Cleanup function - no-op for benchmark
	})
	defer sharedIter.Stop()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Clone the shared iterator concurrently
			clonedIter := sharedIter.clone()
			if clonedIter == nil {
				b.Fatal("Failed to clone shared iterator")
			}
			clonedIter.Stop()
		}
	})
}

func BenchmarkSharedIteratorCloneAndRead(b *testing.B) {
	ctx := context.Background()

	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:5"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Create a static iterator as the internal iterator
	staticIter := storage.NewStaticTupleIterator(tuples)

	// Create shared iterator with cleanup function
	sharedIter := newSharedIterator(staticIter, func() {
		// Cleanup function - no-op for benchmark
	})
	defer sharedIter.Stop()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Clone the shared iterator concurrently
			clonedIter := sharedIter.clone()
			if clonedIter == nil {
				b.Fatal("Failed to clone shared iterator")
			}

			// Read one tuple to simulate realistic usage
			_, err := clonedIter.Next(ctx)
			if err != nil && !errors.Is(err, storage.ErrIteratorDone) {
				b.Fatalf("Unexpected error: %v", err)
			}

			clonedIter.Stop()
		}
	})
}

func BenchmarkSharedIteratorConcurrentCloneStress(b *testing.B) {
	ctx := context.Background()

	// Create larger test data for stress testing
	var tks []*openfgav1.TupleKey
	for i := 0; i < 100; i++ {
		tks = append(tks, tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", fmt.Sprintf("user:%d", i)))
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Create a static iterator as the internal iterator
	staticIter := storage.NewStaticTupleIterator(tuples)

	// Create shared iterator with cleanup function
	sharedIter := newSharedIterator(staticIter, func() {
		// Cleanup function - no-op for benchmark
	})
	defer sharedIter.Stop()

	// Use a higher number of goroutines for stress testing
	const numGoroutines = 100

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		for j := 0; j < numGoroutines; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Clone the shared iterator
				clonedIter := sharedIter.clone()
				if clonedIter == nil {
					b.Error("Failed to clone shared iterator")
					return
				}

				// Simulate some work by reading a few tuples
				for k := 0; k < 3; k++ {
					_, err := clonedIter.Next(ctx)
					if err != nil {
						if errors.Is(err, storage.ErrIteratorDone) {
							break
						}
						b.Errorf("Unexpected error: %v", err)
						break
					}
				}

				clonedIter.Stop()
			}()
		}

		wg.Wait()
	}
}

func BenchmarkIteratorDatastoreReadConcurrentStress(b *testing.B) {
	ctx := context.Background()

	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:5"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Setup mock datastore
	mockController := gomock.NewController(b)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	tk := tuple.NewTupleKey("document:1", "viewer", "")

	// Setup shared iterator datastore
	internalStorage := NewSharedIteratorDatastoreStorage()
	ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
		WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

	// Mock expects single call that returns static iterator
	mockDatastore.EXPECT().
		Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
		DoAndReturn(func(_ context.Context, _ string, _ *openfgav1.TupleKey, _ storage.ReadOptions) (storage.TupleIterator, error) {
			return storage.NewStaticTupleIterator(tuples), nil
		}).AnyTimes()

	const numGoroutines = 100

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		for j := 0; j < numGoroutines; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Create iterator
				iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
				if err != nil {
					b.Errorf("Failed to create iterator: %v", err)
					return
				}
				defer iter.Stop()

				// Read all tuples
				var count int
				for {
					_, err := iter.Next(ctx)
					if err != nil {
						if errors.Is(err, storage.ErrIteratorDone) {
							break
						}
						b.Errorf("Unexpected error: %v", err)
						return
					}
					count++
				}

				if count != len(tuples) {
					b.Errorf("Expected %d tuples, got %d", len(tuples), count)
				}
			}()
		}

		wg.Wait()
	}
}

func BenchmarkIteratorDatastoreReadConcurrentMixedOperations(b *testing.B) {
	ctx := context.Background()

	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:5"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Setup mock datastore
	mockController := gomock.NewController(b)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	tk := tuple.NewTupleKey("document:1", "viewer", "")

	// Setup shared iterator datastore
	internalStorage := NewSharedIteratorDatastoreStorage()
	ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
		WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

	// Mock expects calls
	mockDatastore.EXPECT().
		Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
		DoAndReturn(func(_ context.Context, _ string, _ *openfgav1.TupleKey, _ storage.ReadOptions) (storage.TupleIterator, error) {
			return storage.NewStaticTupleIterator(tuples), nil
		}).AnyTimes()

	const numGoroutines = 50

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		// Mix of different operation patterns
		for j := 0; j < numGoroutines; j++ {
			wg.Add(1)

			switch j % 4 {
			case 0:
				// Full read
				go func() {
					defer wg.Done()
					iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
					if err != nil {
						b.Errorf("Failed to create iterator: %v", err)
						return
					}
					defer iter.Stop()

					for {
						_, err := iter.Next(ctx)
						if err != nil {
							if errors.Is(err, storage.ErrIteratorDone) {
								break
							}
							b.Errorf("Unexpected error: %v", err)
							return
						}
					}
				}()

			case 1:
				// Head then partial read
				go func() {
					defer wg.Done()
					iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
					if err != nil {
						b.Errorf("Failed to create iterator: %v", err)
						return
					}
					defer iter.Stop()

					// Check head
					_, err = iter.Head(ctx)
					if err != nil && !errors.Is(err, storage.ErrIteratorDone) {
						b.Errorf("Unexpected error on Head: %v", err)
						return
					}

					// Read a few items
					for k := 0; k < 2; k++ {
						_, err := iter.Next(ctx)
						if err != nil {
							if errors.Is(err, storage.ErrIteratorDone) {
								break
							}
							b.Errorf("Unexpected error: %v", err)
							return
						}
					}
				}()

			case 2:
				// Early stop
				go func() {
					defer wg.Done()
					iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
					if err != nil {
						b.Errorf("Failed to create iterator: %v", err)
						return
					}

					// Read one item then stop
					_, err = iter.Next(ctx)
					if err != nil && !errors.Is(err, storage.ErrIteratorDone) {
						b.Errorf("Unexpected error: %v", err)
					}

					iter.Stop()
				}()

			case 3:
				// Multiple head calls
				go func() {
					defer wg.Done()
					iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
					if err != nil {
						b.Errorf("Failed to create iterator: %v", err)
						return
					}
					defer iter.Stop()

					// Multiple head calls
					for k := 0; k < 3; k++ {
						_, err = iter.Head(ctx)
						if err != nil && !errors.Is(err, storage.ErrIteratorDone) {
							b.Errorf("Unexpected error on Head: %v", err)
							break
						}
					}
				}()
			}
		}

		wg.Wait()
	}
}

func BenchmarkIteratorDatastoreReadHighContentionStress(b *testing.B) {
	ctx := context.Background()

	// Create larger test data for stress testing
	var tks []*openfgav1.TupleKey
	for i := 0; i < 1000; i++ {
		tks = append(tks, tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", fmt.Sprintf("user:%d", i)))
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Setup mock datastore
	mockController := gomock.NewController(b)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	tk := tuple.NewTupleKey("document:1", "viewer", "")

	// Setup shared iterator datastore
	internalStorage := NewSharedIteratorDatastoreStorage()
	ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
		WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

	// Mock expects single call
	mockDatastore.EXPECT().
		Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
		DoAndReturn(func(_ context.Context, _ string, _ *openfgav1.TupleKey, _ storage.ReadOptions) (storage.TupleIterator, error) {
			return storage.NewStaticTupleIterator(tuples), nil
		}).AnyTimes()

	const numGoroutines = 1000

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var wg sync.WaitGroup

			for j := 0; j < numGoroutines; j++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
					if err != nil {
						b.Errorf("Goroutine %d: Failed to create iterator: %v", id, err)
						return
					}
					defer iter.Stop()

					// Simulate different reading patterns
					switch id % 3 {
					case 0:
						// Full read
						var count int
						for {
							_, err := iter.Next(ctx)
							if err != nil {
								if errors.Is(err, storage.ErrIteratorDone) {
									break
								}
								b.Errorf("Goroutine %d: Unexpected error: %v", id, err)
								return
							}
							count++
						}
					case 1:
						// Partial read with random stop
						readCount := id%10 + 1
						for k := 0; k < readCount; k++ {
							_, err := iter.Next(ctx)
							if err != nil {
								if errors.Is(err, storage.ErrIteratorDone) {
									break
								}
								b.Errorf("Goroutine %d: Unexpected error: %v", id, err)
								return
							}
						}
					default:
						// Head operations
						for k := 0; k < 5; k++ {
							_, err := iter.Head(ctx)
							if err != nil && !errors.Is(err, storage.ErrIteratorDone) {
								b.Errorf("Goroutine %d: Unexpected error on Head: %v", id, err)
								return
							}
						}
					}
				}(j)
			}

			wg.Wait()
		}
	})
}

func BenchmarkIteratorDatastoreReadRapidCreateDestroy(b *testing.B) {
	ctx := context.Background()

	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Setup mock datastore
	mockController := gomock.NewController(b)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	tk := tuple.NewTupleKey("document:1", "viewer", "")

	// Setup shared iterator datastore
	internalStorage := NewSharedIteratorDatastoreStorage()
	ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
		WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

	// Mock expects calls
	mockDatastore.EXPECT().
		Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
		DoAndReturn(func(_ context.Context, _ string, _ *openfgav1.TupleKey, _ storage.ReadOptions) (storage.TupleIterator, error) {
			return storage.NewStaticTupleIterator(tuples), nil
		}).AnyTimes()

	const numGoroutines = 100

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		// Rapidly create and destroy iterators
		for j := 0; j < numGoroutines; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Create iterator
				iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
				if err != nil {
					b.Errorf("Failed to create iterator: %v", err)
					return
				}

				// Immediately stop (stress test reference counting)
				iter.Stop()
			}()
		}

		wg.Wait()

		// Verify internal storage is cleaned up
		if length(&internalStorage.read) != 0 {
			b.Errorf("Expected internal storage to be empty, but found %d items", length(&internalStorage.read))
		}
	}
}

func BenchmarkIteratorDatastoreReadWithContextCancellation(b *testing.B) {
	// Create test data
	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	// Setup mock datastore
	mockController := gomock.NewController(b)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	tk := tuple.NewTupleKey("document:1", "viewer", "")

	// Setup shared iterator datastore
	internalStorage := NewSharedIteratorDatastoreStorage()
	ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
		WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

	// Mock expects calls
	mockDatastore.EXPECT().
		Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
		DoAndReturn(func(_ context.Context, _ string, _ *openfgav1.TupleKey, _ storage.ReadOptions) (storage.TupleIterator, error) {
			return storage.NewStaticTupleIterator(tuples), nil
		}).AnyTimes()

	const numGoroutines = 50

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		for j := 0; j < numGoroutines; j++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Create context with random timeout
				timeout := time.Duration(id%10+1) * time.Millisecond
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()

				iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
				if err != nil {
					// Context might be cancelled during Read
					return
				}
				defer iter.Stop()

				// Try to read with cancelled context
				for {
					_, err := iter.Next(ctx)
					if err != nil {
						// Expected to get context cancelled or iterator done
						if errors.Is(err, context.Canceled) ||
							errors.Is(err, context.DeadlineExceeded) ||
							errors.Is(err, storage.ErrIteratorDone) {
							break
						}
						b.Errorf("Unexpected error: %v", err)
						return
					}
				}
			}(j)
		}

		wg.Wait()
	}
}

// helper function to validate the single client case.
func helperValidateSingleClient(ctx context.Context, t *testing.T, internalStorage *sync.Map, iter storage.TupleIterator, expected []*openfgav1.Tuple) {
	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	require.NotEmpty(t, length(internalStorage))

	var actual []*openfgav1.Tuple

	headTup, err := iter.Head(ctx)
	require.NoError(t, err)
	require.NotNil(t, headTup)

	for {
		tup, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			require.Fail(t, "no error was expected")
			break
		}

		actual = append(actual, tup)
	}

	require.NotEmpty(t, length(internalStorage))

	iter.Stop() // has to be sync otherwise the assertion fails

	if diff := cmp.Diff(expected, actual, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-want +got):\n%s", diff)
	}
	// make sure the internal map is deallocated

	require.Empty(t, length(internalStorage))
}

func helperValidateMultipleClients(ctx context.Context, t *testing.T, internalStorage *sync.Map, iterInfos []testIteratorInfo, expected []*openfgav1.Tuple) {
	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	require.NotEmpty(t, length(internalStorage))
	for i := 0; i < len(iterInfos); i++ {
		require.NoError(t, iterInfos[i].err)
		require.NotNil(t, iterInfos[i].iter)
	}
	p := pool.New().WithErrors()

	for _, iterInfo := range iterInfos {
		var actual []*openfgav1.Tuple
		iter := iterInfo.iter

		p.Go(func() error {
			for {
				tup, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					return fmt.Errorf("no error was expected %v", err)
				}

				actual = append(actual, tup)
			}
			if diff := cmp.Diff(expected, actual, cmpOpts...); diff != "" {
				return fmt.Errorf("mismatch (-want +got):\n%s", diff)
			}
			return nil
		})
	}

	err := p.Wait()
	require.NoError(t, err)

	// make sure the internal map has not deallocated

	require.NotEmpty(t, length(internalStorage))

	for i, iterInfo := range iterInfos {
		iterInfo.iter.Stop()

		if i < len(iterInfos)-1 {
			require.NotEmpty(t, length(internalStorage))
		} else {
			require.Empty(t, length(internalStorage))
		}
	}
}

func TestSharedIteratorDatastore_Read(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("license:1", "owner", "company:1"),
		tuple.NewTupleKey("license:1", "owner", "company:2"),
		tuple.NewTupleKey("license:1", "owner", "company:3"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}
	tk := tuple.NewTupleKey("license:1", "owner", "")

	t.Run("single_client", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		helperValidateSingleClient(ctx, t, &internalStorage.read, iter, tuples)
	})

	t.Run("multiple_concurrent_clients", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

		const numClient = 3
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(storage.NewStaticTupleIterator(tuples), nil)

		iterInfos := make([]testIteratorInfo, numClient)
		wg := sync.WaitGroup{}

		for i := 0; i < numClient; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				curIter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
				iterInfos[i] = testIteratorInfo{curIter, err}
			}(i)
		}
		wg.Wait()
		helperValidateMultipleClients(ctx, t, &internalStorage.read, iterInfos, tuples)
	})
	t.Run("error_when_querying", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

		tk1 := tuple.NewTupleKey("license:1a", "owner", "")

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk1, storage.ReadOptions{}).
			Return(nil, fmt.Errorf("mock_error")).MaxTimes(2)
		_, err := ds.Read(ctx, storeID, tk1, storage.ReadOptions{})
		require.Error(t, err)

		// subsequent request will return the same result
		_, err = ds.Read(ctx, storeID, tk1, storage.ReadOptions{})
		require.Error(t, err)

		require.Empty(t, length(&internalStorage.read))
	})
	t.Run("bypass_due_to_map_size_limit", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()

		internalStorage := NewSharedIteratorDatastoreStorage(WithSharedIteratorDatastoreStorageLimit(0))
		dsLimit := NewSharedIteratorDatastore(mockDatastore, internalStorage)

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := dsLimit.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		// this should not come from the map
		require.Empty(t, length(&internalStorage.read))

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("bypass_due_to_strong_consistency", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk,
				storage.ReadOptions{
					Consistency: storage.ConsistencyOptions{
						Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY}}).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY}})
		require.NoError(t, err)
		// this should not come from the map
		require.Empty(t, length(&ds.internalStorage.read))

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("multiple_concurrent_clients_read_and_done", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

		cmpOpts := []cmp.Option{
			testutils.TupleKeyCmpTransformer,
			protocmp.Transform(),
		}

		const numClient = 10
		mockDatastore.EXPECT().Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).DoAndReturn(
			func(ctx context.Context,
				store string,
				tupleKey *openfgav1.TupleKey,
				options storage.ReadOptions) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator(tuples), nil
			}).MaxTimes(numClient)
		p := pool.New().WithErrors()

		for i := 0; i < numClient; i++ {
			p.Go(func() error {
				iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
				if err != nil {
					return err
				}
				defer iter.Stop()

				var actual []*openfgav1.Tuple

				for {
					tup, err := iter.Next(ctx)
					if err != nil {
						if errors.Is(err, storage.ErrIteratorDone) {
							break
						}
						return fmt.Errorf("no error was expected %v", err)
					}

					actual = append(actual, tup)
				}
				if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
					return fmt.Errorf("mismatch (-want +got):\n%s", diff)
				}
				return nil
			})
		}

		err := p.Wait()
		require.NoError(t, err)
		require.Empty(t, length(&ds.internalStorage.read))
	})
}

func TestSharedIteratorDatastore_ReadUsersetTuples(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:1", "viewer", "user:2"),
		tuple.NewTupleKey("document:1", "viewer", "user:3"),
		tuple.NewTupleKey("document:1", "viewer", "user:4"),
		tuple.NewTupleKey("document:1", "viewer", "user:*"),
		tuple.NewTupleKey("document:1", "viewer", "company:1#viewer"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
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

	t.Run("single_client", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)
		helperValidateSingleClient(ctx, t, &internalStorage.rut, iter, tuples)
	})

	t.Run("multiple_concurrent_clients", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		const numClient = 3
		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)

		iterInfos := make([]testIteratorInfo, numClient)
		wg := sync.WaitGroup{}

		for i := 0; i < numClient; i++ {
			wg.Add(1)
			go func(i int) {
				curIter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
				iterInfos[i] = testIteratorInfo{curIter, err}
				wg.Done()
			}(i)
		}
		wg.Wait()
		helperValidateMultipleClients(ctx, t, &internalStorage.rut, iterInfos, tuples)
	})

	t.Run("error_when_querying", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		filter1 := storage.ReadUsersetTuplesFilter{
			Object:   "document:1a",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				typesystem.DirectRelationReference("company", "viewer"),
				typesystem.WildcardRelationReference("user"),
			},
		}
		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), storeID, filter1, options).
			Return(nil, fmt.Errorf("mock_error")).MaxTimes(2)
		_, err := ds.ReadUsersetTuples(ctx, storeID, filter1, options)
		require.Error(t, err)

		// subsequent call should also return error
		_, err = ds.ReadUsersetTuples(ctx, storeID, filter1, options)
		require.Error(t, err)

		require.Empty(t, length(&internalStorage.rut))
	})
	t.Run("bypass_due_to_map_size_limit", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorageLimit := NewSharedIteratorDatastoreStorage(WithSharedIteratorDatastoreStorageLimit(0))
		dsLimit := NewSharedIteratorDatastore(mockDatastore, internalStorageLimit)

		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := dsLimit.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)
		// this should not come from the map
		require.Empty(t, length(&internalStorageLimit.rut))

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("bypass_due_to_strong_consistency", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), storeID, filter,
				storage.ReadUsersetTuplesOptions{
					Consistency: storage.ConsistencyOptions{
						Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY}}).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY}})
		require.NoError(t, err)
		// this should not come from the map
		require.Empty(t, length(&ds.internalStorage.rut))

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("multiple_concurrent_clients_read_and_done", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		cmpOpts := []cmp.Option{
			testutils.TupleKeyCmpTransformer,
			protocmp.Transform(),
		}

		const numClient = 10
		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, filter, options).DoAndReturn(
			func(ctx context.Context,
				store string,
				filter storage.ReadUsersetTuplesFilter,
				options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator(tuples), nil
			}).MaxTimes(numClient)
		p := pool.New().WithErrors()

		for i := 0; i < numClient; i++ {
			p.Go(func() error {
				iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
				if err != nil {
					return err
				}
				defer iter.Stop()

				var actual []*openfgav1.Tuple

				for {
					tup, err := iter.Next(ctx)
					if err != nil {
						if errors.Is(err, storage.ErrIteratorDone) {
							break
						}
						return fmt.Errorf("no error was expected %v", err)
					}

					actual = append(actual, tup)
				}
				if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
					return fmt.Errorf("mismatch (-want +got):\n%s", diff)
				}
				return nil
			})
		}

		err := p.Wait()
		require.NoError(t, err)
		require.Empty(t, length(&ds.internalStorage.rut))
	})
}

func TestSharedIteratorDatastore_ReadStartingWithUser(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:*"),
	}

	var tuples []*openfgav1.Tuple
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
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

	t.Run("single_client", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)
		helperValidateSingleClient(ctx, t, &internalStorage.rswu, iter, tuples)
	})

	t.Run("multiple_concurrent_clients", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		const numClient = 3
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)

		iterInfos := make([]testIteratorInfo, numClient)
		wg := sync.WaitGroup{}

		for i := 0; i < numClient; i++ {
			wg.Add(1)
			go func(i int) {
				curIter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
				iterInfos[i] = testIteratorInfo{curIter, err}
				wg.Done()
			}(i)
		}
		wg.Wait()
		helperValidateMultipleClients(ctx, t, &internalStorage.rswu, iterInfos, tuples)
	})
	t.Run("error_when_querying", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		filter1 := storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:5"},
				{Object: "user:*"},
			},
			ObjectIDs: storage.NewSortedSet("1a"),
		}

		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter1, options).
			Return(nil, fmt.Errorf("mock_error")).MaxTimes(2)
		_, err := ds.ReadStartingWithUser(ctx, storeID, filter1, options)
		require.Error(t, err)

		// other request should also return error
		_, err = ds.ReadStartingWithUser(ctx, storeID, filter1, options)
		require.Error(t, err)

		require.Empty(t, length(&internalStorage.rswu))
	})
	t.Run("bypass_due_to_map_size_limit", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorageLimit := NewSharedIteratorDatastoreStorage(WithSharedIteratorDatastoreStorageLimit(0))
		dsLimit := NewSharedIteratorDatastore(mockDatastore, internalStorageLimit)

		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := dsLimit.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)
		// this should not come from the map
		require.Empty(t, length(&internalStorageLimit.rswu))

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("bypass_due_to_strong_consistency", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter,
				storage.ReadStartingWithUserOptions{
					Consistency: storage.ConsistencyOptions{
						Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY}}).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, storage.ReadStartingWithUserOptions{Consistency: storage.ConsistencyOptions{
			Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY}})
		require.NoError(t, err)
		// this should not come from the map
		require.Empty(t, length(&ds.internalStorage.rswu))

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("stop_more_than_once", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter1, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)

		require.NotEmpty(t, length(&internalStorage.rswu))

		iter2, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)

		require.NotEmpty(t, length(&internalStorage.rswu))

		iter1.Stop()

		require.NotEmpty(t, length(&internalStorage.rswu))

		// we call stop more than once
		iter1.Stop()

		require.NotEmpty(t, length(&internalStorage.rswu))

		iter2.Stop()

		require.Empty(t, length(&internalStorage.rswu))
	})
	t.Run("multiple_concurrent_clients_read_and_done", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		cmpOpts := []cmp.Option{
			testutils.TupleKeyCmpTransformer,
			protocmp.Transform(),
		}

		const numClient = 10
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, filter, options).DoAndReturn(
			func(ctx context.Context,
				store string,
				filter storage.ReadStartingWithUserFilter,
				options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator(tuples), nil
			}).MaxTimes(numClient)
		p := pool.New().WithErrors()

		for i := 0; i < numClient; i++ {
			p.Go(func() error {
				iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
				if err != nil {
					return err
				}
				defer iter.Stop()

				var actual []*openfgav1.Tuple

				for {
					tup, err := iter.Next(ctx)
					if err != nil {
						if errors.Is(err, storage.ErrIteratorDone) {
							break
						}
						return fmt.Errorf("no error was expected %v", err)
					}

					actual = append(actual, tup)
				}
				if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
					return fmt.Errorf("mismatch (-want +got):\n%s", diff)
				}
				return nil
			})
		}

		err := p.Wait()
		require.NoError(t, err)
		require.Empty(t, length(&internalStorage.rswu))
	})
}

// These tests will focus on the iteration itself.
func TestNewSharedIteratorDatastore_iter(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tk := tuple.NewTupleKey("license:1", "owner", "")

	t.Run("stopped_iterator", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		mockIterator.EXPECT().Stop()

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)

		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		iter.Stop()
		// Head() / Next() should do absolutely nothing except returning Done
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("panic_in_inner_iterator_next", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)

		// Mock the iterator to panic on Next call
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Do(func(context.Context) {
				panic("simulated panic in iterator")
			}),
			mockIterator.EXPECT().Stop(),
		)

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)

		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		// First call should trigger the panic and recover
		_, err = iter.Next(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic")
		require.Contains(t, err.Error(), "simulated panic in iterator")

		// Subsequent calls should return the same error
		_, err = iter.Next(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic")

		// Head should also return the same error
		_, err = iter.Head(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic")

		// Test with a second iterator sharing the same underlying iterator
		iter2, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter2.Stop()

		// Second iterator should also get the panic error
		_, err = iter2.Next(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic")
	})

	t.Run("panic_with_error_type", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)

		// Mock the iterator to panic with an error type
		panicErr := errors.New("custom panic error")
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Do(func(context.Context) {
				panic(panicErr)
			}),
			mockIterator.EXPECT().Stop(),
		)

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)

		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		// First call should trigger the panic and recover
		_, err = iter.Next(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic")
		require.Contains(t, err.Error(), "custom panic error")

		// Verify it's wrapped properly
		require.ErrorIs(t, err, panicErr)
	})

	t.Run("panic_after_successful_items", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)

		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}
		tupleTwo := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:2"), Timestamp: ts}

		// Mock successful items followed by panic
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleTwo, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Do(func(context.Context) {
				panic("panic after successful items")
			}),
			mockIterator.EXPECT().Stop(),
		)

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)

		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		// First call should trigger panic and recover
		_, err = iter.Next(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic")
		require.Contains(t, err.Error(), "panic after successful items")

		// Subsequent calls should return the same error
		_, err = iter.Next(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic")

		// Test with a second iterator - it should see the panic error
		iter2, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter2.Stop()

		// The panic error
		_, err = iter2.Next(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic")
	})

	t.Run("head_empty_list_head", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, storage.ErrIteratorDone),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		// subsequent Head or Next should not involve actual request but return Done
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("head_empty_list_next", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, storage.ErrIteratorDone),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		// subsequent Head or Next should not involve actual request but return Done
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("single_item_head_first", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}

		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, storage.ErrIteratorDone),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()
		item, err := iter.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item)
		item, err = iter.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item)
		item, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item)
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("single_item_next_first", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}

		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, storage.ErrIteratorDone),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()
		item, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item)
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})
	t.Run("single_item_next_first_then_head_first", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}

		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, storage.ErrIteratorDone),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()
		item, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item)
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})
	t.Run("multiple_items_next_without_head", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}
		tupleTwo := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}
		tupleThree := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}

		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleTwo, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleThree, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, storage.ErrIteratorDone),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()
		item, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item)
		item, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleTwo, item)
		item, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleThree, item)
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})
	t.Run("multiple_clients_stop_at_different_time", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}
		tupleTwo := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}
		tupleThree := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}

		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleTwo, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleThree, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, storage.ErrIteratorDone),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)

		iter1, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		item1a, err := iter1.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1a)
		iter2, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		item1b, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1b)
		// at this time, iter1 is stopped, but we should still be able to get item for iter2
		iter1.Stop()
		_, err = iter1.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		item2a, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleTwo, item2a)
		item3a, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleThree, item3a)
		iter3, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		item1c, err := iter3.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1c)
		iter2.Stop()
		iter3.Stop()
	})

	t.Run("error_in_first_item", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		mockedError := errors.New("mocked error")
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, mockedError),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter1, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter1.Stop()
		_, err = iter1.Head(ctx)
		require.ErrorIs(t, err, mockedError)
		_, err = iter1.Head(ctx)
		require.ErrorIs(t, err, mockedError)
		_, err = iter1.Next(ctx)
		require.ErrorIs(t, err, mockedError)
		iter2, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter2.Stop()
		_, err = iter2.Head(ctx)
		require.ErrorIs(t, err, mockedError)
	})

	t.Run("error_in_second_item", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}
		mockedError := errors.New("mocked error")
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, mockedError),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter1, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		// iter1 will be stopped later (but before iter3 is created)
		item1a, err := iter1.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1a)
		item1aNext, err := iter1.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1aNext)
		_, err = iter1.Next(ctx)
		require.ErrorIs(t, err, mockedError)

		// client 2
		iter2, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter2.Stop()
		item1b, err := iter2.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1b)
		item1NextB, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1NextB)
		_, err = iter2.Next(ctx)
		require.ErrorIs(t, err, mockedError)

		// client 1 is stopped. However, since client 2 is not stopped,
		// the iterator is still shared.
		iter1.Stop()

		// client 3
		iter3, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter3.Stop()
		item1NextC, err := iter3.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1NextC)
		_, err = iter3.Next(ctx)
		require.ErrorIs(t, err, mockedError)
	})

	t.Run("ignore_context_cancel_error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, storage.ErrIteratorDone),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter1, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		// iter1 will be stopped later
		cancel()
		_, err = iter1.Next(ctx)
		require.ErrorIs(t, err, context.Canceled)

		ctx = context.Background()
		iter2, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		// iter2 will be stopped later
		tup1, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, tup1)
		iter1.Stop()
		iter2.Stop()
	})

	t.Run("error_in_second_iter_fourth_item", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}
		tupleTwo := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:2"), Timestamp: ts}
		tupleThree := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:3"), Timestamp: ts}
		mockedError := errors.New("mocked error")
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleTwo, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleThree, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, mockedError),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter1, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		// iter1 will be stopped later (but before iter3 is created)
		item1a, err := iter1.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1a)

		// client 2
		iter2, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter2.Stop()

		iter1.Stop()

		item1b, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1b)
		item2b, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleTwo, item2b)
		item3b, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleThree, item3b)
		_, err = iter2.Next(ctx)
		require.ErrorIs(t, err, mockedError)

		// client 3
		iter3, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter3.Stop()
		item1c, err := iter3.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1c)
		item2c, err := iter3.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleTwo, item2c)
		item3c, err := iter3.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleThree, item3c)
		_, err = iter3.Head(ctx)
		require.ErrorIs(t, err, mockedError)
		_, err = iter3.Next(ctx)
		require.ErrorIs(t, err, mockedError)
	})
	t.Run("error_in_third_iter_fourth_item", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))
		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		ts := timestamppb.New(time.Now())
		tupleOne := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:1"), Timestamp: ts}
		tupleTwo := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:2"), Timestamp: ts}
		tupleThree := &openfgav1.Tuple{Key: tuple.NewTupleKey("license:1", "owner", "user:3"), Timestamp: ts}
		mockedError := errors.New("mocked error")
		gomock.InOrder(
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleOne, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleTwo, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(tupleThree, nil),
			mockIterator.EXPECT().Next(gomock.Any()).Return(nil, mockedError),
			mockIterator.EXPECT().Stop(),
		)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		iter1, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		// iter1 will be stopped later (but before iter3 is created)
		item1a, err := iter1.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1a)

		// client 2
		iter2, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter2.Stop()

		iter1.Stop()

		// client 3
		iter3, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter3.Stop()
		item1c, err := iter3.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1c)
		item2c, err := iter3.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleTwo, item2c)
		item3c, err := iter3.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleThree, item3c)
		_, err = iter3.Head(ctx)
		require.ErrorIs(t, err, mockedError)
		_, err = iter3.Next(ctx)
		require.ErrorIs(t, err, mockedError)

		item1b, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleOne, item1b)
		item2b, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleTwo, item2b)
		item3b, err := iter2.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, tupleThree, item3b)
		_, err = iter2.Next(ctx)
		require.ErrorIs(t, err, mockedError)
	})

	t.Run("clone_after_new_admission_time", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := NewSharedIteratorDatastoreStorage()
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()),
			WithMaxAdmissionTime(500*time.Millisecond))

		mockIterator := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		mockIterator.EXPECT().Stop()

		mockIterator2 := mocks.NewMockIterator[*openfgav1.Tuple](mockController)
		mockIterator2.EXPECT().Next(gomock.Any()).Return(nil, storage.ErrIteratorDone)
		mockIterator2.EXPECT().Stop()

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator, nil)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(mockIterator2, nil)
		iter1, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter1.Stop()
		// sleep time is smaller than watchdog but > admission time
		time.Sleep(1 * time.Second)
		iter2, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter2.Stop()
		_, err = iter2.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})
}
