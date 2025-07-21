package sharediterator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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
	sharedIter := newSharedIterator(staticIter)
	defer sharedIter.Stop()

	b.ResetTimer()

	b.SetParallelism(100)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Clone the shared iterator for each goroutine
			var clonedIter sharedIterator
			cloned := sharedIter.clone(&clonedIter)
			if !cloned {
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
	sharedIter := newSharedIterator(staticIter)
	defer sharedIter.Stop()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Clone the shared iterator for each goroutine
			var clonedIter sharedIterator
			cloned := sharedIter.clone(&clonedIter)
			if !cloned {
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
		sharedIter := newSharedIterator(staticIter)
		defer sharedIter.Stop()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var clonedIter sharedIterator
			cloned := sharedIter.clone(&clonedIter)
			if !cloned {
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

func BenchmarkIteratorDatastoreReadLatencyWithDifferentLoads(b *testing.B) {
	ctx := context.Background()

	// Create test data
	var tuples []*openfgav1.Tuple
	for i := 0; i < 50; i++ {
		tk := tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", fmt.Sprintf("user:%d", i))
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	storeID := ulid.Make().String()
	tk := tuple.NewTupleKey("document:1", "viewer", "")

	// Test with different concurrency levels
	concurrencyLevels := []int{1, 10, 50, 100, 200, 500}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			var latencies []time.Duration
			var mu sync.Mutex

			var dbCalls atomic.Int64

			mockController := gomock.NewController(b)
			defer mockController.Finish()
			mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

			// Setup shared iterator datastore
			internalStorage := new(Storage)
			ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
				WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

			// Mock expects single call due to sharing
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
				DoAndReturn(func(_ context.Context, _ string, _ *openfgav1.TupleKey, _ storage.ReadOptions) (storage.TupleIterator, error) {
					time.Sleep(5 * time.Millisecond)
					dbCalls.Add(1)
					return storage.NewStaticTupleIterator(tuples), nil
				}).AnyTimes()

			for b.Loop() {
				var wg sync.WaitGroup

				for j := 0; j < concurrency; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						start := time.Now()
						iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
						if err != nil {
							b.Errorf("Failed to create iterator: %v", err)
							return
						}

						mu.Lock()
						latencies = append(latencies, time.Since(start))
						mu.Unlock()

						iter.Stop()
					}()
				}
				wg.Wait()
			}

			b.StopTimer()

			// Report metrics
			b.ReportMetric(float64(dbCalls.Load()), "db_calls")

			if len(latencies) > 0 {
				sort.Slice(latencies, func(i, j int) bool {
					return latencies[i] < latencies[j]
				})

				var total time.Duration
				for _, lat := range latencies {
					total += lat
				}
				avg := total / time.Duration(len(latencies))

				p95 := latencies[len(latencies)*95/100]
				p99 := latencies[len(latencies)*99/100]

				max := latencies[len(latencies)-1]

				min := latencies[0]

				b.ReportMetric(float64(avg.Microseconds()), "avg_latency_us")
				b.ReportMetric(float64(p95.Microseconds()), "p95_latency_us")
				b.ReportMetric(float64(p99.Microseconds()), "p99_latency_us")
				b.ReportMetric(float64(max.Microseconds()), "max_latency_us")
				b.ReportMetric(float64(min.Microseconds()), "min_latency_us")
			}
		})
	}
}

// helper function to validate the single client case.
func helperValidateSingleClient(ctx context.Context, t *testing.T, iter storage.TupleIterator, expected []*openfgav1.Tuple) {
	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

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

	iter.Stop() // has to be sync otherwise the assertion fails

	if diff := cmp.Diff(expected, actual, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-want +got):\n%s", diff)
	}
}

func helperValidateMultipleClients(ctx context.Context, t *testing.T, iterInfos []testIteratorInfo, expected []*openfgav1.Tuple) {
	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

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
		internalStorage := new(Storage)
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		helperValidateSingleClient(ctx, t, iter, tuples)
	})

	t.Run("multiple_concurrent_clients", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
		helperValidateMultipleClients(ctx, t, iterInfos, tuples)
	})
	t.Run("error_when_querying", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
	})

	t.Run("bypass_due_to_strong_consistency", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("multiple_concurrent_clients_read_and_done", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)
		helperValidateSingleClient(ctx, t, iter, tuples)
	})

	t.Run("multiple_concurrent_clients", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
		helperValidateMultipleClients(ctx, t, iterInfos, tuples)
	})

	t.Run("error_when_querying", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
	})

	t.Run("bypass_due_to_strong_consistency", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("multiple_concurrent_clients_read_and_done", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)
		helperValidateSingleClient(ctx, t, iter, tuples)
	})

	t.Run("multiple_concurrent_clients", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
		helperValidateMultipleClients(ctx, t, iterInfos, tuples)
	})
	t.Run("error_when_querying", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
	})

	t.Run("bypass_due_to_strong_consistency", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("stop_more_than_once", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter1, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)

		iter2, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)

		iter1.Stop()

		// we call stop more than once
		iter1.Stop()

		iter2.Stop()
	})
	t.Run("multiple_concurrent_clients_read_and_done", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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

	t.Run("head_empty_list_head", func(t *testing.T) {
		ctx := context.Background()
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
		internalStorage := new(Storage)
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
}

func TestSharedIterator_ManyTuples(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// Create 150 test tuples
	const numTuples = bufferSize + 1
	var tks []*openfgav1.TupleKey
	for i := 0; i < numTuples; i++ {
		tks = append(tks, tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", fmt.Sprintf("user:%d", i)))
	}

	var tuples []*openfgav1.Tuple
	ts := timestamppb.New(time.Now())
	for _, tk := range tks {
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
	}

	tk := tuple.NewTupleKey("document:*", "viewer", "")

	t.Run("single_client", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()
		internalStorage := new(Storage)
		ds := NewSharedIteratorDatastore(mockDatastore, internalStorage,
			WithSharedIteratorDatastoreLogger(logger.NewNoopLogger()))

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(storage.NewStaticTupleIterator(tuples), nil)

		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		// Verify we can read all tuples
		var receivedTuples []*openfgav1.Tuple
		for {
			tup, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "unexpected error", err)
			}
			receivedTuples = append(receivedTuples, tup)
		}

		// Assert we received all tuples
		require.Len(t, receivedTuples, numTuples, "should receive all tuples")

		// Verify the tuples match (order should be preserved)
		cmpOpts := []cmp.Option{
			testutils.TupleKeyCmpTransformer,
			protocmp.Transform(),
		}
		if diff := cmp.Diff(tuples, receivedTuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})
}
