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
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestSharedIteratorDatastore_Read(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	storeID := ulid.Make().String()
	internalStorage := NewSharedIteratorDatastoreStorage()
	ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)

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

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	t.Run("single_client", func(t *testing.T) {
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

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
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		iter.Stop() // has to be sync otherwise the assertion fails

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
		// make sure the internal map is deallocated
		internalStorage.mu.Lock()
		require.Empty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
	})

	t.Run("multiple_concurrent_clients", func(t *testing.T) {
		const numClient = 3
		for i := 0; i < numClient; i++ {
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator(tuples), nil)
		}

		type iteratorInfo struct {
			iter storage.TupleIterator
			err  error
		}
		iterInfos := make([]iteratorInfo, numClient)
		wg := sync.WaitGroup{}

		for i := 0; i < numClient; i++ {
			wg.Add(1)
			go func(i int) {
				curIter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
				iterInfos[i] = iteratorInfo{curIter, err}
				wg.Done()
			}(i)
		}
		wg.Wait()

		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
		for i := 0; i < numClient; i++ {
			require.NoError(t, iterInfos[i].err)
			require.NotNil(t, iterInfos[i].iter)
		}

		for _, iterInfo := range iterInfos {
			var actual []*openfgav1.Tuple
			iter := iterInfo.iter

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
			if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		}

		// make sure the internal map has not deallocated
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		for i, iterInfo := range iterInfos {
			iterInfo.iter.Stop()
			internalStorage.mu.Lock()
			if i < numClient-1 {
				require.NotEmpty(t, internalStorage.iters)
			} else {
				require.Empty(t, internalStorage.iters)
			}
			internalStorage.mu.Unlock()
		}
	})
	t.Run("error_when_querying", func(t *testing.T) {
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(nil, fmt.Errorf("mock_error"))
		_, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.Error(t, err)

		// 2 seconds should be sufficient for the cleanup goroutine to run
		time.Sleep(2 * time.Second)
		internalStorage.mu.Lock()
		require.Empty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
	})
	t.Run("bypass_due_to_map_size_limit", func(t *testing.T) {
		internalStorageLimit := NewSharedIteratorDatastoreStorage(WithSharedIteratorDatastoreStorageLimit(0))
		dsLimit := NewSharedIteratorDatastore(mockDatastore, internalStorageLimit)

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := dsLimit.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		internalStorageLimit.mu.Lock()
		// this should not come from the map
		require.Empty(t, internalStorageLimit.iters)
		internalStorageLimit.mu.Unlock()

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
}

func TestSharedIteratorDatastore_deref(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	internalStorage := NewSharedIteratorDatastoreStorage()
	ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)

	result, err := ds.deref("key1")
	require.Error(t, err)
	require.False(t, result)

	internalStorage.iters["key1"] = &internalSharedIterator{
		counter: 2,
	}
	result, err = ds.deref("key1")
	require.NoError(t, err)
	require.False(t, result)

	result, err = ds.deref("key1")
	require.NoError(t, err)
	require.True(t, result)
}

func TestSharedIteratorDatastore_ReadUsersetTuples(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	storeID := ulid.Make().String()
	internalStorage := NewSharedIteratorDatastoreStorage()
	ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)

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

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
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
		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

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
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		iter.Stop() // has to be sync otherwise the assertion fails

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
		// make sure the internal map is deallocated
		internalStorage.mu.Lock()
		require.Empty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
	})

	t.Run("multiple_concurrent_clients", func(t *testing.T) {
		const numClient = 3
		for i := 0; i < numClient; i++ {
			mockDatastore.EXPECT().
				ReadUsersetTuples(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator(tuples), nil)
		}

		type iteratorInfo struct {
			iter storage.TupleIterator
			err  error
		}
		iterInfos := make([]iteratorInfo, numClient)
		wg := sync.WaitGroup{}

		for i := 0; i < numClient; i++ {
			wg.Add(1)
			go func(i int) {
				curIter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
				iterInfos[i] = iteratorInfo{curIter, err}
				wg.Done()
			}(i)
		}
		wg.Wait()

		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
		for i := 0; i < numClient; i++ {
			require.NoError(t, iterInfos[i].err)
			require.NotNil(t, iterInfos[i].iter)
		}

		for _, iterInfo := range iterInfos {
			var actual []*openfgav1.Tuple
			iter := iterInfo.iter

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
			if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		}

		// make sure the internal map has not deallocated
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		for i, iterInfo := range iterInfos {
			iterInfo.iter.Stop()
			internalStorage.mu.Lock()
			if i < numClient-1 {
				require.NotEmpty(t, internalStorage.iters)
			} else {
				require.Empty(t, internalStorage.iters)
			}
			internalStorage.mu.Unlock()
		}
	})
	t.Run("error_when_querying", func(t *testing.T) {
		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), storeID, filter, options).
			Return(nil, fmt.Errorf("mock_error"))
		_, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.Error(t, err)

		// 2 seconds should be sufficient for the cleanup goroutine to run
		time.Sleep(2 * time.Second)
		internalStorage.mu.Lock()
		require.Empty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
	})
	t.Run("bypass_due_to_map_size_limit", func(t *testing.T) {
		internalStorageLimit := NewSharedIteratorDatastoreStorage(WithSharedIteratorDatastoreStorageLimit(0))
		dsLimit := NewSharedIteratorDatastore(mockDatastore, internalStorageLimit)

		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := dsLimit.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)
		internalStorageLimit.mu.Lock()
		// this should not come from the map
		require.Empty(t, internalStorageLimit.iters)
		internalStorageLimit.mu.Unlock()

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
}

func TestSharedIteratorDatastore_ReadStartingWithUser(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	storeID := ulid.Make().String()
	internalStorage := NewSharedIteratorDatastoreStorage()
	ds := NewSharedIteratorDatastore(mockDatastore, internalStorage)

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

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
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
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

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
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		iter.Stop() // has to be sync otherwise the assertion fails

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
		// make sure the internal map is deallocated
		internalStorage.mu.Lock()
		require.Empty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
	})

	t.Run("multiple_concurrent_clients", func(t *testing.T) {
		const numClient = 3
		for i := 0; i < numClient; i++ {
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator(tuples), nil)
		}

		type iteratorInfo struct {
			iter storage.TupleIterator
			err  error
		}
		iterInfos := make([]iteratorInfo, numClient)
		wg := sync.WaitGroup{}

		for i := 0; i < numClient; i++ {
			wg.Add(1)
			go func(i int) {
				curIter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
				iterInfos[i] = iteratorInfo{curIter, err}
				wg.Done()
			}(i)
		}
		wg.Wait()

		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
		for i := 0; i < numClient; i++ {
			require.NoError(t, iterInfos[i].err)
			require.NotNil(t, iterInfos[i].iter)
		}

		for _, iterInfo := range iterInfos {
			var actual []*openfgav1.Tuple
			iter := iterInfo.iter

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
			if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		}

		// make sure the internal map has not deallocated
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		for i, iterInfo := range iterInfos {
			iterInfo.iter.Stop()
			internalStorage.mu.Lock()
			if i < numClient-1 {
				require.NotEmpty(t, internalStorage.iters)
			} else {
				require.Empty(t, internalStorage.iters)
			}
			internalStorage.mu.Unlock()
		}
	})
	t.Run("error_when_querying", func(t *testing.T) {
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(nil, fmt.Errorf("mock_error"))
		_, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.Error(t, err)

		// 2 seconds should be sufficient for the cleanup goroutine to run
		time.Sleep(2 * time.Second)
		internalStorage.mu.Lock()
		require.Empty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
	})
	t.Run("bypass_due_to_map_size_limit", func(t *testing.T) {
		internalStorageLimit := NewSharedIteratorDatastoreStorage(WithSharedIteratorDatastoreStorageLimit(0))
		dsLimit := NewSharedIteratorDatastore(mockDatastore, internalStorageLimit)

		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := dsLimit.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)
		internalStorageLimit.mu.Lock()
		// this should not come from the map
		require.Empty(t, internalStorageLimit.iters)
		internalStorageLimit.mu.Unlock()

		_, ok := iter.(*sharedIterator)
		require.False(t, ok)

		iter.Stop()
	})
	t.Run("stop_more_than_once", func(t *testing.T) {
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		mockDatastore.EXPECT().
			ReadStartingWithUser(gomock.Any(), storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter1, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		iter2, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		iter1.Stop()
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		// we call stop more than once
		iter1.Stop()
		internalStorage.mu.Lock()
		require.NotEmpty(t, internalStorage.iters)
		internalStorage.mu.Unlock()

		iter2.Stop()
		internalStorage.mu.Lock()
		require.Empty(t, internalStorage.iters)
		internalStorage.mu.Unlock()
	})
}
