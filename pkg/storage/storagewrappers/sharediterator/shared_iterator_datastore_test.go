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

type testIteratorInfo struct {
	iter storage.TupleIterator
	err  error
}

// helper function to validate the single client case.
func helperValidateSingleClient(ctx context.Context, t *testing.T, internalStorage *Storage, iter storage.TupleIterator, expected []*openfgav1.Tuple) {
	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	require.NotEmpty(t, length(&internalStorage.iters))

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

	require.NotEmpty(t, length(&internalStorage.iters))

	iter.Stop() // has to be sync otherwise the assertion fails

	if diff := cmp.Diff(expected, actual, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-want +got):\n%s", diff)
	}
	// make sure the internal map is deallocated

	require.Empty(t, length(&internalStorage.iters))
}

func helperValidateMultipleClients(ctx context.Context, t *testing.T, internalStorage *Storage, iterInfos []testIteratorInfo, expected []*openfgav1.Tuple) {
	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	require.NotEmpty(t, length(&internalStorage.iters))
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

	require.NotEmpty(t, length(&internalStorage.iters))

	for i, iterInfo := range iterInfos {
		iterInfo.iter.Stop()

		if i < len(iterInfos)-1 {
			require.NotEmpty(t, length(&internalStorage.iters))
		} else {
			require.Empty(t, length(&internalStorage.iters))
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
		helperValidateSingleClient(ctx, t, internalStorage, iter, tuples)
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
		helperValidateMultipleClients(ctx, t, internalStorage, iterInfos, tuples)
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

		require.Empty(t, length(&internalStorage.iters))
	})
	t.Run("bypass_due_to_map_size_limit", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		storeID := ulid.Make().String()

		internalStorageLimit := NewSharedIteratorDatastoreStorage(WithSharedIteratorDatastoreStorageLimit(0))
		dsLimit := NewSharedIteratorDatastore(mockDatastore, internalStorageLimit)

		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, tk, storage.ReadOptions{}).
			Return(storage.NewStaticTupleIterator(tuples), nil)
		iter, err := dsLimit.Read(ctx, storeID, tk, storage.ReadOptions{})
		require.NoError(t, err)
		// this should not come from the map
		require.Empty(t, length(&internalStorageLimit.iters))

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
		require.Empty(t, length(&ds.internalStorage.iters))

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
		require.Empty(t, length(&ds.internalStorage.iters))
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
		helperValidateSingleClient(ctx, t, internalStorage, iter, tuples)
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
		helperValidateMultipleClients(ctx, t, internalStorage, iterInfos, tuples)
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

		require.Empty(t, length(&internalStorage.iters))
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
		require.Empty(t, length(&internalStorageLimit.iters))

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
		require.Empty(t, length(&ds.internalStorage.iters))

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
		require.Empty(t, length(&ds.internalStorage.iters))
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
		helperValidateSingleClient(ctx, t, internalStorage, iter, tuples)
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
		helperValidateMultipleClients(ctx, t, internalStorage, iterInfos, tuples)
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

		require.Empty(t, length(&internalStorage.iters))
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
		require.Empty(t, length(&internalStorageLimit.iters))

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
		require.Empty(t, length(&ds.internalStorage.iters))

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

		require.NotEmpty(t, length(&internalStorage.iters))

		iter2, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)

		require.NotEmpty(t, length(&internalStorage.iters))

		iter1.Stop()

		require.NotEmpty(t, length(&internalStorage.iters))

		// we call stop more than once
		iter1.Stop()

		require.NotEmpty(t, length(&internalStorage.iters))

		iter2.Stop()

		require.Empty(t, length(&internalStorage.iters))
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
		require.Empty(t, length(&internalStorage.iters))
	})
}

// These tests will focus on the iteration itself.
func TestNewSharedIteratorDatastore_iter(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	BufferSize = 1

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
