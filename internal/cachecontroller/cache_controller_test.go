package cachecontroller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestNoopCacheController_DetermineInvalidation(t *testing.T) {
	t.Run("returns_zero_time", func(t *testing.T) {
		ctrl := NewNoopCacheController()
		require.Zero(t, ctrl.DetermineInvalidation(context.Background(), ""))
	})
}

func TestInMemoryCacheController_DetermineInvalidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	cache := mocks.NewMockInMemoryCache[any](ctrl)
	ds := mocks.NewMockOpenFGADatastore(ctrl)

	cacheController := NewCacheController(ds, cache, 10*time.Second, 10*time.Second)
	storeID := "id"

	t.Run("cache_hit", func(t *testing.T) {
		cache.EXPECT().Get(storage.GetChangelogCacheKey(storeID)).
			Return(&storage.ChangelogCacheEntry{LastModified: time.Now()})

		invalidationTime := cacheController.DetermineInvalidation(ctx, storeID)
		require.NotZero(t, invalidationTime)
	})
	t.Run("cache_miss", func(t *testing.T) {
		changelogTimestamp := time.Now().UTC().Add(-20 * time.Second)

		gomock.InOrder(
			cache.EXPECT().Get(storage.GetChangelogCacheKey(storeID)).Return(nil),
			ds.EXPECT().ReadChanges(gomock.Any(), storeID, gomock.Any(), gomock.Any()).AnyTimes().Return([]*openfgav1.TupleChange{
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(changelogTimestamp),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "viewer",
						User:     "test",
					}},
			}, "", nil),
			cache.EXPECT().Set(storage.GetChangelogCacheKey(storeID), gomock.Any(), gomock.Any()).AnyTimes(),
		)
		invalidationTime := cacheController.DetermineInvalidation(ctx, storeID)
		require.Equal(t, time.Time{}, invalidationTime)
	})
}

func generateChanges(object, relation, user string, count int) []*openfgav1.TupleChange {
	changes := make([]*openfgav1.TupleChange, 0, count)
	for i := 0; i < count; i++ {
		changes = append(changes, &openfgav1.TupleChange{
			TupleKey: &openfgav1.TupleKey{
				User:     user,
				Relation: relation,
				Object:   object,
			},
			Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			Timestamp: timestamppb.New(time.Now()),
		})
	}
	return changes
}

func TestInMemoryCacheController_findChangesAndInvalidate(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	type readChangesResponse struct {
		err     error
		changes []*openfgav1.TupleChange
	}

	tests := []struct {
		name               string
		storeID            string
		continuationToken  string
		readChangesResults *readChangesResponse
		setCacheKeys       []string
		expectedError      error
	}{
		{
			name:               "empty_changelog",
			storeID:            "1",
			readChangesResults: &readChangesResponse{err: storage.ErrNotFound},
			setCacheKeys: []string{
				storage.GetInvalidIteratorCacheKey("1")},
			expectedError: storage.ErrNotFound,
		},
		{
			name:               "hard_error",
			storeID:            "2",
			readChangesResults: &readChangesResponse{err: storage.ErrCollision},
			setCacheKeys: []string{
				storage.GetInvalidIteratorCacheKey("2"),
			},
			expectedError: storage.ErrCollision,
		},
		{
			name:    "first_change_from_empty_store",
			storeID: "3",
			readChangesResults: &readChangesResponse{err: nil, changes: []*openfgav1.TupleChange{
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.Now()),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "viewer",
						User:     "test",
					}},
			}},
			setCacheKeys: []string{
				storage.GetChangelogCacheKey("3"),
				storage.GetInvalidIteratorCacheKey("3"),
			},
		},
		{
			name:    "last_change_is_same_change",
			storeID: "4",
			readChangesResults: &readChangesResponse{err: nil, changes: []*openfgav1.TupleChange{
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.Now().Add(-20 * time.Second)),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "viewer",
						User:     "test",
					}},
			}},
			setCacheKeys: []string{
				storage.GetChangelogCacheKey("4"),
			},
		},
		{
			name:    "last_change_is_in_the_newest_batch",
			storeID: "5",
			readChangesResults: &readChangesResponse{err: nil, changes: []*openfgav1.TupleChange{
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.Now()),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test:5",
						Relation: "viewer",
						User:     "test",
					},
				},
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.Now().UTC().Add(-50 * time.Second)),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test:5",
						Relation: "writer",
						User:     "test",
					},
				},
			}},
			setCacheKeys: []string{
				storage.GetChangelogCacheKey("5"),
				storage.GetInvalidIteratorByObjectRelationCacheKeys("5", "test:5", "viewer")[0],
				storage.GetInvalidIteratorByUserObjectTypeCacheKeys("5", []string{"test"}, "test")[0]},
		},
		{
			name:    "last_change_is_halfway_in_the_newest_batch",
			storeID: "6",
			readChangesResults: &readChangesResponse{err: nil, changes: []*openfgav1.TupleChange{
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.Now()),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test:5",
						Relation: "viewer",
						User:     "test",
					},
				},
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.Now().Add(-10 * time.Second)),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test:6",
						Relation: "writer",
						User:     "test",
					},
				},
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.Now().Add(-11 * time.Second)),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test:7",
						Relation: "writer",
						User:     "test",
					},
				},
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.Now().Add(-12 * time.Second)),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test:8",
						Relation: "writer",
						User:     "test",
					},
				},
			}},
			setCacheKeys: []string{
				storage.GetChangelogCacheKey("6"),
				storage.GetInvalidIteratorByObjectRelationCacheKeys("6", "test:5", "viewer")[0],
				storage.GetInvalidIteratorByUserObjectTypeCacheKeys("6", []string{"test"}, "test")[0]},
		},
		{
			name:               "last_change_not_in_newest_batch",
			storeID:            "7",
			readChangesResults: &readChangesResponse{err: nil, changes: generateChanges("test", "relation", "user", 50)},
			setCacheKeys: []string{
				storage.GetChangelogCacheKey("7"),
				storage.GetInvalidIteratorCacheKey("7")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			cache := mocks.NewMockInMemoryCache[any](ctrl)
			for _, k := range test.setCacheKeys {
				cache.EXPECT().Set(k, gomock.Any(), gomock.Any())
			}

			datastore := mocks.NewMockOpenFGADatastore(ctrl)
			datastore.EXPECT().ReadChanges(gomock.Any(), test.storeID, gomock.Any(), gomock.Any()).Return(test.readChangesResults.changes, "", test.readChangesResults.err)
			cacheController := &InMemoryCacheController{
				ds:                    datastore,
				cache:                 cache,
				ttl:                   10 * time.Second,
				iteratorCacheTTL:      10 * time.Second,
				changelogBuckets:      []uint{0, 25, 50, 75, 100},
				mu:                    sync.Mutex{},
				inflightInvalidations: make(map[string]struct{}),
			}
			err := cacheController.findChangesAndInvalidate(ctx, test.storeID)
			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				return
			}
		})
	}
}
