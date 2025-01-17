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
	"github.com/openfga/openfga/pkg/logger"
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
	expectedReadChangesOpts := storage.ReadChangesOptions{
		SortDesc: true,
		Pagination: storage.PaginationOptions{
			PageSize: storage.DefaultPageSize,
			From:     "",
		}}

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
			ds.EXPECT().ReadChanges(gomock.Any(), storeID, gomock.Any(), expectedReadChangesOpts).AnyTimes().Return([]*openfgav1.TupleChange{
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(changelogTimestamp),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "viewer",
						User:     "test",
					}},
			}, "", nil),
			cache.EXPECT().Get(storage.GetChangelogCacheKey(storeID)).AnyTimes().Return(&storage.ChangelogCacheEntry{
				LastModified: time.Now().Add(-20 * time.Second),
			}),
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

	expectedReadChangesOpts := storage.ReadChangesOptions{
		SortDesc: true,
		Pagination: storage.PaginationOptions{
			PageSize: storage.DefaultPageSize,
			From:     "",
		}}

	tests := []struct {
		name     string
		storeID  string
		setMocks func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore)
	}{
		{
			name:    "empty_changelog",
			storeID: "1",
			setMocks: func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore) {
				gomock.InOrder(
					datastore.EXPECT().ReadChanges(gomock.Any(), "1", gomock.Any(), expectedReadChangesOpts).Times(1).Return(nil, "", storage.ErrNotFound),
					cache.EXPECT().Set(storage.GetInvalidIteratorCacheKey("1"), gomock.Any(), gomock.Any()),
				)
			},
		},
		{
			name:    "hard_error",
			storeID: "2",
			setMocks: func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore) {
				gomock.InOrder(
					datastore.EXPECT().ReadChanges(gomock.Any(), "2", gomock.Any(), expectedReadChangesOpts).Return(nil, "", storage.ErrCollision),
					cache.EXPECT().Set(storage.GetInvalidIteratorCacheKey("2"), gomock.Any(), gomock.Any()),
				)
			},
		},
		{
			name:    "first_change_from_empty_store",
			storeID: "3",
			setMocks: func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore) {
				gomock.InOrder(
					datastore.EXPECT().ReadChanges(gomock.Any(), "3", gomock.Any(), expectedReadChangesOpts).Return([]*openfgav1.TupleChange{
						{
							Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
							Timestamp: timestamppb.New(time.Now()),
							TupleKey: &openfgav1.TupleKey{
								Object:   "test:3",
								Relation: "viewer",
								User:     "test",
							}},
					}, "", nil),
					cache.EXPECT().Get(storage.GetChangelogCacheKey("3")).Return(&storage.ChangelogCacheEntry{LastModified: time.Now().Add(-20 * time.Second)}),
					cache.EXPECT().Set(storage.GetChangelogCacheKey("3"), gomock.Any(), gomock.Any()),
					cache.EXPECT().Set(storage.GetInvalidIteratorCacheKey("3"), gomock.Any(), gomock.Any()),
				)
			},
		},
		{
			name:    "last_change_is_same_change",
			storeID: "4",
			setMocks: func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore) {
				gomock.InOrder(
					datastore.EXPECT().ReadChanges(gomock.Any(), "4", gomock.Any(), expectedReadChangesOpts).Return([]*openfgav1.TupleChange{
						{
							Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
							Timestamp: timestamppb.New(time.Now().Add(-20 * time.Second)),
							TupleKey: &openfgav1.TupleKey{
								Object:   "test:4",
								Relation: "viewer",
								User:     "test",
							}},
					}, "", nil),
					cache.EXPECT().Get(storage.GetChangelogCacheKey("4")).Return(&storage.ChangelogCacheEntry{LastModified: time.Now().Add(-20 * time.Second)}),
					cache.EXPECT().Set(storage.GetChangelogCacheKey("4"), gomock.Any(), gomock.Any()),
				)
			},
		},
		{
			name:    "last_change_is_in_the_newest_batch",
			storeID: "5",
			setMocks: func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore) {
				gomock.InOrder(
					datastore.EXPECT().ReadChanges(gomock.Any(), "5", gomock.Any(), expectedReadChangesOpts).Return([]*openfgav1.TupleChange{
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
					}, "", nil),
					cache.EXPECT().Get(storage.GetChangelogCacheKey("5")).Return(&storage.ChangelogCacheEntry{LastModified: time.Now().Add(-20 * time.Second)}),
					cache.EXPECT().Set(storage.GetChangelogCacheKey("5"), gomock.Any(), gomock.Any()),
					cache.EXPECT().Set(storage.GetInvalidIteratorByObjectRelationCacheKeys("5", "test:5", "viewer")[0], gomock.Any(), gomock.Any()),
					cache.EXPECT().Set(storage.GetInvalidIteratorByUserObjectTypeCacheKeys("5", []string{"test"}, "test")[0], gomock.Any(), gomock.Any()),
				)
			},
		},
		{
			name:    "last_change_is_halfway_in_the_newest_batch",
			storeID: "6",
			setMocks: func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore) {
				gomock.InOrder(
					datastore.EXPECT().ReadChanges(gomock.Any(), "6", gomock.Any(), expectedReadChangesOpts).Return([]*openfgav1.TupleChange{
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
					}, "", nil),
					cache.EXPECT().Get(storage.GetChangelogCacheKey("6")).Return(&storage.ChangelogCacheEntry{LastModified: time.Now().Add(-20 * time.Second)}),
					cache.EXPECT().Set(storage.GetChangelogCacheKey("6"), gomock.Any(), gomock.Any()),
					cache.EXPECT().Set(storage.GetInvalidIteratorByObjectRelationCacheKeys("6", "test:5", "viewer")[0], gomock.Any(), gomock.Any()),
					cache.EXPECT().Set(storage.GetInvalidIteratorByUserObjectTypeCacheKeys("6", []string{"test"}, "test")[0], gomock.Any(), gomock.Any()),
				)
			},
		},
		{
			name:    "last_change_not_in_newest_batch",
			storeID: "7",
			setMocks: func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore) {
				gomock.InOrder(
					datastore.EXPECT().ReadChanges(gomock.Any(), "7", gomock.Any(), expectedReadChangesOpts).Return(
						generateChanges("test", "relation", "user", 50), "", nil),
					cache.EXPECT().Get(storage.GetChangelogCacheKey("7")).Return(&storage.ChangelogCacheEntry{LastModified: time.Now().Add(-20 * time.Second)}),
					cache.EXPECT().Set(storage.GetChangelogCacheKey("7"), gomock.Any(), gomock.Any()),
					cache.EXPECT().Set(storage.GetInvalidIteratorCacheKey("7"), gomock.Any(), gomock.Any()),
				)
			},
		},
		{
			name:    "initial_check_for_invalidation",
			storeID: "8",
			setMocks: func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore) {
				gomock.InOrder(
					datastore.EXPECT().ReadChanges(gomock.Any(), "8", gomock.Any(), expectedReadChangesOpts).Return([]*openfgav1.TupleChange{
						{
							Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
							Timestamp: timestamppb.New(time.Now().Add(-20 * time.Second)),
							TupleKey: &openfgav1.TupleKey{
								Object:   "test:8",
								Relation: "viewer",
								User:     "test",
							}},
					}, "", nil),
					cache.EXPECT().Get(storage.GetChangelogCacheKey("8")).Return(nil),
					cache.EXPECT().Set(storage.GetChangelogCacheKey("8"), gomock.Any(), gomock.Any()),
					cache.EXPECT().Set(storage.GetInvalidIteratorCacheKey("8"), gomock.Any(), gomock.Any()),
				)
			},
		},
		{
			name:    "initial_check_for_invalidation_change_is_recent",
			storeID: "9",
			setMocks: func(cache *mocks.MockInMemoryCache[any], datastore *mocks.MockOpenFGADatastore) {
				gomock.InOrder(
					datastore.EXPECT().ReadChanges(gomock.Any(), "9", gomock.Any(), expectedReadChangesOpts).Return([]*openfgav1.TupleChange{
						{
							Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
							Timestamp: timestamppb.New(time.Now().Add(-5 * time.Second)),
							TupleKey: &openfgav1.TupleKey{
								Object:   "test:9",
								Relation: "viewer",
								User:     "test",
							}},
					}, "", nil),
					// there should be no difference with initial_check_for_invalidation case except to
					// verify the double negative case.
					cache.EXPECT().Get(storage.GetChangelogCacheKey("9")).Return(nil),
					cache.EXPECT().Set(storage.GetChangelogCacheKey("9"), gomock.Any(), gomock.Any()),
					cache.EXPECT().Set(storage.GetInvalidIteratorCacheKey("9"), gomock.Any(), gomock.Any()),
				)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, span := tracer.Start(context.Background(), "cachecontroller_test")
			defer span.End()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCache := mocks.NewMockInMemoryCache[any](ctrl)
			mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)

			test.setMocks(mockCache, mockDatastore)

			cacheController := &InMemoryCacheController{
				ds:                    mockDatastore,
				cache:                 mockCache,
				ttl:                   10 * time.Second,
				iteratorCacheTTL:      10 * time.Second,
				changelogBuckets:      []uint{0, 25, 50, 75, 100},
				inflightInvalidations: sync.Map{},
				logger:                logger.NewNoopLogger(),
			}
			cacheController.findChangesAndInvalidate(context.Background(), test.storeID, span)
		})
	}
}
