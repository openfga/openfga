package graph

import (
	"context"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestCacheController_ResolveCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	cache := mocks.NewMockInMemoryCache[any](ctrl)
	ds := mocks.NewMockOpenFGADatastore(ctrl)
	mockResolver := NewMockCheckResolver(ctrl)

	cacheController := NewCacheController(WithDatastore(ds), WithCache(cache), WithTTL(10*time.Second))
	cacheController.SetDelegate(mockResolver)
	storeID := "id"

	t.Run("high_consistency", func(t *testing.T) {
		req := &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: "33",
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:abc",
				Relation: "reader",
				User:     "user:XYZ",
			},
			RequestMetadata: NewCheckRequestMetadata(20),
			Consistency:     openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		}
		mockResolver.EXPECT().ResolveCheck(gomock.Any(), req)
		_, err := cacheController.ResolveCheck(ctx, req)
		require.NoError(t, err)
	})
	t.Run("cache_hit", func(t *testing.T) {
		cache.EXPECT().Get(storage.GetChangelogCacheKey(storeID)).Return(&storage.CachedResult[any]{Value: &storage.ChangelogCacheEntry{LastModified: time.Now()}})
		req := &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: "33",
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:abc",
				Relation: "reader",
				User:     "user:XYZ",
			},
			RequestMetadata: NewCheckRequestMetadata(20),
		}
		mockResolver.EXPECT().ResolveCheck(gomock.Any(), req)
		_, err := cacheController.ResolveCheck(ctx, req)
		require.NoError(t, err)
	})
	t.Run("cache_miss", func(t *testing.T) {
		changelogTimestamp := time.Now().Add(-20 * time.Second)

		gomock.InOrder(
			cache.EXPECT().Get(storage.GetChangelogCacheKey(storeID)).Return(nil),
			ds.EXPECT().ReadChanges(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return([]*openfgav1.TupleChange{
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(changelogTimestamp),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "viewer",
						User:     "test",
					}},
			}, []byte{}, nil),
			cache.EXPECT().Set(storage.GetChangelogCacheKey(storeID), gomock.Any(), gomock.Any()),
		)
		req := &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: "33",
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:abc",
				Relation: "reader",
				User:     "user:XYZ",
			},
			RequestMetadata: NewCheckRequestMetadata(20),
		}
		assertReq := req.clone()
		assertReq.LastChangelogTime = timestamppb.New(changelogTimestamp).AsTime()
		mockResolver.EXPECT().ResolveCheck(gomock.Any(), assertReq)
		_, err := cacheController.ResolveCheck(ctx, req)
		require.NoError(t, err)
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

func TestCacheController_findChangesAndInvalidate(t *testing.T) {
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
						Object:   "test",
						Relation: "viewer",
						User:     "test",
					},
				},
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.UnixMilli(1257894000000)),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "writer",
						User:     "test",
					},
				},
			}},
			setCacheKeys: []string{
				storage.GetChangelogCacheKey("5"),
				storage.GetInvalidIteratorByObjectRelationCacheKey("5", "test", "viewer")},
		},
		{
			name:    "last_change_is_halfway_in_the_newest_batch",
			storeID: "6",
			readChangesResults: &readChangesResponse{err: nil, changes: []*openfgav1.TupleChange{
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.Now()),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "viewer",
						User:     "test",
					},
				},
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.UnixMilli(1257894000000)),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "writer",
						User:     "test",
					},
				},
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.UnixMilli(1257894000000 - 1)),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "writer",
						User:     "test",
					},
				},
				{
					Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
					Timestamp: timestamppb.New(time.UnixMilli(1257894000000 - 2)),
					TupleKey: &openfgav1.TupleKey{
						Object:   "test",
						Relation: "writer",
						User:     "test",
					},
				},
			}},
			setCacheKeys: []string{
				storage.GetChangelogCacheKey("6"),
				storage.GetInvalidIteratorByObjectRelationCacheKey("6", "test", "viewer")},
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
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			cache := mocks.NewMockInMemoryCache[any](ctrl)
			for _, k := range test.setCacheKeys {
				cache.EXPECT().Set(k, gomock.Any(), gomock.Any())
			}

			datastore := mocks.NewMockOpenFGADatastore(ctrl)
			datastore.EXPECT().ReadChanges(gomock.Any(), test.storeID, gomock.Any(), gomock.Any()).Return(test.readChangesResults.changes, []byte{}, test.readChangesResults.err)
			cacheController := NewCacheController(WithDatastore(datastore), WithCache(cache), WithTTL(10*time.Second))
			_, err := cacheController.findChangesAndInvalidate(ctx, test.storeID)
			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				return
			}
		})
	}
}
