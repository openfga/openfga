package check

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestResolveUnionEdges(t *testing.T) {
	t.Run("short_circuit_on_first_true", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user] or admin
                define admin: [user]
        `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		// First edge returns true - should short circuit
		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, tuple.NewTupleKey("group:1", "admin", "user:maria"), gomock.Any()).
			Return(nil, storage.ErrNotFound).Times(1)
		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, tuple.NewTupleKey("group:1", "member", "user:maria"), gomock.Any()).
			Return(&openfgav1.Tuple{Key: tuple.NewTupleKey("group:1", "member", "user:maria")}, nil).Times(1)
		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user")
		require.NoError(t, err)
		require.True(t, ok)

		res, err := resolver.ResolveUnionEdges(context.Background(), req, edges, nil)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("all_edges_false", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user] or admin
                define admin: [user]
        `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		// Both edges return false (not found)
		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			Return(nil, storage.ErrNotFound).Times(2)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user")
		require.NoError(t, err)
		require.True(t, ok)

		res, err := resolver.ResolveUnionEdges(context.Background(), req, edges, nil)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("cache_hit_on_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user] or admin
                define admin: [user]
        `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		cachedEntry := &ResponseCacheEntry{
			Res:          &Response{Allowed: true},
			LastModified: time.Now(),
		}

		resolver := New(Config{
			Model:                     mg,
			Datastore:                 mockDatastore,
			Cache:                     mockCache,
			ConcurrencyLimit:          10,
			LastCacheInvalidationTime: time.Now().Add(-time.Hour),
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		mockCache.EXPECT().Get(req.GetCacheKey()).Return(cachedEntry).Times(1)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user")
		require.NoError(t, err)
		require.True(t, ok)

		res, err := resolver.ResolveUnionEdges(context.Background(), req, edges, nil)
		require.NoError(t, err)
		require.True(t, res.Allowed)
	})

	t.Run("partial_cache_hits", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user] or admin or owner
                define admin: [user]
                define owner: [user]
        `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		cachedFalse := &ResponseCacheEntry{
			Res:          &Response{Allowed: false},
			LastModified: time.Now(),
		}

		// First call checks union cache (miss), then checks edge caches
		mockCache.EXPECT().Get(gomock.Any()).Return(nil).Times(1)
		mockCache.EXPECT().Get(gomock.Any()).Return(cachedFalse).Times(1)
		mockCache.EXPECT().Get(gomock.Any()).Return(nil).Times(1)
		mockCache.EXPECT().Get(gomock.Any()).Return(nil).Times(1)

		// Only two edges need to be evaluated (one was cached)
		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			Return(&openfgav1.Tuple{Key: tuple.NewTupleKey("group:1", "owner", "user:maria")}, nil).Times(2)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:                     mg,
			Datastore:                 mockDatastore,
			Cache:                     mockCache,
			ConcurrencyLimit:          10,
			LastCacheInvalidationTime: time.Now().Add(-time.Hour),
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user")
		require.NoError(t, err)
		require.True(t, ok)

		res, err := resolver.ResolveUnionEdges(context.Background(), req, edges, nil)
		require.NoError(t, err)
		require.True(t, res.Allowed)
	})

	t.Run("error_on_one_edge_returns_error_when_all_fail", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user] or admin
                define admin: [user]
        `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expectedErr := errors.New("database error")
		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			Return(nil, expectedErr).MinTimes(1).MaxTimes(2)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user")
		require.NoError(t, err)
		require.True(t, ok)

		res, err := resolver.ResolveUnionEdges(context.Background(), req, edges, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, res)
	})

	t.Run("context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user] or admin
                define admin: [user]
        `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, string, *openfgav1.TupleKey, storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				cancel()
				time.Sleep(10 * time.Millisecond)
				return nil, storage.ErrNotFound
			}).MaxTimes(2)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user")
		require.NoError(t, err)
		require.True(t, ok)

		res, err := resolver.ResolveUnionEdges(ctx, req, edges, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, res)
	})

	t.Run("empty_edges", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
        `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("user:1", "self", "user:maria"),
		})
		require.NoError(t, err)

		res, err := resolver.ResolveUnionEdges(context.Background(), req, []*authzGraph.WeightedAuthorizationModelEdge{}, nil)
		require.NoError(t, err)
		require.False(t, res.Allowed)
	})
}

func TestIsCached(t *testing.T) {
	t.Run("returns_false_when_higher_consistency_requested", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		resolver := &Resolver{
			cache:                     mockCache,
			lastCacheInvalidationTime: time.Now().Add(-time.Hour),
		}

		res, ok := resolver.isCached(openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY, "test-key")
		require.False(t, ok)
		require.Nil(t, res)
	})

	t.Run("returns_false_when_last_cache_invalidation_time_is_zero", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		resolver := &Resolver{
			cache:                     mockCache,
			lastCacheInvalidationTime: time.Time{},
		}

		res, ok := resolver.isCached(openfgav1.ConsistencyPreference_MINIMIZE_LATENCY, "test-key")
		require.False(t, ok)
		require.Nil(t, res)
	})

	t.Run("returns_false_when_cache_entry_not_found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockCache.EXPECT().Get("test-key").Return(nil).Times(1)

		resolver := &Resolver{
			cache:                     mockCache,
			lastCacheInvalidationTime: time.Now().Add(-time.Hour),
		}

		res, ok := resolver.isCached(openfgav1.ConsistencyPreference_MINIMIZE_LATENCY, "test-key")
		require.False(t, ok)
		require.Nil(t, res)
	})

	t.Run("returns_false_when_cache_entry_wrong_type", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockCache.EXPECT().Get("test-key").Return("wrong-type").Times(1)

		resolver := &Resolver{
			cache:                     mockCache,
			lastCacheInvalidationTime: time.Now().Add(-time.Hour),
		}

		res, ok := resolver.isCached(openfgav1.ConsistencyPreference_MINIMIZE_LATENCY, "test-key")
		require.False(t, ok)
		require.Nil(t, res)
	})

	t.Run("returns_false_when_cache_entry_older_than_invalidation_time", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		invalidationTime := time.Now()
		cacheEntry := &ResponseCacheEntry{
			Res:          &Response{Allowed: true},
			LastModified: invalidationTime.Add(-time.Hour),
		}

		mockCache.EXPECT().Get("test-key").Return(cacheEntry).Times(1)

		resolver := &Resolver{
			cache:                     mockCache,
			lastCacheInvalidationTime: invalidationTime,
		}

		res, ok := resolver.isCached(openfgav1.ConsistencyPreference_MINIMIZE_LATENCY, "test-key")
		require.False(t, ok)
		require.Nil(t, res)
	})

	t.Run("returns_true_when_valid_cache_entry_found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		invalidationTime := time.Now().Add(-time.Hour)
		expectedResponse := &Response{Allowed: true}
		cacheEntry := &ResponseCacheEntry{
			Res:          expectedResponse,
			LastModified: time.Now(),
		}

		mockCache.EXPECT().Get("test-key").Return(cacheEntry).Times(1)

		resolver := &Resolver{
			cache:                     mockCache,
			lastCacheInvalidationTime: invalidationTime,
		}

		res, ok := resolver.isCached(openfgav1.ConsistencyPreference_MINIMIZE_LATENCY, "test-key")
		require.True(t, ok)
		require.Equal(t, expectedResponse, res)
	})

	t.Run("returns_true_when_cache_modified_exactly_at_invalidation_time", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		invalidationTime := time.Now()
		expectedResponse := &Response{Allowed: false}
		cacheEntry := &ResponseCacheEntry{
			Res:          expectedResponse,
			LastModified: invalidationTime.Add(time.Nanosecond),
		}

		mockCache.EXPECT().Get("test-key").Return(cacheEntry).Times(1)

		resolver := &Resolver{
			cache:                     mockCache,
			lastCacheInvalidationTime: invalidationTime,
		}

		res, ok := resolver.isCached(openfgav1.ConsistencyPreference_MINIMIZE_LATENCY, "test-key")
		require.True(t, ok)
		require.Equal(t, expectedResponse, res)
	})

	t.Run("respects_unspecified_consistency_preference", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		expectedResponse := &Response{Allowed: true}
		cacheEntry := &ResponseCacheEntry{
			Res:          expectedResponse,
			LastModified: time.Now(),
		}

		mockCache.EXPECT().Get("test-key").Return(cacheEntry).Times(1)

		resolver := &Resolver{
			cache:                     mockCache,
			lastCacheInvalidationTime: time.Now().Add(-time.Hour),
		}

		res, ok := resolver.isCached(openfgav1.ConsistencyPreference_UNSPECIFIED, "test-key")
		require.True(t, ok)
		require.Equal(t, expectedResponse, res)
	})
}

func TestSpecificType(t *testing.T) {
	t.Run("returns_true_when_direct_tuple_exists", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type document
              relations
                define viewer: [user]
        `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		}

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			gomock.Any(),
		).Return(expectedTuple, nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_false_when_tuple_not_found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
	               model
	                 schema 1.1
	               type user
	               type document
	                 relations
	                   define viewer: [user]
	           `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			gomock.Any(),
		).Return(nil, storage.ErrNotFound).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_error_when_datastore_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
	               model
	                 schema 1.1
	               type user
	               type document
	                 relations
	                   define viewer: [user]
	           `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expectedErr := errors.New("database error")
		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expectedErr).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(context.Background(), req, edges[0])
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, res)
	})

	t.Run("respects_consistency_preference", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
	               model
	                 schema 1.1
	               type user
	               type document
	                 relations
	                   define viewer: [user]
	           `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		}

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			storage.ReadUserTupleOptions{
				Consistency: storage.ConsistencyOptions{
					Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
				},
			},
		).Return(expectedTuple, nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Consistency:          openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("handles_context_cancellation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
	               model
	                 schema 1.1
	               type user
	               type document
	                 relations
	                   define viewer: [user]
	           `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, context.Canceled).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(ctx, req, edges[0])
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, res)
	})

	t.Run("returns_false_when_condition_not_met", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
	               model
	                 schema 1.1
	               type user
	               type document
	                 relations
	                   define viewer: [user with non_expired]

	               condition non_expired(current_time: timestamp, expiration: timestamp) {
	                 current_time < expiration
	               }
	           `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expiredTime := time.Now().Add(-24 * time.Hour)
		expectedTuple := tuple.NewTupleKeyWithCondition(
			"document:1",
			"viewer",
			"user:maria",
			"non_expired",
			testutils.MustNewStruct(t, map[string]interface{}{"expiration": expiredTime.Format(time.RFC3339)}))

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(&openfgav1.Tuple{Key: expectedTuple}, nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"current_time": time.Now().Format(time.RFC3339),
			}),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_true_when_condition_met", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
	               model
	                 schema 1.1
	               type user
	               type document
	                 relations
	                   define viewer: [user with non_expired]

	               condition non_expired(current_time: timestamp, expiration: timestamp) {
	                 current_time < expiration
	               }
	           `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		futureTime := time.Now().Add(24 * time.Hour)

		expectedTuple := tuple.NewTupleKeyWithCondition(
			"document:1",
			"viewer",
			"user:maria",
			"non_expired",
			testutils.MustNewStruct(t, map[string]interface{}{"expiration": futureTime.Format(time.RFC3339)}))

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(&openfgav1.Tuple{Key: expectedTuple}, nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"current_time": time.Now().Format(time.RFC3339),
			}),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_error_when_condition_evaluation_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
	               model
	                 schema 1.1
	               type user
	               type document
	                 relations
	                   define viewer: [user with non_expired]

	               condition non_expired(current_time: timestamp, expiration: timestamp) {
	                 current_time < expiration
	               }
	           `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expectedTuple := tuple.NewTupleKeyWithCondition(
			"document:1",
			"viewer",
			"user:maria",
			"non_expired",
			testutils.MustNewStruct(t, map[string]interface{}{"expiration": "invalid-timestamp"}))

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(&openfgav1.Tuple{Key: expectedTuple}, nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"current_time": time.Now().Format(time.RFC3339),
			}),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(context.Background(), req, edges[0])
		require.Error(t, err)
		require.Nil(t, res)
	})

}

func TestSpecificTypeWildcard(t *testing.T) {
	t.Run("returns_true_when_wildcard_tuple_exists", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user:*]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "user:*"),
		}

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			storage.ReadUsersetTuplesFilter{
				Object:                      "document:1",
				Relation:                    "viewer",
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{WildcardRelationReference("user")},
				Conditions:                  []string{authzGraph.NoCond},
			},
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{expectedTuple}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_false_when_wildcard_tuple_not_found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user:*]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			storage.ReadUsersetTuplesFilter{
				Object:                      "document:1",
				Relation:                    "viewer",
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{WildcardRelationReference("user")},
				Conditions:                  []string{authzGraph.NoCond},
			},
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_error_when_datastore_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user:*]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expectedErr := errors.New("database error")
		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expectedErr).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(context.Background(), req, edges[0])
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, res)
	})

	t.Run("respects_consistency_preference", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user:*]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "user:*"),
		}

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			gomock.Any(),
			storage.ReadUsersetTuplesOptions{
				Consistency: storage.ConsistencyOptions{
					Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
				},
			},
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{expectedTuple}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Consistency:          openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("handles_context_cancellation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user:*]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, context.Canceled).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(ctx, req, edges[0])
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, res)
	})

	t.Run("returns_false_when_condition_not_met", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user:* with non_expired]

   condition non_expired(current_time: timestamp, expiration: timestamp) {
    current_time < expiration
   }
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expiredTime := time.Now().Add(-24 * time.Hour)

		expectedTuple := tuple.NewTupleKeyWithCondition(
			"document:1",
			"viewer",
			"user:*",
			"non_expired",
			testutils.MustNewStruct(t, map[string]interface{}{"expiration": expiredTime.Format(time.RFC3339)}))

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			storage.ReadUsersetTuplesFilter{
				Object:                      "document:1",
				Relation:                    "viewer",
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{WildcardRelationReference("user")},
				Conditions:                  []string{"non_expired"},
			},
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: expectedTuple}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"current_time": time.Now().Format(time.RFC3339),
			}),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_true_when_condition_met", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user:* with non_expired]

   condition non_expired(current_time: timestamp, expiration: timestamp) {
    current_time < expiration
   }
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		futureTime := time.Now().Add(24 * time.Hour)
		expectedTuple := &openfgav1.Tuple{
			Key: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:*",
				Condition: &openfgav1.RelationshipCondition{
					Name: "non_expired",
					Context: testutils.MustNewStruct(t, map[string]interface{}{
						"expiration": futureTime.Format(time.RFC3339),
					}),
				},
			},
		}

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			storage.ReadUsersetTuplesFilter{
				Object:                      "document:1",
				Relation:                    "viewer",
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{WildcardRelationReference("user")},
				Conditions:                  []string{"non_expired"},
			},
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{expectedTuple}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"current_time": time.Now().Format(time.RFC3339),
			}),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_error_when_iterator_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define viewer: [user:*]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		expectedErr := errors.New("iterator error")
		mockIter := mocks.NewMockIterator[*openfgav1.Tuple](ctrl)
		mockIter.EXPECT().Next(gomock.Any()).Return(nil, expectedErr).Times(1)
		mockIter.EXPECT().Stop().Times(1)

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(mockIter, nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(context.Background(), req, edges[0])
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, res)
	})
}

func TestSpecificTypeAndRelation(t *testing.T) {
	t.Run("returns_true_when_related_object_has_permission", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)
		mockSelector := mocks.NewMockSelector(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define viewer: [document#owner]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			storage.ReadUsersetTuplesFilter{
				Object:   "document:1",
				Relation: "viewer",
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
					Type:               "document",
					RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "owner"},
				}},
				Conditions: []string{authzGraph.NoCond},
			},
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "viewer", "document:2#owner")}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)

		mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
			tk := req.GetTupleKey()
			return tk.GetObject() == "document:2" && tk.GetRelation() == "owner" && tk.GetUser() == "user:maria"
		}), gomock.Any(), nil).Return(&Response{Allowed: true}, nil).Times(1)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeAndRelation(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_false_when_related_object_has_no_permission", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)
		mockSelector := mocks.NewMockSelector(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define viewer: [document#owner]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "viewer", "document:2#owner")}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)

		mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Any(), gomock.Any(), nil).Return(&Response{Allowed: false}, nil).Times(1)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeAndRelation(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_false_when_no_related_tuples_found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)
		mockSelector := mocks.NewMockSelector(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define viewer: [document#owner]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeAndRelation(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_error_when_datastore_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)
		mockSelector := mocks.NewMockSelector(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define viewer: [document#owner]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()

		expectedErr := errors.New("database error")
		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expectedErr).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeAndRelation(context.Background(), req, edges[0], nil)
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, res)
	})

	t.Run("handles_context_cancellation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)
		mockSelector := mocks.NewMockSelector(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define viewer: [document#owner]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, context.Canceled).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeAndRelation(ctx, req, edges[0], nil)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, res)
	})

	t.Run("respects_consistency_preference", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)
		mockSelector := mocks.NewMockSelector(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define viewer: [document#owner]
  `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			gomock.Any(),
			storage.ReadUsersetTuplesOptions{
				Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY},
			},
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "viewer", "document:2#owner")}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Consistency:          openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)

		mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Any(), gomock.Any(), nil).Return(&Response{Allowed: true}, nil).Times(1)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeAndRelation(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
	t.Run("returns_false_when_condition_not_met", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)
		mockSelector := mocks.NewMockSelector(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
  model
   schema 1.1
  type user
  type document
   relations
    define owner: [user]
    define viewer: [document#owner with validTime]
  condition validTime(current_time: timestamp, expiration: timestamp) {
   current_time < expiration
  }
 `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		expiredTime := time.Now().Add(-24 * time.Hour)
		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			storage.ReadUsersetTuplesFilter{
				Object:   "document:1",
				Relation: "viewer",
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
					Type:               "document",
					RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "owner"},
				}},
				Conditions: []string{"validTime"},
			},
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{
			Key: tuple.NewTupleKeyWithCondition(
				"document:1",
				"viewer",
				"document:2#owner",
				"validTime",
				testutils.MustNewStruct(t, map[string]interface{}{"expiration": expiredTime.Format(time.RFC3339)})),
		}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"current_time": time.Now().Format(time.RFC3339),
			}),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeAndRelation(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_true_when_condition_met", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)
		mockSelector := mocks.NewMockSelector(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
  model
   schema 1.1
  type user
  type document
   relations
    define owner: [user]
    define viewer: [document#owner with validTime]
  condition validTime(current_time: timestamp, expiration: timestamp) {
   current_time < expiration
  }
 `)

		mg, err := NewAuthorizationModelGraph(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		futureTime := time.Now().Add(24 * time.Hour)
		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			storage.ReadUsersetTuplesFilter{
				Object:   "document:1",
				Relation: "viewer",
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
					Type:               "document",
					RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "owner"},
				}},
				Conditions: []string{"validTime"},
			},
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{
			Key: tuple.NewTupleKeyWithCondition(
				"document:1",
				"viewer",
				"document:2#owner",
				"validTime",
				testutils.MustNewStruct(t, map[string]interface{}{"expiration": futureTime.Format(time.RFC3339)})),
		}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:              storeID,
			AuthorizationModelID: mg.GetModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"current_time": time.Now().Format(time.RFC3339),
			}),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)

		mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Any(), gomock.Any(), nil).Return(&Response{Allowed: true}, nil).Times(1)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeAndRelation(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

}
