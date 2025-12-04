package check

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/modelgraph"
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

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		// First edge returns true - should short circuit
		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, filter storage.ReadUserTupleFilter, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				if filter.Relation == "admin" {
					return nil, storage.ErrNotFound
				}
				return &openfgav1.Tuple{Key: tuple.NewTupleKey("group:1", "member", "user:maria")}, nil
			}).MaxTimes(2)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user", false)
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user", false)
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		mockCache.EXPECT().Get(req.GetCacheKey()).Return(cachedEntry).Times(1)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user", false)
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user", false)
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user", false)
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

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, string, storage.ReadUserTupleFilter, storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("group:1", "member", "user:maria"),
		})
		require.NoError(t, err)

		node, ok := mg.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := mg.FlattenNode(node, "user", false)
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
              relations
                define self: [user]
        `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("user:1", "self", "user:maria"),
		})
		require.NoError(t, err)

		res, err := resolver.ResolveUnionEdges(context.Background(), req, []*authzGraph.WeightedAuthorizationModelEdge{}, nil)
		require.NoError(t, err)
		require.False(t, res.Allowed)
	})
}

func TestResolveIntersection(t *testing.T) {
	t.Run("returns_true_when_all_edges_true", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define editor: [user]
     define viewer: owner and editor
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(&openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "editor", "user:maria"),
		}, nil).Times(1)
		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(&openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "owner", "user:maria"),
		}, nil).Times(1)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		node, ok := mg.GetNodeByID("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveIntersection(context.Background(), req, node)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_false_when_any_edge_false", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define editor: [user]
     define viewer: owner and editor
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(&openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "editor", "user:maria"),
		}, nil).Times(1)
		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, storage.ErrNotFound).Times(1)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		node, ok := mg.GetNodeByID("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveIntersection(context.Background(), req, node)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_error_when_edge_resolution_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define editor: [user]
     define viewer: owner and editor
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		expectedErr := errors.New("database error")

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expectedErr).Times(2)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		node, ok := mg.GetNodeByID("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveIntersection(context.Background(), req, node)
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

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define editor: [user]
     define viewer: owner and editor
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, ctx.Err()).Times(2)

		node, ok := mg.GetNodeByID("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveIntersection(ctx, req, node)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, res)
	})
}

func TestResolveExclusion(t *testing.T) {
	t.Run("returns_true_when_base_true_and_subtract_false", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define editor: [user]
     define banned: [user]
     define viewer: editor but not banned
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(_ context.Context, _ string, filter storage.ReadUserTupleFilter, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
			if filter.Relation == "editor" {
				return &openfgav1.Tuple{Key: tuple.NewTupleKey("document:1", "editor", "user:maria")}, nil
			}
			return nil, storage.ErrNotFound
		}).Times(2)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveExclusion(context.Background(), req, edges[0].GetTo())
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_false_when_base_false", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define editor: [user]
     define banned: [user]
     define viewer: editor but not banned
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(nil, storage.ErrNotFound).Times(2)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveExclusion(context.Background(), req, edges[0].GetTo())
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_false_when_base_true_and_subtract_true", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define editor: [user]
     define banned: [user]
     define viewer: editor but not banned
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(_ context.Context, _ string, filter storage.ReadUserTupleFilter, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
			if filter.Relation == "editor" {
				return &openfgav1.Tuple{Key: tuple.NewTupleKey("document:1", "editor", "user:maria")}, nil
			}
			return &openfgav1.Tuple{Key: tuple.NewTupleKey("document:1", "banned", "user:maria")}, nil
		}).Times(2)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveExclusion(context.Background(), req, edges[0].GetTo())
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_error_when_base_edge_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define editor: [user]
     define banned: [user]
     define viewer: editor but not banned
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		expectedErr := errors.New("database error")
		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(_ context.Context, _ string, filter storage.ReadUserTupleFilter, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
			if filter.Relation == "editor" {
				return nil, expectedErr
			}
			return nil, storage.ErrNotFound
		}).Times(2)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveExclusion(context.Background(), req, edges[0].GetTo())
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, res)
	})

	t.Run("returns_error_when_subtract_edge_fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define editor: [user]
     define banned: [user]
     define viewer: editor but not banned
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		expectedErr := errors.New("database error")
		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(_ context.Context, _ string, filter storage.ReadUserTupleFilter, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
			if filter.Relation == "banned" {
				return nil, expectedErr
			}
			return &openfgav1.Tuple{Key: tuple.NewTupleKey("document:1", "banned", "user:maria")}, nil
		}).Times(2)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveExclusion(context.Background(), req, edges[0].GetTo())
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

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define editor: [user]
     define banned: [user]
     define viewer: editor but not banned
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(_ context.Context, _ string, filter storage.ReadUserTupleFilter, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
			if filter.Relation == "banned" {
				return nil, storage.ErrNotFound
			}
			return &openfgav1.Tuple{Key: tuple.NewTupleKey("document:1", "banned", "user:maria")}, nil
		}).Times(2)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveExclusion(ctx, req, edges[0].GetTo())
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, res)
	})

	t.Run("skips_subtract_edge_when_no_weight_for_user_type", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)
		mockPlanner := mocks.NewMockManager(ctrl)

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type usergroup
   type document
    relations
     define editor: [user]
     define banned: [usergroup]
     define viewer: editor but not banned
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(&openfgav1.Tuple{Key: tuple.NewTupleKey("document:1", "editor", "user:maria")}, nil).Times(1)

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ResolveExclusion(context.Background(), req, edges[0].GetTo())
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
}

func TestResolveCheckUsersetRequest(t *testing.T) {
	t.Run("returns_true_when_related_object_relation_has_permission", func(t *testing.T) {
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
     define owner: [user]
     define viewer: [document#owner]
  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			storage.ReadUserTupleFilter{
				Object:   expectedTuple.GetKey().GetObject(),
				Relation: expectedTuple.GetKey().GetRelation(),
				User:     expectedTuple.GetKey().GetUser(),
			},
			gomock.Any(),
		).Return(expectedTuple, nil).Times(1)
		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
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
     define owner: [user]
     define viewer: [document#owner with tag]
	condition tag(tag: string) {
	                 tag == "valid"
	               }
  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			storage.ReadUserTupleFilter{
				Object:   expectedTuple.GetKey().GetObject(),
				Relation: expectedTuple.GetKey().GetRelation(),
				User:     expectedTuple.GetKey().GetUser(),
			},
			gomock.Any(),
		).Return(expectedTuple, nil).Times(1)
		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_true_when_union_object_relation_has_permission", func(t *testing.T) {
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
			     define owner: [user]
			     define viewer: [document#owner] or member
				 define member: [user, document#owner]

			  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "member", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(2). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				// Manually check the relation and return the right data
				switch filter.Relation {
				case "member":
					return expectedTuple, nil
				default:
					return nil, storage.ErrNotFound
				}
			})

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_true_when_partial_union_object_relation_has_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: [document#owner] or member or reviewer
			 define member: [user]
			 define reviewer: [user]

		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			storage.ReadUserTupleFilter{
				Object:   expectedTuple.GetKey().GetObject(),
				Relation: expectedTuple.GetKey().GetRelation(),
				User:     expectedTuple.GetKey().GetUser(),
			},
			gomock.Any(),
		).Return(expectedTuple, nil).Times(1)
		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_true_when_intersection_object_relation_has_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: [document#owner] and member
			 define member: [user, document#owner]

		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		}
		expected2Tuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "member", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(2). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				// Manually check the relation and return the right data
				switch filter.Relation {
				case "viewer":
					return expectedTuple, nil
				case "member":
					return expected2Tuple, nil
				default:
					return nil, storage.ErrNotFound
				}
			})

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_true_when_exclusion_object_relation_has_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: [document#owner] but not member
			 define member: [user, document#owner]

		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(2). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				// Manually check the relation and return the right data
				switch filter.Relation {
				case "viewer":
					return expectedTuple, nil
				default:
					return nil, storage.ErrNotFound
				}
			})

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_error_when_exclusion_base_has_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: [document#owner] but not member
			 define member: [user]

		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			storage.ReadUserTupleFilter{
				Object:   expectedTuple.GetKey().GetObject(),
				Relation: expectedTuple.GetKey().GetRelation(),
				User:     expectedTuple.GetKey().GetUser(),
			},
			gomock.Any(),
		).Return(expectedTuple, nil).Times(1)
		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		_, err = resolver.ResolveCheck(context.Background(), req)
		require.Error(t, err)
		require.Equal(t, ErrUsersetInvalidRequest, err)
	})

	t.Run("returns_false_when_no_path_leads_to_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: owner but not member
			 define member: [user]

		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_true_when_weight2_for_userset_has_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: [document#owner] or reviewer
			 define reviewer: [user]
			 define member: [user, document#viewer]

		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "member", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			storage.ReadUsersetTuplesFilter{
				Object:   "document:1",
				Relation: "member",
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{{
					Type:               "document",
					RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "viewer"},
				}},
				Conditions: []string{authzGraph.NoCond},
			},
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "member", "document:3#viewer")}}), nil).Times(1)

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			storage.ReadUserTupleFilter{
				Object:   "document:3",
				Relation: "viewer",
				User:     "document:2#owner",
			},
			gomock.Any(),
		).Return(expectedTuple, nil).Times(1)
		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "member", "document:2#owner"),
		})
		require.NoError(t, err)

		// mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_true_when_weight3_for_logicaluserset_has_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: [user, document#owner, document#principal] or reviewer
			 define reviewer: [user]
			 define principal: [user, document#owner]
			 define member: [user, document#viewer]

		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:4", "principal", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(2). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:1":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "member", "document:3#viewer")}}), nil
				case "document:3":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:3", "viewer", "document:4#principal")}}), nil
				default:
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
				}
			})

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(2). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:4":
					// Check the relation and return the right data
					if filter.Relation == "principal" {
						return expectedTuple, nil
					}
					return nil, storage.ErrNotFound
				default:
					return nil, storage.ErrNotFound
				}
			})

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "member", "document:2#owner"),
		})
		require.NoError(t, err)

		// mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_true_when_recursive_userset_has_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: [user, document#owner, document#viewer] or reviewer
			 define reviewer: [user]
		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:2", "viewer", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:1":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "viewer", "document:3#viewer")}}), nil
				case "document:3":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:3", "viewer", "document:2#viewer")}}), nil
				default:
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
				}
			})

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:2":
					return expectedTuple, nil
				default:
					return nil, storage.ErrNotFound
				}
			})

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		// mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
	t.Run("returns_false_when_recursiveCycle_userset_without_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: [user, document#owner, document#viewer] or reviewer
			 define reviewer: [user]
		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			MaxTimes(2). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:1":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "viewer", "document:3#viewer")}}), nil
				case "document:3":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:3", "viewer", "document:1#viewer")}}), nil
				default:
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
				}
			})

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				// Manually check the relation and return the right data
				return nil, storage.ErrNotFound
			})

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		// mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})
	t.Run("returns_true_when_tuplecycle_userset_has_permission", func(t *testing.T) {
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
		     define owner: [user]
		     define viewer: [user, document#owner, document#member] or reviewer
			 define member: [user, document#viewer]
			 define reviewer: [user]
		  `)
		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:3", "viewer", "document:2#owner"),
		}

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:1":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "viewer", "document:2#member")}}), nil
				case "document:2":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:2", "member", "document:3#viewer")}}), nil
				default:
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
				}
			})

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:3":
					return expectedTuple, nil
				default:
					return nil, storage.ErrNotFound
				}
			})

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
		})
		require.NoError(t, err)

		// mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
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

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		}

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			storage.ReadUserTupleFilter{
				Object:   expectedTuple.GetKey().GetObject(),
				Relation: expectedTuple.GetKey().GetRelation(),
				User:     expectedTuple.GetKey().GetUser(),
			},
			gomock.Any(),
		).Return(expectedTuple, nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			storage.ReadUserTupleFilter{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:maria",
			},
			gomock.Any(),
		).Return(nil, storage.ErrNotFound).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedTuple := &openfgav1.Tuple{
			Key: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		}

		mockDatastore.EXPECT().ReadUserTuple(
			gomock.Any(),
			storeID,
			storage.ReadUserTupleFilter{
				Object:   expectedTuple.GetKey().GetObject(),
				Relation: expectedTuple.GetKey().GetRelation(),
				User:     expectedTuple.GetKey().GetUser(),
			},
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
			StoreID:     storeID,
			Model:       mg,
			TupleKey:    tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

	t.Run("returns_true_when_contextual_tuple_exists", func(t *testing.T) {
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

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		// No database call expected since contextual tuple satisfies the check
		resolver := New(Config{
			Model:     mg,
			Datastore: mockDatastore,
			Cache:     mockCache,
		})

		contextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:anne"),
			tuple.NewTupleKey("document:2", "viewer", "user:anne"),
			tuple.NewTupleKey("document:3", "viewer", "user:anne"),
		}

		req, err := NewRequest(RequestParams{
			StoreID:          storeID,
			Model:            mg,
			TupleKey:         tuple.NewTupleKey("document:1", "viewer", "user:anne"),
			ContextualTuples: contextualTuples,
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_false_when_contextual_tuple_condition_not_met", func(t *testing.T) {
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
    define viewer: [user with validTime]
  condition validTime(current_time: timestamp, expiration: timestamp) {
   current_time < expiration
  }
 `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:     mg,
			Datastore: mockDatastore,
			Cache:     mockCache,
		})

		expiredTime := time.Now().Add(-24 * time.Hour)
		contextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:anne", "validTime", &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"expiration": structpb.NewStringValue(expiredTime.Format(time.RFC3339)),
				},
			}),
		}

		req, err := NewRequest(RequestParams{
			StoreID:          storeID,
			Model:            mg,
			TupleKey:         tuple.NewTupleKey("document:1", "viewer", "user:anne"),
			ContextualTuples: contextualTuples,
			Context: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"current_time": structpb.NewStringValue(time.Now().Format(time.RFC3339)),
				},
			},
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificType(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
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

		mg, err := modelgraph.New(model)
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
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{modelgraph.WildcardRelationReference("user")},
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockDatastore.EXPECT().ReadUsersetTuples(
			gomock.Any(),
			storeID,
			storage.ReadUsersetTuplesFilter{
				Object:                      "document:1",
				Relation:                    "viewer",
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{modelgraph.WildcardRelationReference("user")},
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:     storeID,
			Model:       mg,
			TupleKey:    tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{modelgraph.WildcardRelationReference("user")},
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{modelgraph.WildcardRelationReference("user")},
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(context.Background(), req, edges[0])
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, res)
	})

	t.Run("returns_true_when_wildcard_contextual_tuple_exists", func(t *testing.T) {
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

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		resolver := New(Config{
			Model:     mg,
			Datastore: mockDatastore,
			Cache:     mockCache,
		})

		contextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:*"),
		}

		req, err := NewRequest(RequestParams{
			StoreID:          storeID,
			Model:            mg,
			TupleKey:         tuple.NewTupleKey("document:1", "viewer", "user:anne"),
			ContextualTuples: contextualTuples,
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.specificTypeWildcard(context.Background(), req, edges[0])
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:     storeID,
			Model:       mg,
			TupleKey:    tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		mg, err := modelgraph.New(model)
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

	t.Run("returns_true_when_contextual_tuple_provides_related_object", func(t *testing.T) {
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

		mg, err := modelgraph.New(model)
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
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil).MaxTimes(1)

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, tk *openfgav1.TupleKey, opts storage.ReadUserTupleOptions) (*openfgav1.TupleKey, error) {
				return nil, storage.ErrNotFound
			})

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		contextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "document:2#owner"),
			tuple.NewTupleKey("document:2", "owner", "user:anne"),
		}

		req, err := NewRequest(RequestParams{
			StoreID:          storeID,
			Model:            mg,
			TupleKey:         tuple.NewTupleKey("document:1", "viewer", "user:anne"),
			ContextualTuples: contextualTuples,
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)

		res, err := resolver.specificTypeAndRelation(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
}

func TestTTU(t *testing.T) {
	t.Run("returns_true_when_user_has_computed_relation_on_related_object", func(t *testing.T) {
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
     define parent: [document]
     define viewer: owner from parent
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().Read(
			gomock.Any(),
			storeID,
			storage.ReadFilter{
				Object:     "document:1",
				Relation:   "parent",
				User:       "document:",
				Conditions: []string{authzGraph.NoCond},
			},
			storage.ReadOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_UNSPECIFIED}},
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{
			Key: tuple.NewTupleKey("document:1", "parent", "document:2"),
		}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Cond(func(req *Request) bool {
			return req.GetTupleKey().GetObject() == "document:2" &&
				req.GetTupleKey().GetRelation() == "owner" &&
				req.GetTupleKey().GetUser() == "user:maria"
		}), gomock.Any(), nil).Return(&Response{Allowed: true}, nil).Times(1)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ttu(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_false_when_user_has_no_computed_relation_on_related_object", func(t *testing.T) {
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
     define parent: [document]
     define viewer: owner from parent
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().Read(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{
			Key: tuple.NewTupleKey("document:1", "parent", "document:2"),
		}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Any(), gomock.Any(), nil).Return(&Response{Allowed: false}, nil).Times(1)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ttu(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_false_when_no_tupleset_tuples_found", func(t *testing.T) {
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
     define parent: [document]
     define viewer: owner from parent
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().Read(
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ttu(context.Background(), req, edges[0], nil)
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

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define parent: [document]
     define viewer: owner from parent
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		expectedErr := errors.New("database error")
		mockDatastore.EXPECT().Read(
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ttu(context.Background(), req, edges[0], nil)
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

		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
     define owner: [user]
     define parent: [document]
     define viewer: owner from parent
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		mockDatastore.EXPECT().Read(
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
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ttu(ctx, req, edges[0], nil)
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
     define parent: [document]
     define viewer: owner from parent
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().Read(
			gomock.Any(),
			storeID,
			gomock.Any(),
			storage.ReadOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY}},
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{
			Key: tuple.NewTupleKey("document:1", "parent", "document:2"),
		}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:     storeID,
			Model:       mg,
			TupleKey:    tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		mockResolver.EXPECT().ResolveUnion(gomock.Any(), gomock.Any(), gomock.Any(), nil).Return(&Response{Allowed: true}, nil).Times(1)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ttu(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})

	t.Run("returns_false_when_condition_not_met_on_tupleset", func(t *testing.T) {
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
     define parent: [document with non_expired]
     define viewer: owner from parent
   condition non_expired(current_time: timestamp, expiration: timestamp) {
    current_time < expiration
   }
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		expiredTime := time.Now().Add(-24 * time.Hour)
		mockDatastore.EXPECT().Read(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{
			Key: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "parent",
				User:     "document:2",
				Condition: &openfgav1.RelationshipCondition{
					Name: "non_expired",
					Context: testutils.MustNewStruct(t, map[string]interface{}{
						"expiration": expiredTime.Format(time.RFC3339),
					}),
				},
			},
		}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"current_time": time.Now().Format(time.RFC3339),
			}),
		})
		require.NoError(t, err)

		mockResolver := NewMockCheckResolver(ctrl)
		resolver.strategies[DefaultStrategyName] = NewDefault(mg, mockResolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ttu(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})

	t.Run("returns_true_when_condition_met_on_tupleset", func(t *testing.T) {
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
     define parent: [document with non_expired]
     define viewer: owner from parent
   condition non_expired(current_time: timestamp, expiration: timestamp) {
    current_time < expiration
   }
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		futureTime := time.Now().Add(24 * time.Hour)
		mockDatastore.EXPECT().Read(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{{
			Key: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "parent",
				User:     "document:2",
				Condition: &openfgav1.RelationshipCondition{
					Name: "non_expired",
					Context: testutils.MustNewStruct(t, map[string]interface{}{
						"expiration": futureTime.Format(time.RFC3339),
					}),
				},
			},
		}}), nil).Times(1)

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:maria"),
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

		res, err := resolver.ttu(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
	t.Run("returns_true_when_contextual_tupleset_provides_relationship", func(t *testing.T) {
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
  type folder
   relations
    define viewer: [user]
  type document
   relations
    define parent: [folder]
    define viewer: viewer from parent
 `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().Read(
			gomock.Any(),
			storeID,
			gomock.Any(),
			gomock.Any(),
		).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil).AnyTimes()

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, tk *openfgav1.TupleKey, opts storage.ReadUserTupleOptions) (*openfgav1.TupleKey, error) {
				return nil, storage.ErrNotFound
			})

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		contextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "parent", "folder:shared"),
			tuple.NewTupleKey("folder:shared", "viewer", "user:anne"),
		}

		req, err := NewRequest(RequestParams{
			StoreID:          storeID,
			Model:            mg,
			TupleKey:         tuple.NewTupleKey("document:1", "viewer", "user:anne"),
			ContextualTuples: contextualTuples,
		})
		require.NoError(t, err)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)

		edges, ok := mg.GetEdgesFromNodeId("document#viewer")
		require.True(t, ok)

		res, err := resolver.ttu(context.Background(), req, edges[0], nil)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
}

func TestResolveRecursiveCheck(t *testing.T) {
	t.Run("returns_false_ttu_no_assignation", func(t *testing.T) {
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
       define viewer: [user] or viewer from parent
	   define parent: [document]
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.TupleKey, error) {
				return nil, storage.ErrNotFound
			})

		mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes().
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadFilter, opts storage.ReadOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:1":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "parent", "document:x")}}), nil
				case "document:2":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:2", "parent", "document:1")}}), nil
				case "document:3":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:3", "parent", "document:2")}}), nil
				case "document:4":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:4", "parent", "document:3")}}), nil
				default:
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
				}
			})

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:4", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})
	t.Run("returns_true_ttu_mixed_assignation", func(t *testing.T) {
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
       define viewer: [group] or viewer from parent
	   define parent: [document, group]
   type group
    relations
	   define viewer: member
	   define member: reader or public
	   define public: [user:*]
	   define reader: [user]
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(weight2Plan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.TupleKey, error) {
				if filter.Object == "group:g1" && filter.Relation == "reader" && filter.User == "user:u1" {
					return tuple.NewTupleKey("group:g1", "reader", "user:u1"), nil
				}

				return nil, storage.ErrNotFound
			})

		readStartingWithUserReader := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:g1", "reader", "user:1"),
			},
		}
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Relation {
				case "reader":
					return storage.NewStaticTupleIterator(readStartingWithUserReader), nil
				default:
					return nil, storage.ErrNotFound
				}
			})

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
			})

		mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes().
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadFilter, opts storage.ReadOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:1":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "parent", "group:g1")}}), nil
				case "document:2":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:2", "parent", "document:1")}}), nil
				case "document:3":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:3", "parent", "document:2")}}), nil
				case "document:4":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:4", "parent", "document:3")}}), nil
				default:
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
				}
			})

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:4", "viewer", "user:u1"),
		})
		require.NoError(t, err)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)
		resolver.strategies[WeightTwoStrategyName] = NewWeight2(mg, mockDatastore)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
	t.Run("returns_true_ttu_contextual_mixed_assignation", func(t *testing.T) {
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
       define viewer: [group] or viewer from parent
	   define parent: [document, group]
   type group
    relations
	   define viewer: member
	   define member: reader or public
	   define public: [user:*]
	   define reader: [user]
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(weight2Plan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.TupleKey, error) {
				if filter.Object == "group:g1" && filter.Relation == "reader" && filter.User == "user:u1" {
					return tuple.NewTupleKey("group:g1", "reader", "user:u1"), nil
				}

				return nil, storage.ErrNotFound
			})
		contextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:g1", "reader", "user:1"),
		}

		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
			})

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
			})

		mockDatastore.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes().
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadFilter, opts storage.ReadOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Object {
				case "document:1":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "parent", "group:g1")}}), nil
				case "document:2":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:2", "parent", "document:1")}}), nil
				case "document:3":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:3", "parent", "document:2")}}), nil
				case "document:4":
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:4", "parent", "document:3")}}), nil
				default:
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
				}
			})

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:          storeID,
			Model:            mg,
			TupleKey:         tuple.NewTupleKey("document:4", "viewer", "user:u1"),
			ContextualTuples: contextualTuples,
		})
		require.NoError(t, err)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)
		resolver.strategies[WeightTwoStrategyName] = NewWeight2(mg, mockDatastore)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
	t.Run("returns_false_userset_no_assignation", func(t *testing.T) {
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
       define viewer: [user, document#viewer]
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(DefaultPlan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.TupleKey, error) {
				return nil, storage.ErrNotFound
			})

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes().DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
			switch filter.Object {
			case "document:1":
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "viewer", "document:x#viewer")}}), nil
			case "document:2":
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:2", "viewer", "document:1#viewer")}}), nil
			case "document:3":
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:3", "viewer", "document:2#viewer")}}), nil
			case "document:4":
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:4", "viewer", "document:3#viewer")}}), nil
			default:
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
			}
		})

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:4", "viewer", "user:maria"),
		})
		require.NoError(t, err)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.False(t, res.GetAllowed())
	})
	t.Run("returns_true_userset_mixed_assignation", func(t *testing.T) {
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
       define viewer: [group, document#viewer, group#viewer]
   type group
    relations
	   define viewer: member
	   define member: reader or public
	   define public: [user:*]
	   define reader: [user]
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(weight2Plan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.TupleKey, error) {
				if filter.Object == "group:g1" && filter.Relation == "reader" && filter.User == "user:u1" {
					return tuple.NewTupleKey("group:g1", "reader", "user:u1"), nil
				}

				return nil, storage.ErrNotFound
			})

		readStartingWithUserReader := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("group:g1", "reader", "user:1"),
			},
		}
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.Relation {
				case "reader":
					return storage.NewStaticTupleIterator(readStartingWithUserReader), nil
				default:
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
				}
			})

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes().DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
			switch filter.Object {
			case "document:1":
				if filter.AllowedUserTypeRestrictions[0].GetType() == "group" {
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "viewer", "group:g1#viewer")}}), nil
				}
			case "document:2":
				if filter.AllowedUserTypeRestrictions[0].GetType() == "document" {
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:2", "viewer", "document:1#viewer")}}), nil
				}
			case "document:3":
				if filter.AllowedUserTypeRestrictions[0].GetType() == "document" {
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:3", "viewer", "document:2#viewer")}}), nil
				}
			case "document:4":
				if filter.AllowedUserTypeRestrictions[0].GetType() == "document" {
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:4", "viewer", "document:3#viewer")}}), nil
				}
			}
			return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
		})

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:4", "viewer", "user:u1"),
		})
		require.NoError(t, err)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)
		resolver.strategies[WeightTwoStrategyName] = NewWeight2(mg, mockDatastore)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
	t.Run("returns_true_userset_contextual_mixed_assignation", func(t *testing.T) {
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
       define viewer: [group, document#viewer, group#viewer]
   type group
    relations
	   define viewer: member
	   define member: reader or public
	   define public: [user:*]
	   define reader: [user]
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(weight2Plan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.TupleKey, error) {
				if filter.Object == "group:g1" && filter.Relation == "reader" && filter.User == "user:u1" {
					return tuple.NewTupleKey("group:g1", "reader", "user:u1"), nil
				}

				return nil, storage.ErrNotFound
			})

		contextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:g1", "reader", "user:u1"),
		}

		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
			})

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes().DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
			switch filter.Object {
			case "document:1":
				if filter.AllowedUserTypeRestrictions[0].GetType() == "group" {
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:1", "viewer", "group:g1#viewer")}}), nil
				}
			case "document:2":
				if filter.AllowedUserTypeRestrictions[0].GetType() == "document" {
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:2", "viewer", "document:1#viewer")}}), nil
				}
			case "document:3":
				if filter.AllowedUserTypeRestrictions[0].GetType() == "document" {
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:3", "viewer", "document:2#viewer")}}), nil
				}
			case "document:4":
				if filter.AllowedUserTypeRestrictions[0].GetType() == "document" {
					return storage.NewStaticTupleIterator([]*openfgav1.Tuple{{Key: tuple.NewTupleKey("document:4", "viewer", "document:3#viewer")}}), nil
				}
			}
			return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
		})

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:          storeID,
			Model:            mg,
			TupleKey:         tuple.NewTupleKey("document:4", "viewer", "user:u1"),
			ContextualTuples: contextualTuples,
		})
		require.NoError(t, err)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)
		resolver.strategies[WeightTwoStrategyName] = NewWeight2(mg, mockDatastore)

		res, err := resolver.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())
	})
}

func TestResolveCheck(t *testing.T) {
	t.Run("returns_true_for_mixed_assignation", func(t *testing.T) {
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
   type group
	relations
		define viewer: [user with xcond]
   type document
    relations
       define viewer: [group#viewer with ycond]
    condition xcond(x: string) {
  	x == '1'
	}
	 condition ycond(y: string) {
  	y == '1'
	}
  `)

		mg, err := modelgraph.New(model)
		require.NoError(t, err)

		mockPlanner.EXPECT().GetPlanSelector(gomock.Any()).Return(mockSelector).AnyTimes()
		mockSelector.EXPECT().Select(gomock.Any()).Return(weight2Plan).AnyTimes()
		mockSelector.EXPECT().UpdateStats(gomock.Any(), gomock.Any()).AnyTimes()

		mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

		mockDatastore.EXPECT().ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.TupleKey, error) {
				if filter.Object == "group:g1" && filter.Relation == "reader" && filter.User == "user:u1" {
					return tuple.NewTupleKey("group:g1", "reader", "user:u1"), nil
				}

				return nil, storage.ErrNotFound
			})

		readStartingWithUserReader := []*openfgav1.Tuple{
			{
				Key: &openfgav1.TupleKey{
					Object:   "group:g1",
					Relation: "viewer",
					User:     "user:u1",
					Condition: &openfgav1.RelationshipCondition{
						Name: "xcond",
						Context: testutils.MustNewStruct(t, map[string]interface{}{
							"x": "1",
						}),
					},
				},
			},
		}

		readTuples := []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKeyWithCondition(
					"document:d1",
					"viewer",
					"group:g1#viewer",
					"ycond",
					&structpb.Struct{}),
			},
		}
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadStartingWithUserFilter, opts storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				// Manually check the relation and return the right data
				switch filter.ObjectType {
				case "group":
					return storage.NewStaticTupleIterator(readStartingWithUserReader), nil
				default:
					return nil, storage.ErrNotFound
				}
			})

		mockDatastore.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
			AnyTimes(). // Allow any number of calls
			DoAndReturn(func(ctx context.Context, sID string, filter storage.ReadUsersetTuplesFilter, opts storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator(readTuples), nil
			})

		resolver := New(Config{
			Model:            mg,
			Datastore:        mockDatastore,
			Cache:            mockCache,
			Planner:          mockPlanner,
			ConcurrencyLimit: 10,
		})

		req, err := NewRequest(RequestParams{
			StoreID:  storeID,
			Model:    mg,
			TupleKey: tuple.NewTupleKey("document:d1", "viewer", "user:u1"),
		})
		require.NoError(t, err)

		resolver.strategies[DefaultStrategyName] = NewDefault(mg, resolver, 10)
		resolver.strategies[WeightTwoStrategyName] = NewWeight2(mg, mockDatastore)

		_, err = resolver.ResolveCheck(context.Background(), req)
		require.Error(t, err)
	})
}
