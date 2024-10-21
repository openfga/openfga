package commands

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/throttler/threshold"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestNewListObjectsQuery(t *testing.T) {
	t.Run("nil_datastore", func(t *testing.T) {
		checkResolver, checkResolverCloser := graph.NewOrderedCheckResolvers().Build()
		t.Cleanup(checkResolverCloser)
		q, err := NewListObjectsQuery(nil, checkResolver)
		require.Nil(t, q)
		require.Error(t, err)
	})

	t.Run("nil_checkResolver", func(t *testing.T) {
		q, err := NewListObjectsQuery(memory.New(), nil)
		require.Nil(t, q)
		require.Error(t, err)
	})

	t.Run("empty_typesystem_in_context", func(t *testing.T) {
		checkResolver := graph.NewLocalChecker()
		q, err := NewListObjectsQuery(memory.New(), checkResolver)
		require.NoError(t, err)

		_, err = q.Execute(context.Background(), &openfgav1.ListObjectsRequest{})
		require.ErrorContains(t, err, "typesystem missing in context")
	})
}

func TestListObjectsDispatchCount(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)
	ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	mockThrottler := mocks.NewMockThrottler(ctrl)
	tests := []struct {
		name                    string
		model                   string
		tuples                  []string
		objectType              string
		relation                string
		user                    string
		expectedDispatchCount   uint32
		expectedThrottlingValue int
	}{
		{
			name: "test_direct_relation",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define viewer: [user]
			`,
			tuples: []string{
				"folder:C#viewer@user:jon",
				"folder:B#viewer@user:jon",
				"folder:A#viewer@user:jon",
			},
			objectType:              "folder",
			relation:                "viewer",
			user:                    "user:jon",
			expectedDispatchCount:   3,
			expectedThrottlingValue: 0,
		},
		{
			name: "test_union_relation",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define editor: [user]
						define viewer: [user] or editor
			`,
			tuples: []string{
				"folder:C#editor@user:jon",
				"folder:B#viewer@user:jon",
				"folder:A#viewer@user:jon",
			},
			objectType:              "folder",
			relation:                "viewer",
			user:                    "user:jon",
			expectedDispatchCount:   4,
			expectedThrottlingValue: 1,
		},
		{
			name: "test_intersection_relation",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define editor: [user]
						define can_delete: [user] and editor
			`,
			tuples: []string{
				"folder:C#can_delete@user:jon",
				"folder:C#editor@user:jon",
			},
			objectType:              "folder",
			relation:                "can_delete",
			user:                    "user:jon",
			expectedDispatchCount:   1,
			expectedThrottlingValue: 0,
		},
		{
			name: "test_intersection_relation_check_dispatch",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user, group#member]

				type folder
					relations
						define editor: [group#member]
						define can_delete: [user] and editor
			`,
			tuples: []string{
				"folder:C#can_delete@user:jon",
				"folder:C#editor@group:fga#member",
				"group:fga#member@user:jon",
			},
			objectType:              "folder",
			relation:                "can_delete",
			user:                    "user:jon",
			expectedDispatchCount:   2,
			expectedThrottlingValue: 1,
		},
		{
			name: "no_tuples",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define editor: [user]
						define can_delete: [user] and editor
			`,
			tuples:                  []string{},
			objectType:              "folder",
			relation:                "can_delete",
			user:                    "user:jon",
			expectedDispatchCount:   0,
			expectedThrottlingValue: 0,
		},
		{
			name: "direct_userset_dispatch",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user, group#member]
			`,
			tuples: []string{
				"group:eng#member@group:fga#member",
				"group:fga#member@user:jon",
			},
			objectType:              "group",
			relation:                "member",
			user:                    "user:jon",
			expectedDispatchCount:   2,
			expectedThrottlingValue: 0,
		},
		{
			name: "computed_userset_dispatch",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define editor: [user]
						define viewer: editor
			`,
			tuples: []string{
				"document:1#editor@user:jon",
			},
			objectType:              "document",
			relation:                "viewer",
			user:                    "user:jon",
			expectedDispatchCount:   2,
			expectedThrottlingValue: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeID, model := storagetest.BootstrapFGAStore(t, ds, test.model, test.tuples)
			ts, err := typesystem.NewAndValidate(
				context.Background(),
				model,
			)
			require.NoError(t, err)
			ctx = typesystem.ContextWithTypesystem(ctx, ts)

			checker, checkResolverCloser := graph.NewOrderedCheckResolvers(
				graph.WithDispatchThrottlingCheckResolverOpts(true, []graph.DispatchThrottlingCheckResolverOpt{
					graph.WithDispatchThrottlingCheckResolverConfig(graph.DispatchThrottlingCheckResolverConfig{
						DefaultThreshold: 0,
						MaxThreshold:     0,
					}),
					graph.WithThrottler(mockThrottler),
				}...),
				graph.WithLocalCheckerOpts(graph.WithMaxConcurrentReads(1))).Build()
			t.Cleanup(checkResolverCloser)

			q, _ := NewListObjectsQuery(
				ds,
				checker,
				WithDispatchThrottlerConfig(threshold.Config{
					Throttler:    mockThrottler,
					Enabled:      true,
					Threshold:    3,
					MaxThreshold: 0,
				}),
			)
			mockThrottler.EXPECT().Throttle(gomock.Any()).Times(test.expectedThrottlingValue)
			mockThrottler.EXPECT().Close().Times(1) // LO closes throttler during server close call.

			resp, err := q.Execute(ctx, &openfgav1.ListObjectsRequest{
				StoreId:  storeID,
				Type:     test.objectType,
				Relation: test.relation,
				User:     test.user,
			})

			require.NoError(t, err)

			require.Equal(t, test.expectedDispatchCount, resp.ResolutionMetadata.DispatchCounter.Load())
			require.Equal(t, test.expectedThrottlingValue > 0, resp.ResolutionMetadata.WasThrottled.Load())
		})
	}
}

func TestDoesNotUseCacheWhenHigherConsistencyEnabled(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)
	ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	modelDsl := `model
			schema 1.1

			type user

			type folder
				relations
					define viewer: [user] but not blocked
					define blocked: [user]`
	tuples := []string{
		"folder:C#viewer@user:jon",
		"folder:B#viewer@user:jon",
		"folder:A#viewer@user:jon",
	}

	storeID, model := storagetest.BootstrapFGAStore(t, ds, modelDsl, tuples)
	ts, err := typesystem.NewAndValidate(
		context.Background(),
		model,
	)
	require.NoError(t, err)

	checkCache := storage.NewInMemoryLRUCache[any]()
	defer checkCache.Stop()

	// Write an item to the cache that has an Allowed value of false for folder:A
	req := &graph.ResolveCheckRequest{
		StoreID: storeID,
		TupleKey: &openfgav1.TupleKey{
			User:     "user:jon",
			Relation: "viewer",
			Object:   "folder:A",
		},
	}
	cacheKey, err := graph.CheckRequestCacheKey(req)
	require.NoError(t, err)

	checkCache.Set(cacheKey, &graph.CheckResponseCacheEntry{
		LastModified: time.Now(),
		CheckResponse: &graph.ResolveCheckResponse{
			Allowed: false,
		}}, 10*time.Second)

	require.NoError(t, err)
	ctx = typesystem.ContextWithTypesystem(ctx, ts)

	checkResolver, checkResolverCloser := graph.NewOrderedCheckResolvers([]graph.CheckResolverOrderedBuilderOpt{
		graph.WithCachedCheckResolverOpts(true, []graph.CachedCheckResolverOpt{
			graph.WithExistingCache(checkCache),
		}...),
	}...).Build()
	t.Cleanup(checkResolverCloser)

	q, _ := NewListObjectsQuery(
		ds,
		checkResolver,
	)

	// Run a check with MINIMIZE_LATENCY that will use the cache we added with 2 tuples
	resp, err := q.Execute(ctx, &openfgav1.ListObjectsRequest{
		StoreId:     storeID,
		Type:        "folder",
		Relation:    "viewer",
		User:        "user:jon",
		Consistency: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
	})

	require.NoError(t, err)
	require.Len(t, resp.Objects, 2)

	// Now run a check with HIGHER_CONSISTENCY that will evaluate against the known tuples and return 3 tuples
	resp, err = q.Execute(ctx, &openfgav1.ListObjectsRequest{
		StoreId:     storeID,
		Type:        "folder",
		Relation:    "viewer",
		User:        "user:jon",
		Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
	})

	require.NoError(t, err)
	require.Len(t, resp.Objects, 3)

	// Rerun check with MINIMIZE_LATENCY to ensure the cache was updated with the tuple we retrieved during the previous call
	resp, err = q.Execute(ctx, &openfgav1.ListObjectsRequest{
		StoreId:     storeID,
		Type:        "folder",
		Relation:    "viewer",
		User:        "user:jon",
		Consistency: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
	})

	require.NoError(t, err)
	require.Len(t, resp.Objects, 3)

	// Now set the third item as `allowed: false` in the cache and run with `UNSPECIFIED`, it should use the cache and only return two item
	checkCache.Set(cacheKey, &graph.CheckResponseCacheEntry{
		LastModified: time.Now(),
		CheckResponse: &graph.ResolveCheckResponse{
			Allowed: false,
		}}, 10*time.Second)

	resp, err = q.Execute(ctx, &openfgav1.ListObjectsRequest{
		StoreId:     storeID,
		Type:        "folder",
		Relation:    "viewer",
		User:        "user:jon",
		Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED,
	})

	require.NoError(t, err)
	require.Len(t, resp.Objects, 2)
}
