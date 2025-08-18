package commands

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/internal/throttler/threshold"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestNewListObjectsQuery(t *testing.T) {
	t.Run("nil_datastore", func(t *testing.T) {
		checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers().Build()
		require.NoError(t, err)
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

func TestNewListObjectsQueryReturnsShadowedQueryWhenEnabled(t *testing.T) {
	testLogger := logger.NewNoopLogger()
	q, err := NewListObjectsQueryWithShadowConfig(memory.New(), graph.NewLocalChecker(), NewShadowListObjectsQueryConfig(
		WithShadowListObjectsQueryEnabled(true),
		WithShadowListObjectsQuerySamplePercentage(100),
		WithShadowListObjectsQueryTimeout(13*time.Second),
		WithShadowListObjectsQueryLogger(testLogger),
	))
	require.NoError(t, err)
	require.NotNil(t, q)
	sq, isShadowed := q.(*shadowedListObjectsQuery)
	require.True(t, isShadowed)
	assert.True(t, sq.checkShadowModeSampleRate())
	assert.Equal(t, 100, sq.shadowPct)
	assert.Equal(t, 13*time.Second, sq.shadowTimeout)
	assert.Equal(t, testLogger, sq.logger)
}

func TestNewListObjectsQueryReturnsStandardQueryWhenShadowDisabled(t *testing.T) {
	q, err := NewListObjectsQueryWithShadowConfig(memory.New(), graph.NewLocalChecker(), NewShadowListObjectsQueryConfig(
		WithShadowListObjectsQueryEnabled(false),
	))
	require.NoError(t, err)
	require.NotNil(t, q)
	_, isStandard := q.(*ListObjectsQuery)
	require.True(t, isStandard)
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
			ctx := typesystem.ContextWithTypesystem(ctx, ts)

			checker, checkResolverCloser, err := graph.NewOrderedCheckResolvers(
				graph.WithDispatchThrottlingCheckResolverOpts(true, []graph.DispatchThrottlingCheckResolverOpt{
					graph.WithDispatchThrottlingCheckResolverConfig(graph.DispatchThrottlingCheckResolverConfig{
						DefaultThreshold: 0,
						MaxThreshold:     0,
					}),
					graph.WithThrottler(mockThrottler),
				}...)).Build()
			require.NoError(t, err)
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
				WithMaxConcurrentReads(1),
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

	checkCache, err := storage.NewInMemoryLRUCache[any]()
	require.NoError(t, err)
	defer checkCache.Stop()

	// Write an item to the cache that has an Allowed value of false for folder:A
	req, err := graph.NewResolveCheckRequest(graph.ResolveCheckRequestParams{
		StoreID:              storeID,
		AuthorizationModelID: ts.GetAuthorizationModelID(),
		TupleKey: &openfgav1.TupleKey{
			User:     "user:jon",
			Relation: "viewer",
			Object:   "folder:A",
		},
	})
	require.NoError(t, err)

	// Preload the cache
	cacheKey := graph.BuildCacheKey(*req)
	checkCache.Set(cacheKey, &graph.CheckResponseCacheEntry{
		LastModified: time.Now(),
		CheckResponse: &graph.ResolveCheckResponse{
			Allowed: false,
		}}, 10*time.Second)

	require.NoError(t, err)
	ctx = typesystem.ContextWithTypesystem(ctx, ts)

	checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers([]graph.CheckResolverOrderedBuilderOpt{
		graph.WithCachedCheckResolverOpts(true, []graph.CachedCheckResolverOpt{
			graph.WithExistingCache(checkCache),
		}...),
	}...).Build()
	require.NoError(t, err)
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

func TestErrorInCheckSurfacesInListObjects(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
	modelDsl := `
		model
			schema 1.1

		type user

		type folder
			relations
				define viewer: [user] but not blocked
				define blocked: [user]`
	tuples := []string{
		"folder:x#viewer@user:maria",
	}

	storeID, model := storagetest.BootstrapFGAStore(t, ds, modelDsl, tuples)
	ts, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockCheckResolver := graph.NewMockCheckResolver(mockController)
	mockCheckResolver.EXPECT().
		ResolveCheck(gomock.Any(), gomock.Any()).
		Return(nil, errors.ErrUnknown).
		Times(1)
	mockCheckResolver.EXPECT().GetDelegate().AnyTimes().Return(nil)

	q, _ := NewListObjectsQuery(ds, mockCheckResolver)

	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
	resp, err := q.Execute(ctx, &openfgav1.ListObjectsRequest{
		StoreId:  storeID,
		Type:     "folder",
		Relation: "viewer",
		User:     "user:maria",
	})

	require.Nil(t, resp)
	require.ErrorIs(t, err, errors.ErrUnknown)
}
func TestAttemptsToInvalidateWhenIteratorCacheIsEnabled(t *testing.T) {
	tests := []struct {
		shadowEnabled bool
	}{
		{
			shadowEnabled: false,
		},
		{
			shadowEnabled: true,
		},
	}

	for _, test := range tests {
		t.Run("shadow_enabled_"+strconv.FormatBool(test.shadowEnabled), func(t *testing.T) {
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

			ctx = typesystem.ContextWithTypesystem(ctx, ts)

			// Don't care about the resolver for this test
			mockCheckResolver := graph.NewMockCheckResolver(ctrl)
			mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
				return &graph.ResolveCheckResponse{}, nil
			})
			mockCheckResolver.EXPECT().GetDelegate().AnyTimes().Return(nil)

			// Need to make sure list objects attempts to invalidate when cache is enabled
			mockCacheController := mocks.NewMockCacheController(ctrl)
			mockCacheController.EXPECT().InvalidateIfNeeded(gomock.Any(), gomock.Any()).Times(1)

			mockShadowCacheController := mocks.NewMockCacheController(ctrl)
			if test.shadowEnabled {
				mockShadowCacheController.EXPECT().InvalidateIfNeeded(gomock.Any(), gomock.Any()).Times(1)
			}

			cacheSettings := serverconfig.CacheSettings{
				ListObjectsIteratorCacheEnabled:    true,
				ListObjectsIteratorCacheTTL:        1 * time.Second,
				ListObjectsIteratorCacheMaxResults: 1000,
				CacheControllerEnabled:             true,
				CacheControllerTTL:                 1 * time.Nanosecond,
				CheckCacheLimit:                    1000,
				ShadowCheckCacheEnabled:            test.shadowEnabled,
			}

			sharedResources, err := shared.NewSharedDatastoreResources(
				ctx,
				&singleflight.Group{},
				ds,
				cacheSettings,
				shared.WithCacheController(mockCacheController),
				shared.WithShadowCacheController(mockShadowCacheController),
			)
			require.NoError(t, err)

			q, _ := NewListObjectsQuery(
				ds,
				mockCheckResolver,
				WithListObjectsCache(sharedResources, cacheSettings),
			)

			// Run a check, mockCacheController should receive its invalidate call
			_, err = q.Execute(ctx, &openfgav1.ListObjectsRequest{
				StoreId:  storeID,
				Type:     "folder",
				Relation: "viewer",
				User:     "user:jon",
			})

			sharedResources.Close()
			require.NoError(t, err)
		})
	}
}

func reportLatencies(b *testing.B, latencies []time.Duration) {
	// Sort latencies ascending
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	worst := latencies[len(latencies)-1]

	best := latencies[0]
	b.ReportMetric(float64(p95.Microseconds()), "p95_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us")
	b.ReportMetric(float64(worst.Microseconds()), "max_us")
	b.ReportMetric(float64(best.Microseconds()), "min_us")
}

// BenchmarkListObjects sets up an authorization model with various relationship weights:
// weight one direct, weight one computed, weight two, weight three, and recursive (weight INF).
// Each weight has 10k tuples written for that relation, and a benchmark is b.Run() specific to each weight.
// The benchmarks are currently run 2x—once with optimizations enabled and once without.
func BenchmarkListObjects(b *testing.B) {
	datastore := memory.New()
	b.Cleanup(datastore.Close)
	storeID := ulid.Make().String()

	model := &openfgav1.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustTransformDSLToProto(`
			model
			schema 1.1
			type user
			type org
				relations
					define member: [user]
					define computed: member
					define parent: [org]
					define recursive: [user] or recursive from parent
			type company
				relations
					define owner: [org]
					define org_member: member from owner
			type office
				relations
					define parent: [company]
					define weight_three: org_member from parent
		`).GetTypeDefinitions(),
	}
	ctx := context.Background()
	err := datastore.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(b, err)

	n := 5000
	createDirectWeightOneRelations(b, ctx, datastore, storeID, n)
	createWeightTwoRelations(b, ctx, datastore, storeID, n)
	createWeightThreeRelations(b, ctx, datastore, storeID, n)
	createRecursiveRelations(b, ctx, datastore, storeID, n)

	checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers().Build()
	require.NoError(b, err)
	b.Cleanup(checkResolverCloser)

	query, err := NewListObjectsQuery(
		datastore,
		checkResolver,
		WithListObjectsOptimizationsEnabled(true),

		// unlimited results, these tests are designed to return `n` results per iteration
		WithListObjectsMaxResults(0),
	)
	require.NoError(b, err)

	ts, err := typesystem.New(model)
	require.NoError(b, err)
	ctx = typesystem.ContextWithTypesystem(ctx, ts)

	weightOneRequest := &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "org",
		Relation:             "member",
		User:                 "user:justin",
	}

	b.Run("weight_one_direct_with_optimization", func(b *testing.B) {
		query.optimizationsEnabled = true
		var latencies []time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, weightOneRequest)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)
			require.Len(b, res.Objects, n)
		}
		reportLatencies(b, latencies)
	})

	b.Run("weight_one_direct", func(b *testing.B) {
		query.optimizationsEnabled = false
		var latencies []time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, weightOneRequest)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)
			require.Len(b, res.Objects, n)
		}
		reportLatencies(b, latencies)
	})

	weightOneComputedRequest := &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "org",
		Relation:             "computed",
		User:                 "user:justin",
	}

	b.Run("weight_one_computed_with_optimization", func(b *testing.B) {
		query.optimizationsEnabled = true
		var latencies []time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, weightOneComputedRequest)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)
			require.Len(b, res.Objects, n)
		}
		reportLatencies(b, latencies)
	})

	b.Run("weight_one_computed", func(b *testing.B) {
		query.optimizationsEnabled = false
		var latencies []time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, weightOneComputedRequest)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)
			require.Len(b, res.Objects, n)
		}
		reportLatencies(b, latencies)
	})

	weightTwoRequest := &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "company",
		Relation:             "org_member",
		User:                 "user:justin",
	}

	b.Run("weight_two_ttu_with_optimizations", func(b *testing.B) {
		query.optimizationsEnabled = true
		var latencies []time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, weightTwoRequest)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)
			require.Len(b, res.Objects, n) // probably don't even need these?
		}
		reportLatencies(b, latencies)
	})

	b.Run("weight_two_ttu", func(b *testing.B) {
		query.optimizationsEnabled = false
		var latencies []time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, weightTwoRequest)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)
			require.Len(b, res.Objects, n)
		}
		reportLatencies(b, latencies)
	})

	weightThreeRequest := &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "office",
		Relation:             "weight_three",
		User:                 "user:justin",
	}

	b.Run("weight_three_with_optimization", func(b *testing.B) {
		query.optimizationsEnabled = true
		var latencies []time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, weightThreeRequest)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)
			require.Len(b, res.Objects, n)
		}
		reportLatencies(b, latencies)
	})

	b.Run("weight_three", func(b *testing.B) {
		query.optimizationsEnabled = false
		var latencies []time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, weightThreeRequest)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)
			require.Len(b, res.Objects, n)
		}
		reportLatencies(b, latencies)
	})

	recursiveRequest := &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "org",
		Relation:             "recursive",
		User:                 "user:justin",
	}

	// optimization currently falls back to non-optimized code when it's a recursive query
	// Uncomment this when recursive listObjects work is underway
	// b.Run("recursive_ttu_with_optimizations", func(b *testing.B) {
	//	query.optimizationsEnabled = true
	//	for i := 0; i < b.N; i++ {
	//		_, err := query.Execute(ctx, recursiveRequest)
	//		require.NoError(b, err)
	//	}
	// })

	b.Run("recursive_ttu", func(b *testing.B) {
		query.optimizationsEnabled = false
		var latencies []time.Duration
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, recursiveRequest)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)
			require.Len(b, res.Objects, n)
		}
		reportLatencies(b, latencies)
	})
}

// This helper writes tuples for user:justin with relation "member" to org:0...org:numTuples.
func createDirectWeightOneRelations(
	b *testing.B,
	ctx context.Context,
	datastore storage.OpenFGADatastore,
	storeID string,
	numTuples int,
) {
	b.Helper()
	objID := 0
	for objID < numTuples {
		tuples := make([]*openfgav1.TupleKey, datastore.MaxTuplesPerWrite())

		for j := 0; j < datastore.MaxTuplesPerWrite(); j++ {
			obj := "org:" + strconv.Itoa(objID)
			tuples[j] = tuple.NewTupleKey(obj, "member", "user:justin")
			objID++
		}
		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(b, err)
	}
}

func createWeightTwoRelations(b *testing.B, ctx context.Context, datastore storage.OpenFGADatastore, storeID string, numTuples int) {
	b.Helper()
	objID := 0
	for objID < numTuples {
		tuples := make([]*openfgav1.TupleKey, datastore.MaxTuplesPerWrite())

		for j := 0; j < datastore.MaxTuplesPerWrite(); j++ {
			// These IDs can be the same as we already created org:0 - org:numTuples
			obj := "company:" + strconv.Itoa(objID)
			user := "org:" + strconv.Itoa(objID)
			tuples[j] = tuple.NewTupleKey(obj, "owner", user)
			objID++
		}
		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(b, err)
	}
}

func createWeightThreeRelations(b *testing.B, ctx context.Context, datastore storage.OpenFGADatastore, storeID string, numTuples int) {
	b.Helper()
	objID := 0
	for objID < numTuples {
		tuples := make([]*openfgav1.TupleKey, datastore.MaxTuplesPerWrite())

		for j := 0; j < datastore.MaxTuplesPerWrite(); j++ {
			// These IDs can be the same as we already created org:0 - org:numTuples
			obj := "office:" + strconv.Itoa(objID)
			user := "company:" + strconv.Itoa(objID)
			tuples[j] = tuple.NewTupleKey(obj, "parent", user)
			objID++
		}
		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(b, err)
	}
}

func createRecursiveRelations(b *testing.B, ctx context.Context, datastore storage.OpenFGADatastore, storeID string, numTuples int) {
	b.Helper()
	objID := 0
	for objID < numTuples {
		tuples := make([]*openfgav1.TupleKey, datastore.MaxTuplesPerWrite())

		for j := 0; j < datastore.MaxTuplesPerWrite(); j++ {
			// Every 10th item, make user:justin the leaf
			if j%10 == 0 {
				obj := "org:" + strconv.Itoa(objID)
				tk := tuple.NewTupleKey(obj, "recursive", "user:justin")
				tuples[j] = tk
				tuples = append(tuples, tk)
				objID++
				continue
			}

			// otherwise, chain the org#parent#org relation
			obj := "org:" + strconv.Itoa(objID)
			user := "org:" + strconv.Itoa(objID-1)
			tuples[j] = tuple.NewTupleKey(obj, "parent", user)
			objID++
		}
		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(b, err)
	}
}
