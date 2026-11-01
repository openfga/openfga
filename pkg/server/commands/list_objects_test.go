package commands

import (
	"context"
	"errors"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	internalErrors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/internal/throttler/threshold"
	"github.com/openfga/openfga/pkg/featureflags"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const fakeStoreID = "store_id_123"

func TestNewListObjectsQuery(t *testing.T) {
	t.Run("nil_datastore", func(t *testing.T) {
		checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers().Build()
		require.NoError(t, err)
		t.Cleanup(checkResolverCloser)
		q, err := NewListObjectsQuery(nil, checkResolver, fakeStoreID)
		require.Nil(t, q)
		require.Error(t, err)
	})

	t.Run("nil_checkResolver", func(t *testing.T) {
		q, err := NewListObjectsQuery(memory.New(), nil, fakeStoreID)
		require.Nil(t, q)
		require.Error(t, err)
	})

	t.Run("empty_typesystem_in_context", func(t *testing.T) {
		checkResolver := graph.NewLocalChecker()
		q, err := NewListObjectsQuery(memory.New(), checkResolver, fakeStoreID)
		require.NoError(t, err)

		_, err = q.Execute(context.Background(), &openfgav1.ListObjectsRequest{})
		require.ErrorContains(t, err, "typesystem missing in context")

		var srv openfgav1.OpenFGAService_StreamedListObjectsServer
		_, err = q.ExecuteStreamed(context.Background(), &openfgav1.StreamedListObjectsRequest{}, srv)
		require.ErrorContains(t, err, "typesystem missing in context")
	})
}

func TestNewListObjectsQueryReturnsShadowedQueryWhenEnabled(t *testing.T) {
	testLogger := logger.NewNoopLogger()
	q, err := NewListObjectsQueryWithShadowConfig(memory.New(), graph.NewLocalChecker(),
		NewShadowListObjectsQueryConfig(
			WithShadowListObjectsQueryEnabled(true),
			WithShadowListObjectsQueryTimeout(13*time.Second),
			WithShadowListObjectsQueryLogger(testLogger),
		),
		fakeStoreID,
	)
	require.NoError(t, err)
	require.NotNil(t, q)
	sq, isShadowed := q.(*shadowedListObjectsQuery)
	require.True(t, isShadowed)
	assert.Equal(t, 13*time.Second, sq.shadowTimeout)
	assert.Equal(t, testLogger, sq.logger)
}

func TestNewListObjectsQueryReturnsStandardQueryWhenShadowDisabled(t *testing.T) {
	q, err := NewListObjectsQueryWithShadowConfig(memory.New(), graph.NewLocalChecker(),
		NewShadowListObjectsQueryConfig(WithShadowListObjectsQueryEnabled(false)),
		fakeStoreID,
	)
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
				fakeStoreID,
				WithListObjectsPipelineEnabled(false),
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
			require.Equal(t, test.expectedThrottlingValue > 0, resp.ResolutionMetadata.DispatchThrottled.Load())
		})
	}
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
		Return(nil, internalErrors.ErrUnknown).
		Times(1)
	mockCheckResolver.EXPECT().GetDelegate().AnyTimes().Return(nil)

	q, _ := NewListObjectsQuery(
		ds,
		mockCheckResolver,
		fakeStoreID,
		WithListObjectsPipelineEnabled(false),
	)

	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
	resp, err := q.Execute(ctx, &openfgav1.ListObjectsRequest{
		StoreId:  storeID,
		Type:     "folder",
		Relation: "viewer",
		User:     "user:maria",
	})

	require.Nil(t, resp)
	require.ErrorIs(t, err, internalErrors.ErrUnknown)
}
func TestAttemptsToInvalidateWhenIteratorCacheIsEnabled(t *testing.T) {
	t.Run("cache_is_invalidated_if_enabled", func(t *testing.T) {
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
		mockShadowCacheController.EXPECT().InvalidateIfNeeded(gomock.Any(), gomock.Any()).Times(1)

		cacheSettings := serverconfig.CacheSettings{
			ListObjectsIteratorCacheEnabled:    true,
			ListObjectsIteratorCacheTTL:        1 * time.Second,
			ListObjectsIteratorCacheMaxResults: 1000,
			CacheControllerEnabled:             true,
			CacheControllerTTL:                 1 * time.Nanosecond,
			CheckCacheLimit:                    1000,
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
			fakeStoreID,
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

func TestListObjectsSeqError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	errorRet := errors.New("test")
	mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
	mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any())
	mockDatastore.EXPECT().MaxTuplesPerWrite().Return(40)
	mockDatastore.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, errorRet)

	model := `
		model
			schema 1.1
			type user

			type document
			relations
				define viewer: [user]
				define editor: [user]
				define admin: viewer and editor
	`
	tuples := []string{
		"document:1#viewer@user:a",
		"document:2#editor@user:a",
	}
	storeID, authModel := storagetest.BootstrapFGAStore(t, mockDatastore, model, tuples)
	typesys, err := typesystem.New(
		authModel,
	)
	require.NoError(t, err)
	ctx := storage.ContextWithRelationshipTupleReader(context.Background(), mockDatastore)
	ctx = typesystem.ContextWithTypesystem(ctx, typesys)

	checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers().Build()
	require.NoError(t, err)
	t.Cleanup(checkResolverCloser)

	t.Run("execute_seq_error", func(t *testing.T) {
		query, err := NewListObjectsQuery(
			mockDatastore,
			checkResolver,
			fakeStoreID,
		)
		require.NoError(t, err)

		_, err = query.Execute(ctx, &openfgav1.ListObjectsRequest{
			StoreId:              storeID,
			AuthorizationModelId: authModel.GetId(),
			Type:                 "document",
			Relation:             "admin",
			User:                 "user:a",
		})
		require.ErrorIs(t, err, errorRet)
	})

	t.Run("execute_streamed_seq_error", func(t *testing.T) {
		query, err := NewListObjectsQuery(
			mockDatastore,
			checkResolver,
			fakeStoreID,
		)
		require.NoError(t, err)

		var srv openfgav1.OpenFGAService_StreamedListObjectsServer
		_, err = query.ExecuteStreamed(ctx, &openfgav1.StreamedListObjectsRequest{
			StoreId:              storeID,
			AuthorizationModelId: authModel.GetId(),
			Type:                 "document",
			Relation:             "admin",
			User:                 "user:a",
		}, srv)
		require.ErrorIs(t, err, errorRet)
	})
}

func TestListObjectsPipelineConditionError(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)

	storeID, model := storagetest.BootstrapFGAStore(t, ds, `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with cond]
		condition cond(x: int) {
			x > 0
		}`, nil)
	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:anne", "cond", nil),
	})
	require.NoError(t, err)

	typesys, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)

	checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers().Build()
	require.NoError(t, err)
	t.Cleanup(checkResolverCloser)

	q, err := NewListObjectsQuery(ds, checkResolver, storeID, WithListObjectsPipelineEnabled(true))
	require.NoError(t, err)

	_, err = q.Execute(ctx, &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "document",
		Relation:             "viewer",
		User:                 "user:anne",
	})
	require.Equal(t, codes.Code(openfgav1.ErrorCode_validation_error), status.Code(err))
	require.ErrorContains(t, err, "is missing context parameters '[x]'")
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

	b.ReportMetric(float64(best.Microseconds()), "min_us")
	b.ReportMetric(float64(worst.Microseconds()), "max_us")
	b.ReportMetric(float64(p95.Microseconds()), "p95_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us")
}

func runOneBenchmark(
	b *testing.B,
	ctx context.Context,
	name string,
	optimizationsEnabled bool,
	pipelineEnabled bool,
	query ListObjectsQuery,
	request *openfgav1.ListObjectsRequest,
	numTuples int, //nolint:unparam
) {
	if optimizationsEnabled {
		name += "_with_optimization"
	}
	query.ff = featureflags.NewHardcodedBooleanClient(optimizationsEnabled)

	if pipelineEnabled {
		name += "_with_pipeline"
		query.pipelineEnabled = true
	} else {
		query.pipelineEnabled = false
	}
	var latencies []time.Duration
	b.Run(name, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			start := time.Now()
			res, err := query.Execute(ctx, request)
			latencies = append(latencies, time.Since(start))
			require.NoError(b, err)

			// all the tests are configured to return 5k objects
			require.Len(b, res.Objects, numTuples)
		}
		reportLatencies(b, latencies)
	})
}

// BenchmarkListObjects sets up an authorization model with various relationship weights:
// weight one direct, weight one computed, weight two, weight three, and recursive (weight INF).
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
		fakeStoreID,
		WithFeatureFlagClient(featureflags.NewHardcodedBooleanClient(true)),

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

	// once with optimizations enabled, once without
	runOneBenchmark(b, ctx, "weight_one_direct", true, false, *query, weightOneRequest, n)
	runOneBenchmark(b, ctx, "weight_one_direct", false, false, *query, weightOneRequest, n)
	runOneBenchmark(b, ctx, "weight_one_direct", false, true, *query, weightOneRequest, n)

	weightOneComputedRequest := &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "org",
		Relation:             "computed",
		User:                 "user:justin",
	}

	// once with optimizations enabled, once without
	runOneBenchmark(b, ctx, "weight_one_computed", true, false, *query, weightOneComputedRequest, n)
	runOneBenchmark(b, ctx, "weight_one_computed", false, false, *query, weightOneComputedRequest, n)
	runOneBenchmark(b, ctx, "weight_one_computed", false, true, *query, weightOneComputedRequest, n)

	weightTwoRequest := &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "company",
		Relation:             "org_member",
		User:                 "user:justin",
	}
	// once with optimizations enabled, once without
	runOneBenchmark(b, ctx, "weight_two_ttu", true, false, *query, weightTwoRequest, n)
	runOneBenchmark(b, ctx, "weight_two_ttu", false, false, *query, weightTwoRequest, n)
	runOneBenchmark(b, ctx, "weight_two_ttu", false, true, *query, weightTwoRequest, n)

	weightThreeRequest := &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "office",
		Relation:             "weight_three",
		User:                 "user:justin",
	}

	// once with optimizations enabled, once without
	runOneBenchmark(b, ctx, "weight_three", true, false, *query, weightThreeRequest, n)
	runOneBenchmark(b, ctx, "weight_three", false, false, *query, weightThreeRequest, n)
	runOneBenchmark(b, ctx, "weight_three", false, true, *query, weightThreeRequest, n)

	recursiveRequest := &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: model.GetId(),
		Type:                 "org",
		Relation:             "recursive",
		User:                 "user:justin",
	}

	// optimization currently falls back to non-optimized code when it's a recursive query
	// Uncomment this when recursive listObjects work is underway
	// runOneBenchmark(b, ctx, "recursive_ttu", true, *query, recursiveRequest)

	runOneBenchmark(b, ctx, "recursive_ttu", false, false, *query, recursiveRequest, n)
	runOneBenchmark(b, ctx, "recursive_ttu", false, true, *query, recursiveRequest, n)
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

// TestListObjectsDeadlineExceeded verifies the regression for the silent
// truncation reported in issue #3276: when the ListObjects deadline cuts
// off a Check that a candidate object depends on, Execute still returns
// the partial results with a nil error (the established behaviour), but
// the resolution metadata must record that the deadline was exceeded so
// the server handler can warn the caller. A request that completes
// within its deadline must not set the flag.
func TestListObjectsDeadlineExceeded(t *testing.T) {
	modelDsl := `
		model
			schema 1.1

		type user

		type folder
			relations
				define viewer: [user] but not blocked
				define blocked: [user]`
	tuples := []string{"folder:x#viewer@user:maria"}

	setup := func(t *testing.T, resolveCheck func(ctx context.Context) (*graph.ResolveCheckResponse, error), deadline time.Duration) (*ListObjectsQuery, string, context.Context) {
		t.Helper()

		ds := memory.New()
		t.Cleanup(ds.Close)

		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelDsl, tuples)
		ts, err := typesystem.NewAndValidate(context.Background(), model)
		require.NoError(t, err)

		ctrl := gomock.NewController(t)
		mockCheckResolver := graph.NewMockCheckResolver(ctrl)
		mockCheckResolver.EXPECT().
			ResolveCheck(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, _ *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
				return resolveCheck(ctx)
			}).
			AnyTimes()
		mockCheckResolver.EXPECT().GetDelegate().AnyTimes().Return(nil)

		q, err := NewListObjectsQuery(
			ds,
			mockCheckResolver,
			fakeStoreID,
			WithListObjectsPipelineEnabled(false),
			WithListObjectsDeadline(deadline),
		)
		require.NoError(t, err)

		return q, storeID, typesystem.ContextWithTypesystem(context.Background(), ts)
	}

	t.Run("deadline_exceeded_records_truncation_and_returns_partial_results", func(t *testing.T) {
		// The check blocks until its context is done, which happens when
		// the ListObjects deadline fires, and returns the context error:
		// the same shape as a real check cut off by the deadline.
		q, storeID, ctx := setup(t, func(ctx context.Context) (*graph.ResolveCheckResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}, 100*time.Millisecond)

		resp, err := q.Execute(ctx, &openfgav1.ListObjectsRequest{
			StoreId:  storeID,
			Type:     "folder",
			Relation: "viewer",
			User:     "user:maria",
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.Objects)
		require.True(t, resp.ResolutionMetadata.DeadlineExceeded.Load(),
			"expected the deadline truncation to be recorded on the resolution metadata")
	})

	t.Run("completed_request_does_not_record_deadline_exceeded", func(t *testing.T) {
		q, storeID, ctx := setup(t, func(_ context.Context) (*graph.ResolveCheckResponse, error) {
			return &graph.ResolveCheckResponse{Allowed: true}, nil
		}, 10*time.Second)

		resp, err := q.Execute(ctx, &openfgav1.ListObjectsRequest{
			StoreId:  storeID,
			Type:     "folder",
			Relation: "viewer",
			User:     "user:maria",
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, []string{"folder:x"}, resp.Objects)
		require.False(t, resp.ResolutionMetadata.DeadlineExceeded.Load())
	})
}

// blockingReadDatastore is a datastore whose reverse and userset reads block
// until their context is done, as reads cut off by a deadline do.
type blockingReadDatastore struct {
	storage.OpenFGADatastore
}

func (b blockingReadDatastore) ReadStartingWithUser(ctx context.Context, _ string, _ storage.ReadStartingWithUserFilter, _ storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b blockingReadDatastore) ReadUsersetTuples(ctx context.Context, _ string, _ storage.ReadUsersetTuplesFilter, _ storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestListObjectsPipelineDeadlineExceeded verifies that the pipeline
// (weighted graph) path of ListObjects records the deadline truncation:
// when the deadline fires while the pipeline is still reading, Recv stops
// without an error and Close drops the deadline error, so the flag must be
// taken from the timeout context. A request that completes within its
// deadline, or that stops at the max results, must not set it.
func TestListObjectsPipelineDeadlineExceeded(t *testing.T) {
	modelDsl := `
		model
			schema 1.1

		type user

		type document
			relations
				define viewer: [user]`

	execute := func(t *testing.T, blocking bool, deadline time.Duration, maxResults uint32, tuples []string) *ListObjectsResponse {
		t.Helper()

		ds := memory.New()
		t.Cleanup(ds.Close)

		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelDsl, tuples)
		ts, err := typesystem.NewAndValidate(context.Background(), model)
		require.NoError(t, err)

		var datastore storage.OpenFGADatastore
		if blocking {
			datastore = blockingReadDatastore{OpenFGADatastore: ds}
		} else {
			datastore = ds
		}

		checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers().Build()
		require.NoError(t, err)
		t.Cleanup(checkResolverCloser)

		q, err := NewListObjectsQuery(
			datastore,
			checkResolver,
			storeID,
			WithListObjectsPipelineEnabled(true),
			WithListObjectsDeadline(deadline),
			WithListObjectsMaxResults(maxResults),
		)
		require.NoError(t, err)

		resp, err := q.Execute(typesystem.ContextWithTypesystem(context.Background(), ts), &openfgav1.ListObjectsRequest{
			StoreId:              storeID,
			AuthorizationModelId: model.GetId(),
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:maria",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.ResolutionMetadata.WasWeightedGraphUsed.Load(), "expected the pipeline path to answer")
		return resp
	}

	t.Run("deadline_exceeded_records_truncation", func(t *testing.T) {
		resp := execute(t, true, 100*time.Millisecond, 0, []string{"document:1#viewer@user:maria"})

		require.Empty(t, resp.Objects)
		require.True(t, resp.ResolutionMetadata.DeadlineExceeded.Load(),
			"expected the deadline truncation to be recorded on the resolution metadata")
	})

	t.Run("completed_request_does_not_record_deadline_exceeded", func(t *testing.T) {
		resp := execute(t, false, 10*time.Second, 0, []string{"document:1#viewer@user:maria"})

		require.Equal(t, []string{"document:1"}, resp.Objects)
		require.False(t, resp.ResolutionMetadata.DeadlineExceeded.Load())
	})

	t.Run("max_results_reached_does_not_record_deadline_exceeded", func(t *testing.T) {
		resp := execute(t, false, 10*time.Second, 1, []string{
			"document:1#viewer@user:maria",
			"document:2#viewer@user:maria",
		})

		require.Len(t, resp.Objects, 1)
		require.False(t, resp.ResolutionMetadata.DeadlineExceeded.Load())
	})
}
