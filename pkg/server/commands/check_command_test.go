package commands

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	ofga_errors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/internal/graph"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestCheckQuery(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockCheckResolver := graph.NewMockCheckResolver(mockController)
	model := testutils.MustTransformDSLToProtoWithID(`
model
	schema 1.1
type user
type doc
	relations
		define viewer: [user]
		define viewer_computed: viewer
`)
	ts, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	t.Run("validates_input_user", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID: ulid.Make().String(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "invalid:1",
				Relation: "viewer",
				Object:   "doc:1",
			},
			Typesys: ts,
		})
		_, _, err := cmd.Execute(context.Background())
		require.ErrorContains(t, err, "type 'invalid' not found")
	})

	t.Run("validates_input_relation", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID: ulid.Make().String(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "user:1",
				Relation: "invalid",
				Object:   "doc:1",
			},
			Typesys: ts,
		})
		_, _, err := cmd.Execute(context.Background())
		require.ErrorContains(t, err, "relation 'doc#invalid' not found")
	})

	t.Run("validates_input_object", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID: ulid.Make().String(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "user:1",
				Relation: "viewer",
				Object:   "invalid:1",
			},
			Typesys: ts,
		})
		_, _, err := cmd.Execute(context.Background())
		require.ErrorContains(t, err, "type 'invalid' not found")
	})

	t.Run("validates_input_contextual_tuple", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("invalid:1", "viewer", "user:1"),
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("invalid:1", "viewer", "user:1"),
				},
			},
			Typesys: ts,
		})
		_, _, err := cmd.Execute(context.Background())
		require.ErrorContains(t, err, "type 'invalid' not found")
	})

	t.Run("validates_tuple_key_less_strictly_than_contextual_tuples", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer_computed", "user:1"),
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					// this isn't a tuple that you can write
					tuple.NewTupleKey("doc:1", "viewer_computed", "user:1"),
				},
			},
			Typesys: ts,
		})
		_, _, err := cmd.Execute(context.Background())
		require.ErrorContains(t, err, "type 'user' is not an allowed type restriction for 'doc#viewer_computed'")
	})

	t.Run("no_validation_error_and_call_to_resolver_goes_through", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
			Typesys:  ts,
		})
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			Return(&graph.ResolveCheckResponse{}, nil)
		_, _, err := cmd.Execute(context.Background())
		require.NoError(t, err)
	})

	t.Run("returns_db_metrics", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
			Typesys:  ts,
		})
		mockDatastore.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
				ds, _ := storage.RelationshipTupleReaderFromContext(ctx)
				_, _ = ds.Read(ctx, req.StoreID, nil, storage.ReadOptions{})
				return &graph.ResolveCheckResponse{}, nil
			})
		checkResp, _, err := cmd.Execute(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint32(1), checkResp.GetResolutionMetadata().DatastoreQueryCount)
	})

	t.Run("sets_context", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
			Typesys:  ts,
		})
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
				tsFromContext, ok := typesystem.TypesystemFromContext(ctx)
				require.True(t, ok)
				require.Equal(t, ts, tsFromContext)

				_, ok = storage.RelationshipTupleReaderFromContext(ctx)
				require.True(t, ok)
				return &graph.ResolveCheckResponse{}, nil
			})
		_, _, err := cmd.Execute(context.Background())
		require.NoError(t, err)
	})

	t.Run("no_validation_error_but_call_to_resolver_fails", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
			Typesys:  ts,
		})
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).Return(nil, ofga_errors.ErrUnknown)
		_, _, err := cmd.Execute(context.Background())
		require.ErrorIs(t, err, ofga_errors.ErrUnknown)
	})

	t.Run("ignores_cache_controller_with_high_consistency", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:     ulid.Make().String(),
			TupleKey:    tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			Typesys:     ts,
		})
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
			require.Zero(t, req.GetLastCacheInvalidationTime())
			return &graph.ResolveCheckResponse{}, nil
		})
		_, _, err := cmd.Execute(context.Background())
		require.NoError(t, err)
	})

	t.Run("cache_controller_sets_invalidation_time", func(t *testing.T) {
		storeID := ulid.Make().String()
		invalidationTime := time.Now().UTC()
		cacheController := mockstorage.NewMockCacheController(mockController)
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:  storeID,
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
			Typesys:  ts,
		},
			WithCheckCommandCache(&shared.SharedDatastoreResources{
				CacheController: cacheController,
				Logger:          logger.NewNoopLogger(),
			}, config.CacheSettings{}))
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
			require.Equal(t, req.GetLastCacheInvalidationTime(), invalidationTime)
			return &graph.ResolveCheckResponse{}, nil
		})
		cacheController.EXPECT().DetermineInvalidationTime(gomock.Any(), storeID).Return(invalidationTime)
		_, _, err := cmd.Execute(context.Background())
		require.NoError(t, err)
	})

	t.Run("fails_if_store_id_is_missing", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:  "",
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
			Typesys:  ts,
		})

		_, _, err := cmd.Execute(context.Background())
		require.Error(t, err)
	})

	t.Run("metadata_on_error", func(t *testing.T) {
		ctx := context.Background()

		mockDatastore.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Times(1)

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
				req.GetRequestMetadata().Depth++
				req.GetRequestMetadata().DispatchCounter.Add(1)
				req.GetRequestMetadata().WasThrottled.Store(true)
				ds, _ := storage.RelationshipTupleReaderFromContext(ctx)
				_, _ = ds.Read(ctx, req.StoreID, nil, storage.ReadOptions{})
				return nil, context.DeadlineExceeded
			})

		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
			Typesys:  ts,
		})
		checkResp, checkRequestMetadata, err := cmd.Execute(ctx)

		require.Error(t, err)
		require.Equal(t, uint32(1), checkResp.GetResolutionMetadata().DatastoreQueryCount)
		require.Equal(t, uint32(1), checkRequestMetadata.Depth)
		require.Equal(t, uint32(1), checkRequestMetadata.DispatchCounter.Load())
		require.True(t, checkRequestMetadata.WasThrottled.Load())
	})
}

func TestCheckCommandErrorToServerError(t *testing.T) {
	testcases := map[string]struct {
		inputError    error
		expectedError error
	}{
		`1`: {
			inputError:    graph.ErrResolutionDepthExceeded,
			expectedError: serverErrors.ErrAuthorizationModelResolutionTooComplex,
		},
		`2`: {
			inputError:    condition.ErrEvaluationFailed,
			expectedError: serverErrors.ValidationError(condition.ErrEvaluationFailed),
		},
		`3`: {
			inputError:    &ThrottledError{},
			expectedError: serverErrors.ErrThrottledTimeout,
		},
		`4`: {
			inputError:    context.DeadlineExceeded,
			expectedError: serverErrors.ErrRequestDeadlineExceeded,
		},
		`5`: {
			inputError: &InvalidTupleError{Cause: errors.New("oh no")},
			expectedError: serverErrors.HandleTupleValidateError(
				&tuple.InvalidTupleError{
					Cause: &InvalidTupleError{Cause: errors.New("oh no")},
				},
			),
		},
		`6`: {
			inputError:    &InvalidRelationError{Cause: errors.New("oh no")},
			expectedError: serverErrors.ValidationError(&InvalidRelationError{Cause: errors.New("oh no")}),
		},
		`7`: {
			inputError:    ofga_errors.ErrUnknown,
			expectedError: ofga_errors.ErrUnknown,
		},
	}

	for name, testCase := range testcases {
		t.Run(name, func(t *testing.T) {
			actualError := CheckCommandErrorToServerError(testCase.inputError)
			require.ErrorIs(t, actualError, testCase.expectedError)
		})
	}
}

func TestCheckCommandConfig(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockCheckResolver := graph.NewMockCheckResolver(mockController)

	t.Run("NewCheckCommandConfig", func(t *testing.T) {
		options := []CheckQueryOption{
			WithCheckCommandMaxConcurrentReads(10),
			WithCheckCommandLogger(logger.NewNoopLogger()),
		}

		config := NewCheckCommandConfig(mockDatastore, mockCheckResolver, options...)

		require.Equal(t, mockDatastore, config.datastore)
		require.Equal(t, mockCheckResolver, config.checkResolver)
		require.Equal(t, options, config.options)
	})

	t.Run("NewCheckCommandConfig_WithoutOptions", func(t *testing.T) {
		config := NewCheckCommandConfig(mockDatastore, mockCheckResolver)

		require.Equal(t, mockDatastore, config.datastore)
		require.Equal(t, mockCheckResolver, config.checkResolver)
		require.Empty(t, config.options)
	})
}

func TestCheckCommandServerConfig(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockCheckResolver := graph.NewMockCheckResolver(mockController)
	mockShadowDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockShadowCheckResolver := graph.NewMockCheckResolver(mockController)

	mainConfig := NewCheckCommandConfig(mockDatastore, mockCheckResolver)

	t.Run("NewCheckCommandServerConfig", func(t *testing.T) {
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(true),
			WithShadowCheckQueryPct(50),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))

		require.Equal(t, mainConfig, serverConfig.checkCfg)
		require.Equal(t, shadowConfig, serverConfig.shadowCfg)
	})

	t.Run("IsShadowEnabled_WhenShadowEnabled", func(t *testing.T) {
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(true),
			WithShadowCheckQueryPct(50),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))

		require.True(t, serverConfig.shadowCfg.isEnabled())
	})

	t.Run("IsShadowEnabled_WhenShadowDisabled", func(t *testing.T) {
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(false),
			WithShadowCheckQueryPct(0),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))

		require.False(t, serverConfig.shadowCfg.isEnabled())
	})

	t.Run("IsShadowEnabled_WhenEnabledButZeroPercentage", func(t *testing.T) {
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(true),
			WithShadowCheckQueryPct(0),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))

		require.False(t, serverConfig.shadowCfg.isEnabled())
	})
}

func TestNewCheckCommandFromServerConfig(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockCheckResolver := graph.NewMockCheckResolver(mockController)
	mockShadowDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockShadowCheckResolver := graph.NewMockCheckResolver(mockController)

	model := testutils.MustTransformDSLToProtoWithID(`
model
	schema 1.1
type user
type doc
	relations
		define viewer: [user]
`)
	ts, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	params := CheckCommandParams{
		StoreID: ulid.Make().String(),
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:1",
			Relation: "viewer",
			Object:   "doc:1",
		},
		Typesys: ts,
	}

	mainConfig := NewCheckCommandConfig(mockDatastore, mockCheckResolver)

	t.Run("CreatesRegularCommand_WhenShadowDisabled", func(t *testing.T) {
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(false),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))
		command := NewCheckCommandWithServerConfig(serverConfig, params)

		// Should be a regular CheckQuery, not a shadowedCheckQuery
		_, isCheckQuery := command.(*CheckQuery)
		require.True(t, isCheckQuery, "Expected regular CheckQuery when shadow is disabled")
	})

	t.Run("CreatesShadowCommand_WhenShadowEnabled", func(t *testing.T) {
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(true),
			WithShadowCheckQueryPct(50),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))
		command := NewCheckCommandWithServerConfig(serverConfig, params)

		// Should be a shadowedCheckQuery when shadow is enabled
		_, isCheckQuery := command.(*CheckQuery)
		require.False(t, isCheckQuery, "Expected shadowedCheckQuery when shadow is enabled")
	})

	t.Run("CreatesRegularCommand_WhenShadowEnabledButZeroPercentage", func(t *testing.T) {
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(true),
			WithShadowCheckQueryPct(0),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))
		command := NewCheckCommandWithServerConfig(serverConfig, params)

		// Should be a regular CheckQuery when percentage is 0
		_, isCheckQuery := command.(*CheckQuery)
		require.True(t, isCheckQuery, "Expected regular CheckQuery when shadow percentage is 0")
	})

	t.Run("UsesMainConfigOptions", func(t *testing.T) {
		testLogger := logger.NewNoopLogger()
		mainConfigWithOptions := NewCheckCommandConfig(
			mockDatastore,
			mockCheckResolver,
			WithCheckCommandLogger(testLogger),
			WithCheckCommandMaxConcurrentReads(42),
		)

		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(false),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfigWithOptions, WithShadowCheckCommandConfig(shadowConfig))
		command := NewCheckCommandWithServerConfig(serverConfig, params)

		checkQuery, ok := command.(*CheckQuery)
		require.True(t, ok)
		require.Equal(t, testLogger, checkQuery.logger)
		require.Equal(t, uint32(42), checkQuery.maxConcurrentReads)
	})
}

func TestNewCheckCommandFromServerConfig_ShadowIntegration(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	// Main mocks
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockCheckResolver := graph.NewMockCheckResolver(mockController)

	// Shadow mocks
	mockShadowDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockShadowCheckResolver := graph.NewMockCheckResolver(mockController)

	model := testutils.MustTransformDSLToProtoWithID(`
model
	schema 1.1
type user
type doc
	relations
		define viewer: [user]
`)
	ts, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	storeID := ulid.Make().String()
	params := CheckCommandParams{
		StoreID: storeID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:1",
			Relation: "viewer",
			Object:   "doc:1",
		},
		Typesys: ts,
	}

	t.Run("ShadowCommand_CallsBothResolvers_TracksQueryCounts", func(t *testing.T) {
		// Create a test logger that we can inspect
		ctrl := gomock.NewController(t)
		mockLogger := mockstorage.NewMockLogger(ctrl)

		// Create shared resources for main resolver
		mainSharedResources := &shared.SharedDatastoreResources{
			CacheController: mockstorage.NewMockCacheController(mockController),
			Logger:          logger.NewNoopLogger(),
		}

		// Create separate shared resources for shadow resolver
		shadowSharedResources := &shared.SharedDatastoreResources{
			CacheController: mockstorage.NewMockCacheController(mockController),
			Logger:          logger.NewNoopLogger(),
		}

		// Configure main command config with cache
		mainConfig := NewCheckCommandConfig(
			mockDatastore,
			mockCheckResolver,
			WithCheckCommandLogger(logger.NewNoopLogger()),
			WithCheckCommandCache(mainSharedResources, config.CacheSettings{
				CheckQueryCacheEnabled: true,
				CheckCacheLimit:        100,
				CheckQueryCacheTTL:     time.Minute,
			}),
		)

		// Configure shadow command config with separate cache
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(
				mockShadowDatastore,
				mockShadowCheckResolver,
				WithCheckCommandLogger(logger.NewNoopLogger()),
				WithCheckCommandCache(shadowSharedResources, config.CacheSettings{
					CheckQueryCacheEnabled: true,
					CheckCacheLimit:        100,
					CheckQueryCacheTTL:     time.Minute,
				}),
			),
			WithShadowCheckQueryEnabled(true),
			WithShadowCheckQueryPct(100), // 100% to ensure shadow runs
			WithShadowCheckQueryTimeout(5*time.Second),
			WithShadowCheckQueryLogger(mockLogger), // Use mock logger to verify logging
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))

		// Verify shadow is enabled
		require.True(t, serverConfig.shadowCfg.isEnabled())

		// Create command
		command := NewCheckCommandWithServerConfig(serverConfig, params)

		// Verify we get a shadow command
		shadowedCommand, isShadowedQuery := command.(*shadowedCheckQuery)
		require.True(t, isShadowedQuery, "Expected shadowedCheckQuery when shadow is enabled")

		// Set up main resolver expectations
		mainQueryCount := uint32(5)
		mainResponse := &graph.ResolveCheckResponse{
			Allowed: true,
			ResolutionMetadata: graph.ResolveCheckResponseMetadata{
				DatastoreQueryCount: mainQueryCount,
				CycleDetected:       false,
				Duration:            100 * time.Millisecond,
			},
		}

		// Set up shadow resolver expectations
		shadowQueryCount := uint32(7)
		shadowResponse := &graph.ResolveCheckResponse{
			Allowed: true, // Same result to test "match" case
			ResolutionMetadata: graph.ResolveCheckResponseMetadata{
				DatastoreQueryCount: shadowQueryCount,
				CycleDetected:       false,
				Duration:            80 * time.Millisecond,
			},
		}

		// Mock cache controller expectations
		mainSharedResources.CacheController.(*mockstorage.MockCacheController).
			EXPECT().DetermineInvalidationTime(gomock.Any(), storeID).
			Return(time.Time{}).
			Times(1)

		shadowSharedResources.CacheController.(*mockstorage.MockCacheController).
			EXPECT().DetermineInvalidationTime(gomock.Any(), storeID).
			Return(time.Time{}).
			Times(1)

		// Mock main resolver call
		mockCheckResolver.EXPECT().
			ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			Return(mainResponse, nil)

		// Mock shadow resolver call (happens in goroutine)
		mockShadowCheckResolver.EXPECT().
			ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			Return(shadowResponse, nil)

		// Expect the "shadow check match" log with all the correct fields
		mockLogger.EXPECT().InfoWithContext(
			gomock.Any(),
			gomock.Eq("shadow check match"),
			zap.String("resolver", "check"),
			zap.String("request", params.TupleKey.String()),
			zap.String("store_id", params.StoreID),
			zap.String("model_id", params.Typesys.GetAuthorizationModelID()),
			zap.String("function", ShadowCheckQueryFunction),
			gomock.Any(), // main_latency - we can't predict exact timing
			zap.Uint32("main_query_count", uint32(0)), // FIXME: mock metadata
			gomock.Any(), // shadow_latency - we can't predict exact timing
			zap.Uint32("shadow_query_count", uint32(0)), // FIXME: mock metadata
		).Times(1)

		// Execute the command
		ctx := context.Background()
		response, metadata, err := command.Execute(ctx)

		// Verify no error
		require.NoError(t, err)

		// Verify main response is returned (shadow runs in background)
		require.NotNil(t, response)
		require.True(t, response.GetAllowed())
		// TODO : find a way to mock the datastore query count so we can verify it's correctly set from the main resolver
		// require.Equal(t, mainQueryCount, response.GetResolutionMetadata().DatastoreQueryCount)

		// Verify metadata is from main query
		require.NotNil(t, metadata)

		// Wait for shadow query to complete
		shadowedCommand.wg.Wait()

		// Verify both resolvers were called (this is implicitly verified by the mock expectations above)
		// The test will fail if either resolver wasn't called due to gomock's strict checking
	})

	t.Run("ShadowCommand_DifferentResults_LogsDifference", func(t *testing.T) {
		// Create a test logger that we can inspect
		ctrl := gomock.NewController(t)
		mockLogger := mockstorage.NewMockLogger(ctrl)

		mainConfig := NewCheckCommandConfig(mockDatastore, mockCheckResolver)
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(true),
			WithShadowCheckQueryPct(100), // 100% to ensure shadow runs
			WithShadowCheckQueryTimeout(5*time.Second),
			WithShadowCheckQueryLogger(mockLogger),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))
		command := NewCheckCommandWithServerConfig(serverConfig, params)

		shadowedCommand, ok := command.(*shadowedCheckQuery)
		require.True(t, ok)

		// Set up different results - main returns true, shadow returns false
		mainResponse := &graph.ResolveCheckResponse{
			Allowed: true, // Main returns true
			ResolutionMetadata: graph.ResolveCheckResponseMetadata{
				DatastoreQueryCount: 3,
				CycleDetected:       false,
				Duration:            50 * time.Millisecond,
			},
		}

		shadowResponse := &graph.ResolveCheckResponse{
			Allowed: false, // Shadow returns false (difference!)
			ResolutionMetadata: graph.ResolveCheckResponseMetadata{
				DatastoreQueryCount: 4,
				CycleDetected:       false,
				Duration:            60 * time.Millisecond,
			},
		}

		// Mock resolver calls
		mockCheckResolver.EXPECT().
			ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			Return(mainResponse, nil)

		mockShadowCheckResolver.EXPECT().
			ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			Return(shadowResponse, nil)

		// Expect the "shadow check difference" log with all the correct fields
		mockLogger.EXPECT().InfoWithContext(
			gomock.Any(),
			gomock.Eq("shadow check difference"),
			zap.String("resolver", "check"),
			zap.String("request", params.TupleKey.String()),
			zap.String("store_id", params.StoreID),
			zap.String("model_id", params.Typesys.GetAuthorizationModelID()),
			zap.String("function", ShadowCheckQueryFunction),
			zap.Bool("main", true), // Bug: should be true (response.GetAllowed()) but uses shadowRes.GetAllowed()
			zap.Bool("main_cycle", false),
			gomock.Any(),                              // main_latency - we can't predict exact timing
			zap.Uint32("main_query_count", uint32(0)), // FIXME: mock metadata
			zap.Bool("shadow", false),
			zap.Bool("shadow_cycle", false),
			gomock.Any(),                                // shadow_latency - we can't predict exact timing
			zap.Uint32("shadow_query_count", uint32(0)), // FIXME: mock metadata
		).Times(1)

		// Execute
		response, _, err := command.Execute(context.Background())

		// Verify main response is returned
		require.NoError(t, err)
		require.True(t, response.GetAllowed()) // Main result

		// Wait for shadow to complete
		shadowedCommand.wg.Wait()
	})

	t.Run("ShadowCommand_ShadowTimeout_MainStillSucceeds", func(t *testing.T) {
		// Create a test logger that we can inspect
		ctrl := gomock.NewController(t)
		mockLogger := mockstorage.NewMockLogger(ctrl)

		mainConfig := NewCheckCommandConfig(mockDatastore, mockCheckResolver)
		shadowConfig := NewCheckCommandShadowConfig(
			NewCheckCommandConfig(mockShadowDatastore, mockShadowCheckResolver),
			WithShadowCheckQueryEnabled(true),
			WithShadowCheckQueryPct(100),
			WithShadowCheckQueryTimeout(10*time.Millisecond), // Very short timeout
			WithShadowCheckQueryLogger(mockLogger),
		)

		serverConfig := NewCheckCommandServerConfig(mainConfig, WithShadowCheckCommandConfig(shadowConfig))
		command := NewCheckCommandWithServerConfig(serverConfig, params)

		shadowedCommand, ok := command.(*shadowedCheckQuery)
		require.True(t, ok)

		mainResponse := &graph.ResolveCheckResponse{
			Allowed: true,
			ResolutionMetadata: graph.ResolveCheckResponseMetadata{
				DatastoreQueryCount: 2,
				CycleDetected:       false,
				Duration:            25 * time.Millisecond,
			},
		}

		// Main resolver should succeed quickly
		mockCheckResolver.EXPECT().
			ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			Return(mainResponse, nil)

		// Shadow resolver should be called but may timeout
		mockShadowCheckResolver.EXPECT().
			ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
				// Simulate slow shadow query that will timeout
				select {
				case <-time.After(100 * time.Millisecond):
					return &graph.ResolveCheckResponse{Allowed: true}, nil
				case <-ctx.Done():
					return nil, ctx.Err() // Should timeout
				}
			})

		// Expect the "panic recovered" log with all the correct fields
		mockLogger.EXPECT().WarnWithContext(
			gomock.Any(),
			gomock.Eq("shadow check errored"),
			zap.String("resolver", "check"),
			zap.String("request", params.TupleKey.String()),
			zap.String("store_id", params.StoreID),
			zap.String("model_id", params.Typesys.GetAuthorizationModelID()),
			zap.String("function", ShadowCheckQueryFunction),
			gomock.Any(), // error
		).Times(1)

		// Execute
		response, _, err := command.Execute(context.Background())

		// Main should succeed even if shadow times out
		require.NoError(t, err)
		require.True(t, response.GetAllowed())
		// require.Equal(t, uint32(2), response.GetResolutionMetadata().DatastoreQueryCount)

		// Wait for shadow to complete (or timeout)
		shadowedCommand.wg.Wait()
	})
}
