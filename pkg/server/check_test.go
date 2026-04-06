package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/featureflags"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestCheck_Validation(t *testing.T) {
	t.Parallel()

	testServer := &Server{featureFlagClient: featureflags.NewNoopFeatureFlagClient()}
	ctx := t.Context()

	tests := []struct {
		name                 string
		givenRequest         *openfgav1.CheckRequest
		expectedErrorMessage string
	}{
		{
			name:                 "missing store id",
			givenRequest:         &openfgav1.CheckRequest{},
			expectedErrorMessage: "invalid CheckRequest.StoreId: value does not match regex pattern \"^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$\"",
		},
		{
			name: "missing tuple_key",
			givenRequest: &openfgav1.CheckRequest{
				StoreId: "01K3RZVNE3NJ4FYKK99QN013G2",
			},
			expectedErrorMessage: "invalid CheckRequest.TupleKey: value is required",
		},
		{
			name: "missing object",
			givenRequest: &openfgav1.CheckRequest{
				StoreId: "01K3RZVNE3NJ4FYKK99QN013G2",
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:anne",
					Relation: "viewer",
				},
			},
			expectedErrorMessage: "invalid CheckRequest.TupleKey: embedded message failed validation | caused by: invalid CheckRequestTupleKey.Object: value does not match regex pattern \"^[^\\\\s]{2,256}$\"",
		},
		{
			name: "missing relation",
			givenRequest: &openfgav1.CheckRequest{
				StoreId: "01K3RZVNE3NJ4FYKK99QN013G2",
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object: "doc:1",
					User:   "user:anne",
				},
			},
			expectedErrorMessage: "invalid CheckRequest.TupleKey: embedded message failed validation | caused by: invalid CheckRequestTupleKey.Relation: value does not match regex pattern \"^[^:#@\\\\s]{1,50}$\"",
		},
		{
			name: "missing user",
			givenRequest: &openfgav1.CheckRequest{
				StoreId: "01K3RZVNE3NJ4FYKK99QN013G2",
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object:   "doc:1",
					Relation: "viewer",
				},
			},
			expectedErrorMessage: "invalid CheckRequest.TupleKey: embedded message failed validation | caused by: invalid CheckRequestTupleKey.User: value does not match regex pattern \"^[^\\\\s]{2,512}$\"",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			actualResponse, err := testServer.Check(ctx, test.givenRequest)

			gRPCStatus, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.InvalidArgument, gRPCStatus.Code())
			require.Equal(t, test.expectedErrorMessage, gRPCStatus.Message())
			require.Nil(t, actualResponse)
		})
	}
}

// setupCheckServer creates a server with a model and tuples for Check tests.
// If modelDSL is empty, a default model with document#viewer:[user] is used.
// If tuples is empty, a default tuple document:1#viewer@user:alice is written.
// Returns the server and a check request for document:1#viewer@user:alice.
func setupCheckServer(t *testing.T, modelDSL string, tuples []*openfgav1.TupleKey, opts ...OpenFGAServiceV1Option) (*Server, *openfgav1.CheckRequest) {
	t.Helper()

	if modelDSL == "" {
		modelDSL = `
			model
				schema 1.1
			type user
			type document
				relations
					define viewer: [user]
		`
	}

	if len(tuples) == 0 {
		tuples = []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:alice"),
		}
	}

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	defaultOpts := []OpenFGAServiceV1Option{
		WithDatastore(ds),
	}
	defaultOpts = append(defaultOpts, opts...)

	s := MustNewServerWithOpts(defaultOpts...)
	t.Cleanup(s.Close)

	ctx := context.Background()

	createStoreResp, err := s.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "v2check-test",
	})
	require.NoError(t, err)
	storeID := createStoreResp.GetId()

	model := testutils.MustTransformDSLToProtoWithID(modelDSL)

	writeModelResp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: model.GetTypeDefinitions(),
	})
	require.NoError(t, err)
	modelID := writeModelResp.GetAuthorizationModelId()

	_, err = s.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: tuples,
		},
	})
	require.NoError(t, err)

	req := &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:alice",
		},
	}

	return s, req
}

func TestShadowV2Check(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	core, logs := observer.New(zap.DebugLevel)
	testLogger := &logger.ZapLogger{Logger: zap.New(core)}

	s, req := setupCheckServer(t, "", nil,
		WithLogger(testLogger),
		WithShadowCheckResolverTimeout(5*time.Second),
	)

	t.Run("logs_match_when_results_agree", func(t *testing.T) {
		logs.TakeAll() // clear previous logs

		mainRes := &openfgav1.CheckResponse{Allowed: true}
		s.shadowV2Check(context.Background(), req, mainRes, 10)

		shadowLogs := logs.FilterMessage("shadow check")
		require.Equal(t, 1, shadowLogs.Len())

		entry := shadowLogs.All()[0]
		require.Equal(t, zapcore.InfoLevel, entry.Level)

		fields := fieldMap(entry.Context)
		require.Equal(t, true, fields["matches"])
		require.Equal(t, true, fields["main_result"])
		require.Equal(t, true, fields["shadow_result"])
		require.Equal(t, int64(10), fields["main_took"])
		shadowTook, ok := fields["shadow_took"].(int64)
		require.True(t, ok, "shadow_took should be int64 milliseconds")
		require.GreaterOrEqual(t, shadowTook, int64(0))
	})

	t.Run("logs_mismatch_when_results_disagree", func(t *testing.T) {
		logs.TakeAll()

		// main says allowed=false, but alice IS a viewer, so shadow will say true
		mainRes := &openfgav1.CheckResponse{Allowed: false}
		s.shadowV2Check(context.Background(), req, mainRes, 20)

		shadowLogs := logs.FilterMessage("shadow check")
		require.Equal(t, 1, shadowLogs.Len())

		entry := shadowLogs.All()[0]
		fields := fieldMap(entry.Context)
		require.Equal(t, false, fields["matches"])
		require.Equal(t, false, fields["main_result"])
		require.Equal(t, true, fields["shadow_result"])
		require.Equal(t, int64(20), fields["main_took"])
		shadowTook, ok := fields["shadow_took"].(int64)
		require.True(t, ok, "shadow_took should be int64 milliseconds")
		require.GreaterOrEqual(t, shadowTook, int64(0))
	})

	t.Run("logs_error_on_invalid_store", func(t *testing.T) {
		logs.TakeAll()

		mainRes := &openfgav1.CheckResponse{Allowed: false}
		req := &openfgav1.CheckRequest{
			StoreId:              "01K3RZVNE3NJ4FYKK99QN013G2", // non-existent store
			AuthorizationModelId: req.GetAuthorizationModelId(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
			},
		}

		s.shadowV2Check(context.Background(), req, mainRes, 5)

		// Should log an error, not a "shadow check" info log
		shadowInfoLogs := logs.FilterMessage("shadow check")
		require.Equal(t, 0, shadowInfoLogs.Len())

		errorLogs := logs.FilterMessage("shadow v2 check failed")
		require.Equal(t, 1, errorLogs.Len())
		require.Equal(t, zapcore.ErrorLevel, errorLogs.All()[0].Level)
	})
}

// fieldMap converts a slice of zap.Field into a map for easy lookup in assertions.
func fieldMap(fields []zap.Field) map[string]interface{} {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	return enc.Fields
}

// recordingCache implements storage.InMemoryCache[any] and records all Set/Get/Delete keys
// so tests can assert which cache received entries.
type recordingCache struct {
	mu      sync.Mutex
	entries map[string]any
}

func newRecordingCache() *recordingCache {
	return &recordingCache{entries: make(map[string]any)}
}

func (c *recordingCache) Get(key string) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[key]
}

func (c *recordingCache) Set(key string, value any, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = value
}

func (c *recordingCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

func (c *recordingCache) Stop() {}

func (c *recordingCache) keysWithPrefix(prefix string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var keys []string
	for k := range c.entries {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys
}

func TestV2CheckCacheSeparation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	s, req := setupCheckServer(t, "", nil,
		WithCheckQueryCacheEnabled(true),
		WithCheckCacheLimit(10),
		WithCheckQueryCacheTTL(1*time.Minute),
		WithCacheControllerEnabled(true),
	)
	ctx := context.Background()

	// Stop the original LRU caches created during server construction so their
	// background maintenance goroutines don't leak when we swap in recording caches.
	origCheckCache := s.sharedDatastoreResources.CheckCache
	origShadowCache := s.sharedDatastoreResources.ShadowCheckCache
	origCheckCache.Stop()
	if origShadowCache != origCheckCache {
		origShadowCache.Stop()
	}

	t.Run("shadow_mode_uses_shadow_cache", func(t *testing.T) {
		checkCache := newRecordingCache()
		shadowCache := newRecordingCache()

		// Replace caches on the server's shared resources.
		s.sharedDatastoreResources.CheckCache = checkCache
		s.sharedDatastoreResources.ShadowCheckCache = shadowCache

		// Re-create resolvers so they capture the new cache instances.
		s.authzModelGraphResolver = modelgraph.NewResolver(s.datastore, checkCache, 24*7*time.Hour)
		s.shadowAuthzModelGraphResolver = modelgraph.NewResolver(s.datastore, shadowCache, 24*7*time.Hour)

		_, err := s.v2Check(ctx, req,
			s.sharedDatastoreResources.ShadowCheckCache,
			s.sharedDatastoreResources.ShadowCacheController,
			s.shadowAuthzModelGraphResolver,
		)
		require.NoError(t, err)

		// Shadow mode should route model graph and subproblem entries to the shadow cache.
		require.NotEmpty(t, shadowCache.keysWithPrefix("wg|"), "shadow cache should have model graph entries")
		require.NotEmpty(t, shadowCache.keysWithPrefix("c."), "shadow cache should have subproblem cache entries")

		// Main cache should remain empty.
		require.Empty(t, checkCache.keysWithPrefix("wg|"), "main check cache should not have model graph entries")
		require.Empty(t, checkCache.keysWithPrefix("c."), "main check cache should not have subproblem cache entries")
	})

	t.Run("non_shadow_mode_uses_main_cache", func(t *testing.T) {
		checkCache := newRecordingCache()
		shadowCache := newRecordingCache()

		s.sharedDatastoreResources.CheckCache = checkCache
		s.sharedDatastoreResources.ShadowCheckCache = shadowCache

		s.authzModelGraphResolver = modelgraph.NewResolver(s.datastore, checkCache, 24*7*time.Hour)
		s.shadowAuthzModelGraphResolver = modelgraph.NewResolver(s.datastore, shadowCache, 24*7*time.Hour)

		_, err := s.v2Check(ctx, req,
			s.sharedDatastoreResources.CheckCache,
			s.sharedDatastoreResources.CacheController,
			s.authzModelGraphResolver,
		)
		require.NoError(t, err)

		// Non-shadow mode should route model graph and subproblem entries to the main cache.
		require.NotEmpty(t, checkCache.keysWithPrefix("wg|"), "main check cache should have model graph entries")
		require.NotEmpty(t, checkCache.keysWithPrefix("c."), "main check cache should have subproblem cache entries")

		// Shadow cache should remain empty.
		require.Empty(t, shadowCache.keysWithPrefix("wg|"), "shadow cache should not have model graph entries")
		require.Empty(t, shadowCache.keysWithPrefix("c."), "shadow cache should not have subproblem cache entries")
	})

	t.Run("cache_controller_instances_are_separate", func(t *testing.T) {
		// When caching and cache controller are both enabled, the shadow cache controller
		// should be a separate instance from the main cache controller.
		require.NotSame(t,
			s.sharedDatastoreResources.CacheController,
			s.sharedDatastoreResources.ShadowCacheController,
			"ShadowCacheController should be a different instance than CacheController",
		)
	})
}

func TestV2CheckMetadata(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("publishes_datastore_query_and_item_counts", func(t *testing.T) {
		s, req := setupCheckServer(t, "", nil)

		ctx := grpc_ctxtags.SetInContext(context.Background(), grpc_ctxtags.NewTags())

		res, err := s.v2Check(ctx, req,
			s.sharedDatastoreResources.CheckCache,
			s.sharedDatastoreResources.CacheController,
			s.authzModelGraphResolver,
		)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())

		tags := grpc_ctxtags.Extract(ctx).Values()

		queryCount, ok := tags[datastoreQueryCountHistogramName]
		require.True(t, ok, "datastoreQueryCount should be set in context tags")
		require.Greater(t, queryCount, float64(0), "datastoreQueryCount should be > 0")

		itemCount, ok := tags[datastoreItemCountHistogramName]
		require.True(t, ok, "datastoreItemCount should be set in context tags")
		require.Greater(t, itemCount, float64(0), "datastoreItemCount should be > 0")

		throttled, ok := tags["request.datastore_throttled"]
		require.True(t, ok, "request.datastore_throttled should be set in context tags")
		require.Equal(t, false, throttled, "throttling should be disabled by default")
	})

	t.Run("throttling_enabled_when_flag_on_and_threshold_exceeded", func(t *testing.T) {
		// Use an intersection model so the check requires multiple datastore reads,
		// exceeding the throttle threshold of 1. Intersection is used instead of union
		// because union short-circuits on the first allowed=true result, which can
		// nondeterministically reduce the number of reads depending on goroutine scheduling.
		// Intersection only short-circuits on false, so when both branches are true,
		// all reads are guaranteed to complete.
		s, req := setupCheckServer(t, `
			model
				schema 1.1
			type user
			type document
				relations
					define editor: [user]
					define viewer: [user] and editor
		`,
			[]*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				tuple.NewTupleKey("document:1", "editor", "user:alice"),
			},
			WithFeatureFlagClient(featureflags.NewHardcodedBooleanClient(true)),
			WithCheckDatabaseThrottle(1, 1*time.Millisecond),
		)

		ctx := grpc_ctxtags.SetInContext(context.Background(), grpc_ctxtags.NewTags())

		res, err := s.v2Check(ctx, req,
			s.sharedDatastoreResources.CheckCache,
			s.sharedDatastoreResources.CacheController,
			s.authzModelGraphResolver,
		)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())

		tags := grpc_ctxtags.Extract(ctx).Values()

		throttled, ok := tags["request.datastore_throttled"]
		require.True(t, ok, "request.datastore_throttled should be set in context tags")
		require.Equal(t, true, throttled, "throttling should be enabled when feature flag is true and threshold exceeded")
	})

	t.Run("throttling_disabled_when_flag_off", func(t *testing.T) {
		s, req := setupCheckServer(t, "", nil,
			WithFeatureFlagClient(featureflags.NewNoopFeatureFlagClient()),
			WithCheckDatabaseThrottle(1, 1*time.Millisecond),
		)

		ctx := grpc_ctxtags.SetInContext(context.Background(), grpc_ctxtags.NewTags())

		res, err := s.v2Check(ctx, req,
			s.sharedDatastoreResources.CheckCache,
			s.sharedDatastoreResources.CacheController,
			s.authzModelGraphResolver,
		)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())

		tags := grpc_ctxtags.Extract(ctx).Values()

		throttled, ok := tags["request.datastore_throttled"]
		require.True(t, ok, "request.datastore_throttled should be set in context tags")
		require.Equal(t, false, throttled, "throttling should be disabled when feature flag is false")
	})
}
