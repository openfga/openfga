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
	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/featureflags"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands/v2breaking"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
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
		s.shadowV2Check(context.Background(), req, mainRes, 10, 3, 5)

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
		require.Equal(t, uint32(3), fields["main_datastore_query_count"])
		require.Equal(t, uint64(5), fields["main_datastore_item_count"])
		require.NotNil(t, fields["shadow_datastore_query_count"])
		require.NotNil(t, fields["shadow_datastore_item_count"])
		require.Equal(t, "document:1", fields["object"])
		require.Equal(t, "viewer", fields["relation"])
		require.Equal(t, "user:alice", fields["user"])
		// zap.Any encodes nil proto messages as the string "<nil>" rather than omitting the key.
		require.Equal(t, "<nil>", fields["context"], "context should be nil when not set on request")
		require.Equal(t, "<nil>", fields["contextual_tuples"], "contextual_tuples should be nil when not set on request")
	})

	t.Run("logs_mismatch_when_results_disagree", func(t *testing.T) {
		logs.TakeAll()

		// main says allowed=false, but alice IS a viewer, so shadow will say true
		mainRes := &openfgav1.CheckResponse{Allowed: false}
		s.shadowV2Check(context.Background(), req, mainRes, 20, 0, 0)

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
		require.Equal(t, "document:1", fields["object"])
		require.Equal(t, "viewer", fields["relation"])
		require.Equal(t, "user:alice", fields["user"])
	})

	t.Run("logs_contextual_tuples_when_present", func(t *testing.T) {
		logs.TakeAll()

		reqWithCtxTuples := &openfgav1.CheckRequest{
			StoreId:              req.GetStoreId(),
			AuthorizationModelId: req.GetAuthorizationModelId(),
			TupleKey:             req.GetTupleKey(),
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:bob"),
				},
			},
		}

		mainRes := &openfgav1.CheckResponse{Allowed: true}
		s.shadowV2Check(context.Background(), reqWithCtxTuples, mainRes, 10, 1, 1)

		shadowLogs := logs.FilterMessage("shadow check")
		require.Equal(t, 1, shadowLogs.Len())

		fields := fieldMap(shadowLogs.All()[0].Context)
		require.NotNil(t, fields["contextual_tuples"], "contextual_tuples should be logged when present")
		require.Equal(t, "<nil>", fields["context"], "context should be nil when not set on request")
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

		s.shadowV2Check(context.Background(), req, mainRes, 5, 0, 0)

		// Should log an error, not a "shadow check" info log
		shadowInfoLogs := logs.FilterMessage("shadow check")
		require.Equal(t, 0, shadowInfoLogs.Len())

		errorLogs := logs.FilterMessage("shadow v2 check failed")
		require.Equal(t, 1, errorLogs.Len())
		require.Equal(t, zapcore.ErrorLevel, errorLogs.All()[0].Level)
	})
}

func TestCheck_ShadowV2CheckGoroutine(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	core, logs := observer.New(zap.DebugLevel)
	testLogger := &logger.ZapLogger{Logger: zap.New(core)}

	s, req := setupCheckServer(t, "", nil,
		WithLogger(testLogger),
		WithShadowCheckResolverTimeout(5*time.Second),
		WithFeatureFlagClient(featureflags.NewDefaultClient([]string{serverconfig.ExperimentalShadowWeightedGraphCheck})),
	)

	resp, err := s.Check(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.GetAllowed())

	// The shadow check runs in a goroutine; wait for the log to appear.
	require.Eventually(t, func() bool {
		return logs.FilterMessage("shadow check").Len() == 1
	}, 2*time.Second, 10*time.Millisecond)

	entry := logs.FilterMessage("shadow check").All()[0]
	require.Equal(t, zapcore.InfoLevel, entry.Level)

	fields := fieldMap(entry.Context)
	require.Equal(t, true, fields["matches"])
	require.Equal(t, true, fields["main_result"])
	require.Equal(t, true, fields["shadow_result"])
	require.NotEmpty(t, fields["store_id"])
	require.NotNil(t, fields["main_datastore_query_count"])
	require.NotNil(t, fields["shadow_datastore_query_count"])
	require.NotNil(t, fields["main_datastore_item_count"])
	require.NotNil(t, fields["shadow_datastore_item_count"])
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
	entries map[keys.Key]any
	getKeys []string
	setKeys []string
	delKeys []string
}

// subproblemCachePrefix is the hex-encoded TLV prefix every per-edge subproblem
// cache key starts with. ModelGraphCachePrefix is the equivalent for entries
// the authorization-model-graph resolver writes. Tests use them to partition
// recorded cache keys by what they cache.
var (
	subproblemCachePrefix = tlvHexPrefix(check.PrefixEdgeCacheKey)
	modelGraphCachePrefix = tlvHexPrefix(modelgraph.CacheKeyPrefix)
)

func tlvHexPrefix(s string) string {
	var b keys.Builder
	b.EncodeString(s)
	return b.Key().String()
}

func newRecordingCache() *recordingCache {
	return &recordingCache{entries: make(map[keys.Key]any)}
}

func (c *recordingCache) Get(key keys.Key) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getKeys = append(c.getKeys, key.String())
	return c.entries[key]
}

func (c *recordingCache) Set(key keys.Key, value any, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setKeys = append(c.setKeys, key.String())
	c.entries[key] = value
}

func (c *recordingCache) Delete(key keys.Key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delKeys = append(c.delKeys, key.String())
	delete(c.entries, key)
}

func (c *recordingCache) Stop() {}

func (c *recordingCache) getKeysWithPrefix(prefix string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []string
	for _, k := range c.getKeys {
		if strings.HasPrefix(k, prefix) {
			result = append(result, k)
		}
	}
	return result
}

func (c *recordingCache) setKeysWithPrefix(prefix string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []string
	for _, k := range c.setKeys {
		if strings.HasPrefix(k, prefix) {
			result = append(result, k)
		}
	}
	return result
}

func (c *recordingCache) resetTracking() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getKeys = nil
	c.setKeys = nil
	c.delKeys = nil
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
		require.NotEmpty(t, shadowCache.setKeysWithPrefix(modelGraphCachePrefix), "shadow cache should have model graph entries")
		require.NotEmpty(t, shadowCache.setKeysWithPrefix(subproblemCachePrefix), "shadow cache should have subproblem cache entries")

		// Main cache should remain empty.
		require.Empty(t, checkCache.setKeysWithPrefix(modelGraphCachePrefix), "main check cache should not have model graph entries")
		require.Empty(t, checkCache.setKeysWithPrefix(subproblemCachePrefix), "main check cache should not have subproblem cache entries")
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
		require.NotEmpty(t, checkCache.setKeysWithPrefix(modelGraphCachePrefix), "main check cache should have model graph entries")
		require.NotEmpty(t, checkCache.setKeysWithPrefix(subproblemCachePrefix), "main check cache should have subproblem cache entries")

		// Shadow cache should remain empty.
		require.Empty(t, shadowCache.setKeysWithPrefix(modelGraphCachePrefix), "shadow cache should not have model graph entries")
		require.Empty(t, shadowCache.setKeysWithPrefix(subproblemCachePrefix), "shadow cache should not have subproblem cache entries")
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
		s, req := setupCheckServer(t, "", nil,
			WithFeatureFlagClient(featureflags.NewDefaultClient([]string{serverconfig.ExperimentalWeightedGraphCheck})),
		)

		ctx := grpc_ctxtags.SetInContext(context.Background(), grpc_ctxtags.NewTags())

		res, err := s.Check(ctx, req)
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

		// v2 does not track dispatch count, so the tag must be absent.
		_, ok = tags[dispatchCountHistogramName]
		require.False(t, ok, "dispatch_count should not be set for v2 path (no dispatcher)")
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

		res, err := s.Check(ctx, req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())

		tags := grpc_ctxtags.Extract(ctx).Values()

		throttled, ok := tags["request.datastore_throttled"]
		require.True(t, ok, "request.datastore_throttled should be set in context tags")
		require.Equal(t, true, throttled, "throttling should be enabled when feature flag is true and threshold exceeded")
	})

	t.Run("throttling_disabled_when_flag_off", func(t *testing.T) {
		s, req := setupCheckServer(t, "", nil,
			// Enable v2 but NOT datastore throttling, to verify throttling is disabled.
			WithFeatureFlagClient(featureflags.NewDefaultClient([]string{serverconfig.ExperimentalWeightedGraphCheck})),
			WithCheckDatabaseThrottle(1, 1*time.Millisecond),
		)

		ctx := grpc_ctxtags.SetInContext(context.Background(), grpc_ctxtags.NewTags())

		res, err := s.Check(ctx, req)
		require.NoError(t, err)
		require.True(t, res.GetAllowed())

		tags := grpc_ctxtags.Extract(ctx).Values()

		throttled, ok := tags["request.datastore_throttled"]
		require.True(t, ok, "request.datastore_throttled should be set in context tags")
		require.Equal(t, false, throttled, "throttling should be disabled when feature flag is false")
	})

	t.Run("cancelled_context_returns_error_not_fallback", func(t *testing.T) {
		// When the caller's context is already cancelled, v2Check returns context.Canceled.
		// Check must return that error immediately (not fall back to v1) and still emit metrics.
		// The error surfaces as the gRPC-mapped ErrRequestCancelled, not the raw sentinel.
		s, req := setupCheckServer(t, "", nil,
			WithFeatureFlagClient(featureflags.NewDefaultClient([]string{serverconfig.ExperimentalWeightedGraphCheck})),
		)

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel() // cancel before the call
		ctx := grpc_ctxtags.SetInContext(cancelledCtx, grpc_ctxtags.NewTags())

		_, err := s.Check(ctx, req)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, "error should be a gRPC status error")
		require.Equal(t, "Request Cancelled", st.Message())

		tags := grpc_ctxtags.Extract(ctx).Values()
		// Metrics must be set even for the error path.
		_, ok = tags[datastoreQueryCountHistogramName]
		require.True(t, ok, "datastore_query_count should be set even on cancelled context")
		_, ok = tags["request.datastore_throttled"]
		require.True(t, ok, "request.datastore_throttled should be set even on cancelled context")
		// v1 dispatch tag must be absent — we didn't fall back.
		_, ok = tags[dispatchCountHistogramName]
		require.False(t, ok, "dispatch_count must not be set — cancelled context must not fall back to v1")
	})
}

func TestV2Check_SanitizeRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	modelDSL := `
		model
			schema 1.1
		type user
		type document
			relations
				define view: [user]
	`

	s, baseReq := setupCheckServer(t, modelDSL, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "view", "user:bob"),
	})
	ctx := context.Background()

	doV2Check := func(t *testing.T, req *openfgav1.CheckRequest) error {
		t.Helper()
		_, err := s.v2Check(ctx, req,
			s.sharedDatastoreResources.CheckCache,
			s.sharedDatastoreResources.CacheController,
			s.authzModelGraphResolver,
		)
		return err
	}

	// Helper to clone the base request with a clean tuple key.
	freshReq := func() *openfgav1.CheckRequest {
		return &openfgav1.CheckRequest{
			StoreId:              baseReq.GetStoreId(),
			AuthorizationModelId: baseReq.GetAuthorizationModelId(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				Object:   "document:1",
				Relation: "view",
				User:     "user:alice",
			},
		}
	}

	t.Run("rejects_null_byte_in_tuple_key_object", func(t *testing.T) {
		req := freshReq()
		req.TupleKey.Object = "document:1\x00"
		require.Error(t, doV2Check(t, req))
	})

	t.Run("rejects_null_byte_in_tuple_key_relation", func(t *testing.T) {
		req := freshReq()
		req.TupleKey.Relation = "vi\x00ew"
		require.Error(t, doV2Check(t, req))
	})

	t.Run("rejects_null_byte_in_tuple_key_user", func(t *testing.T) {
		req := freshReq()
		req.TupleKey.User = "user:ali\x00ce"
		require.Error(t, doV2Check(t, req))
	})

	t.Run("rejects_null_byte_in_contextual_tuple_object", func(t *testing.T) {
		req := freshReq()
		req.ContextualTuples = &openfgav1.ContextualTupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:\x001", "view", "user:bob"),
			},
		}
		require.Error(t, doV2Check(t, req))
	})

	t.Run("rejects_null_byte_in_contextual_tuple_relation", func(t *testing.T) {
		req := freshReq()
		req.ContextualTuples = &openfgav1.ContextualTupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "v\x00iew", "user:bob"),
			},
		}
		require.Error(t, doV2Check(t, req))
	})

	t.Run("rejects_null_byte_in_contextual_tuple_user", func(t *testing.T) {
		req := freshReq()
		req.ContextualTuples = &openfgav1.ContextualTupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "view", "user:b\x00ob"),
			},
		}
		require.Error(t, doV2Check(t, req))
	})

	t.Run("accepts_valid_request_without_contextual_tuples", func(t *testing.T) {
		req := freshReq()
		require.NoError(t, doV2Check(t, req))
	})

	t.Run("accepts_valid_request_with_contextual_tuples", func(t *testing.T) {
		req := freshReq()
		req.ContextualTuples = &openfgav1.ContextualTupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "view", "user:bob"),
			},
		}
		require.NoError(t, doV2Check(t, req))
	})
}

func TestV2CheckQueryCacheEnabled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("caches_subproblem_results_when_enabled", func(t *testing.T) {
		s, req := setupCheckServer(t, "", nil,
			WithCheckQueryCacheEnabled(true),
			WithCheckCacheLimit(10),
			WithCheckQueryCacheTTL(1*time.Minute),
		)

		checkCache := newRecordingCache()
		s.sharedDatastoreResources.CheckCache.Stop()
		s.sharedDatastoreResources.CheckCache = checkCache
		s.authzModelGraphResolver = modelgraph.NewResolver(s.datastore, checkCache, 24*7*time.Hour)

		ctx := context.Background()
		res, err := s.v2Check(ctx, req,
			s.sharedDatastoreResources.CheckCache,
			s.sharedDatastoreResources.CacheController,
			s.authzModelGraphResolver,
		)
		require.NoError(t, err)
		require.True(t, res.Allowed)

		require.NotEmpty(t, checkCache.setKeysWithPrefix(subproblemCachePrefix), "cache should have subproblem entries written when query cache is enabled")

		// Reset tracking so the second call's Get activity is isolated.
		checkCache.resetTracking()

		// Call v2Check again with the same request to verify cached entries are retrieved.
		res, err = s.v2Check(ctx, req,
			s.sharedDatastoreResources.CheckCache,
			s.sharedDatastoreResources.CacheController,
			s.authzModelGraphResolver,
		)
		require.NoError(t, err)
		require.True(t, res.Allowed)
		require.NotEmpty(t, checkCache.getKeysWithPrefix(subproblemCachePrefix),
			"second check should read subproblem entries from cache")
		require.Empty(t, checkCache.setKeysWithPrefix(subproblemCachePrefix),
			"second check should not write new subproblem entries (cache should be hit)")
	})

	t.Run("skips_subproblem_caching_when_disabled", func(t *testing.T) {
		s, req := setupCheckServer(t, "", nil,
			WithCheckQueryCacheEnabled(false),
			WithCheckCacheLimit(10),
			WithCheckQueryCacheTTL(1*time.Minute),
		)

		checkCache := newRecordingCache()
		s.sharedDatastoreResources.CheckCache = checkCache
		s.authzModelGraphResolver = modelgraph.NewResolver(s.datastore, checkCache, 24*7*time.Hour)

		ctx := context.Background()
		res, err := s.v2Check(ctx, req,
			s.sharedDatastoreResources.CheckCache,
			s.sharedDatastoreResources.CacheController,
			s.authzModelGraphResolver,
		)
		require.NoError(t, err)
		require.True(t, res.Allowed)

		require.Empty(t, checkCache.setKeysWithPrefix(subproblemCachePrefix), "cache should have no subproblem entries when query cache is disabled")
	})
}

func TestCheck_FallsBackToV1WhenWeightedGraphInvalid(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// An intersection whose branches resolve to different terminal types (user vs bot).
	// WriteAuthorizationModel accepts this because v1 validation doesn't enforce that all
	// intersection branches reach the same terminal type, but the weighted graph builder
	// returns ErrInvalidModel for it. Check must fall back to v1 instead of surfacing the error.
	modelDSL := `
		model
			schema 1.1
		type user
		type bot
		type document
			relations
				define user_access: [user]
				define bot_access: [bot]
				define viewer: user_access and bot_access
	`

	s, req := setupCheckServer(t, modelDSL,
		[]*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "user_access", "user:alice"),
		},
		WithFeatureFlagClient(featureflags.NewDefaultClient([]string{serverconfig.ExperimentalWeightedGraphCheck})),
	)

	ctx := grpc_ctxtags.SetInContext(context.Background(), grpc_ctxtags.NewTags())
	resp, err := s.Check(ctx, req)
	require.NoError(t, err)
	// v1 fallback: alice has user_access but not bot_access, so viewer (AND) is false.
	require.False(t, resp.GetAllowed())

	tags := grpc_ctxtags.Extract(ctx).Values()
	// dispatch_count is only set by the v1 metrics path, confirming fallback ran v1.
	_, ok := tags[dispatchCountHistogramName]
	require.True(t, ok, "dispatch_count should be set, confirming v1 metrics path ran")
	_, ok = tags[datastoreQueryCountHistogramName]
	require.True(t, ok, "datastore_query_count should be set")
}

func TestCheck_DoesNotFallBackOnInvalidContextualTuple(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// A contextual tuple with a relation that doesn't exist on the model is rejected by v2's
	// request-validation step. v1 would reject it identically, so we must not fall back.
	s, baseReq := setupCheckServer(t, "", nil,
		WithFeatureFlagClient(featureflags.NewDefaultClient([]string{serverconfig.ExperimentalWeightedGraphCheck})),
	)

	req := &openfgav1.CheckRequest{
		StoreId:              baseReq.GetStoreId(),
		AuthorizationModelId: baseReq.GetAuthorizationModelId(),
		TupleKey:             baseReq.GetTupleKey(),
		ContextualTuples: &openfgav1.ContextualTupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "no_such_relation", "user:bob"),
			},
		},
	}

	ctx := grpc_ctxtags.SetInContext(context.Background(), grpc_ctxtags.NewTags())
	_, err := s.Check(ctx, req)
	require.Error(t, err)

	tags := grpc_ctxtags.Extract(ctx).Values()
	// v2 metrics must be present; v1 dispatch_count must be absent (no fallback).
	_, ok := tags[datastoreQueryCountHistogramName]
	require.True(t, ok, "datastore_query_count should be set on v2 terminal error")
	_, ok = tags[dispatchCountHistogramName]
	require.False(t, ok, "dispatch_count must not be set — invalid contextual tuple must not fall back to v1")
}

func TestBreakingChangeReason(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name      string
		modelDSL  string
		seedTuple *openfgav1.TupleKey
		object    string
		relation  string
		user      string
	}{
		{
			name: "alias_userset",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define reader: [user]
						define allowed: reader
						define viewer: [user, document#allowed]
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "reader", "user:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "document:d3#reader",
		},
		{
			name: "self_referential_userset",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "viewer", "user:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "document:d1#viewer",
		},
		{
			name: "computed_userset_self_object",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define writer: [user]
						define viewer: editor or writer
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "editor", "user:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "document:d1#writer",
		},
		{
			name: "ttu_userset",
			modelDSL: `
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
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "parent", "folder:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "folder:f2#viewer",
		},
		{
			name: "no_match_direct_userset_assignable",
			modelDSL: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [user, group#member]
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "viewer", "user:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "group:g1#member",
		},
		{
			// User is a plain object (no #relation), so none of the userset-shape
			// reasons should fire. Mirrors the IsObjectRelation gate at the caller.
			name: "no_match_user_is_not_userset",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "viewer", "user:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "user:bob",
		},
		{
			// computed_userset shape exists in the rewrite, but the user's object
			// differs from the target object — so computed_userset_self_object must NOT fire.
			name: "no_match_computed_userset_different_object",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define writer: [user]
						define viewer: editor or writer
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "editor", "user:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "document:d2#writer",
		},
		{
			// TTU shape exists (viewer from parent) and the computed relation matches
			// the user's relation, but the user's object type (user) is not in the
			// tupleset's directly-related types (parent: [folder]) — so ttu_userset must NOT fire.
			name: "no_match_ttu_user_object_type_not_in_tupleset",
			modelDSL: `
				model
					schema 1.1
				type user
					relations
						define viewer: [user]
				type folder
					relations
						define viewer: [user]
				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "parent", "folder:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "user:u1#viewer",
		},
		{
			// self_referential_userset is an exact (object, relation) match shape.
			// User's object differs from target's object, so it must NOT fire.
			name: "no_match_self_referential_different_object",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user, document#viewer]
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "viewer", "user:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "document:d2#viewer",
		},
		{
			// Same object as target, but user's relation is not a ComputedUserset leaf
			// in the rewrite. computed_userset_self_object must NOT fire.
			name: "no_match_computed_userset_relation_not_in_rewrite",
			modelDSL: `
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define writer: [user]
						define other: [user]
						define viewer: editor or writer
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "editor", "user:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "document:d1#other",
		},
		{
			// TTU exists (viewer from parent) but the user's relation does not match
			// the TTU's computed relation. ttu_userset must NOT fire.
			name: "no_match_ttu_user_relation_mismatch",
			modelDSL: `
				model
					schema 1.1
				type user
				type folder
					relations
						define viewer: [user]
						define editor: [user]
				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent
			`,
			seedTuple: tuple.NewTupleKey("document:seed", "parent", "folder:seed"),
			object:    "document:d1",
			relation:  "viewer",
			user:      "folder:f2#editor",
		},
	}

	negativeCases := map[string]bool{
		"no_match_direct_userset_assignable":                true,
		"no_match_user_is_not_userset":                      true,
		"no_match_computed_userset_different_object":        true,
		"no_match_ttu_user_object_type_not_in_tupleset":     true,
		"no_match_self_referential_different_object":        true,
		"no_match_computed_userset_relation_not_in_rewrite": true,
		"no_match_ttu_user_relation_mismatch":               true,
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, baseReq := setupCheckServer(t, tc.modelDSL, []*openfgav1.TupleKey{tc.seedTuple})

			req := &openfgav1.CheckRequest{
				StoreId:              baseReq.GetStoreId(),
				AuthorizationModelId: baseReq.GetAuthorizationModelId(),
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object:   tc.object,
					Relation: tc.relation,
					User:     tc.user,
				},
			}

			typesys, err := s.resolveTypesystem(context.Background(), baseReq.GetStoreId(), req.GetAuthorizationModelId())
			require.NoError(t, err)
			tk := req.GetTupleKey()
			got := v2breaking.CheckReason(typesys, tk)
			if negativeCases[tc.name] {
				require.Empty(t, got, "expected no breaking change reason")
				return
			}
			require.Equal(t, tc.name, got)
		})
	}
}
