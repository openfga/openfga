package commands

import (
	"context"
	"errors"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
)

const ListObjectsShadowExecute = "ShadowedListObjectsQuery.Execute"

type shadowedListObjectsQuery struct {
	main          ListObjectsResolver
	shadow        ListObjectsResolver
	shadowPct     int           // An integer representing the shadowPct of list_objects requests that will also trigger the shadow query. This allows for controlled rollout and data collection without impacting all requests. Value should be between 0 and 100.
	shadowTimeout time.Duration // A time.Duration specifying the maximum amount of time to wait for the shadow list_objects query to complete. If the shadow query exceeds this shadowTimeout, it will be cancelled, and its result will be ignored, but the shadowTimeout event will be logged.
	maxDeltaItems int           // The maximum number of items to log in the delta between the main and shadow results. This prevents excessive logging in case of large differences.
	logger        logger.Logger
	// only used for testing signals
	wg *sync.WaitGroup
}

type ShadowListObjectsQueryOption func(d *ShadowListObjectsQueryConfig)

// WithShadowListObjectsQueryEnabled sets whether the shadow list_objects query should use optimizations.
func WithShadowListObjectsQueryEnabled(enabled bool) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.shadowEnabled = enabled
	}
}

// WithShadowListObjectsQuerySamplePercentage sets the shadowPct of list_objects requests that will trigger the shadow query.
func WithShadowListObjectsQuerySamplePercentage(samplePercentage int) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.shadowPct = samplePercentage
	}
}

// WithShadowListObjectsQueryTimeout sets the shadowTimeout for the shadow list_objects query.
func WithShadowListObjectsQueryTimeout(timeout time.Duration) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.shadowTimeout = timeout
	}
}

func WithShadowListObjectsQueryLogger(logger logger.Logger) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.logger = logger
	}
}

func WithShadowListObjectsQueryMaxDeltaItems(maxDeltaItems int) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.maxDeltaItems = maxDeltaItems
	}
}

// WithShadowListObjectsQueryOptions appends ListObjectsQueryOptions to override the default options for the shadow list_objects query.
func WithShadowListObjectsQueryOptions(opts ...ListObjectsQueryOption) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.opts = append(c.opts, opts...)
	}
}

type ShadowListObjectsQueryConfig struct {
	shadowEnabled bool          // A boolean flag to globally enable or disable the shadow mode for list_objects queries. When false, the shadow query will not be executed.
	shadowPct     int           // An integer representing the shadowPct of list_objects requests that will also trigger the shadow query. This allows for controlled rollout and data collection without impacting all requests. Value should be between 0 and 100.
	shadowTimeout time.Duration // A time.Duration specifying the maximum amount of time to wait for the shadow list_objects query to complete. If the shadow query exceeds this shadowTimeout, it will be cancelled, and its result will be ignored, but the shadowTimeout event will be logged.
	maxDeltaItems int           // The maximum number of items to log in the delta between the main and shadow results. This prevents excessive logging in case of large differences.
	logger        logger.Logger
	opts          []ListObjectsQueryOption
}

func NewShadowListObjectsQueryConfig(opts ...ShadowListObjectsQueryOption) *ShadowListObjectsQueryConfig {
	result := &ShadowListObjectsQueryConfig{
		shadowEnabled: false,                  // Disabled by default
		shadowPct:     0,                      // Default to 0% to disable shadow mode
		shadowTimeout: 1 * time.Second,        // Default shadowTimeout for shadow queries
		logger:        logger.NewNoopLogger(), // Default to a noop logger
		maxDeltaItems: 100,                    // Default max delta items to log
	}
	for _, opt := range opts {
		opt(result)
	}
	return result
}

// NewListObjectsQueryWithShadowConfig creates a new ListObjectsResolver that can run in shadow mode based on the provided ShadowListObjectsQueryConfig.
func NewListObjectsQueryWithShadowConfig(
	ds storage.RelationshipTupleReader,
	checkResolver graph.CheckResolver,
	checkCommandServerConfig CheckCommandServerConfig,
	shadowConfig *ShadowListObjectsQueryConfig,
	opts ...ListObjectsQueryOption,
) (ListObjectsResolver, error) {
	if shadowConfig != nil && shadowConfig.shadowEnabled {
		return newShadowedListObjectsQuery(ds, checkResolver, checkCommandServerConfig, shadowConfig, opts...)
	}

	return NewListObjectsQuery(ds, checkResolver, checkCommandServerConfig, opts...)
}

// newShadowedListObjectsQuery creates a new ListObjectsResolver that runs two queries in parallel: one with optimizations and one without.
func newShadowedListObjectsQuery(
	ds storage.RelationshipTupleReader,
	checkResolver graph.CheckResolver,
	checkCommandServerConfig CheckCommandServerConfig,
	shadowConfig *ShadowListObjectsQueryConfig,
	opts ...ListObjectsQueryOption,
) (ListObjectsResolver, error) {
	if shadowConfig == nil {
		return nil, errors.New("shadowConfig must be set")
	}
	standard, err := NewListObjectsQuery(ds, checkResolver, checkCommandServerConfig,
		// force disable optimizations
		slices.Concat(opts, []ListObjectsQueryOption{WithListObjectsOptimizationsEnabled(false)})...,
	)
	if err != nil {
		return nil, err
	}
	shadowOptions := slices.Concat(opts,
		shadowConfig.opts, // override with any options using shadow config
		[]ListObjectsQueryOption{
			// force enable optimizations
			WithListObjectsOptimizationsEnabled(true),
		})

	optimized, err := NewListObjectsQuery(ds, checkResolver, checkCommandServerConfig, shadowOptions...)
	if err != nil {
		return nil, err
	}

	result := &shadowedListObjectsQuery{
		main:          standard,
		shadow:        optimized,
		shadowPct:     shadowConfig.shadowPct,
		shadowTimeout: shadowConfig.shadowTimeout,
		logger:        shadowConfig.logger,
		maxDeltaItems: shadowConfig.maxDeltaItems,
		wg:            &sync.WaitGroup{}, // only used for testing signals
	}

	return result, nil
}

func (q *shadowedListObjectsQuery) Execute(
	ctx context.Context,
	req *openfgav1.ListObjectsRequest,
) (*ListObjectsResponse, error) {
	cloneCtx := context.WithoutCancel(ctx) // needs typesystem and datastore etc

	startTime := time.Now()
	res, err := q.main.Execute(ctx, req)
	if err != nil {
		return nil, err
	}
	latency := time.Since(startTime)

	// If shadow mode is not shadowEnabled, just execute the main query
	if q.checkShadowModePreconditions(cloneCtx, req, res, latency) {
		q.wg.Add(1) // only used for testing signals
		go func() {
			startTime = time.Now()
			defer func() {
				defer q.wg.Done() // only used for testing signals
				if r := recover(); r != nil {
					q.logger.ErrorWithContext(cloneCtx, "panic recovered",
						loShadowLogFields(req,
							zap.Duration("main_latency", latency),
							zap.Duration("shadow_latency", time.Since(startTime)),
							zap.Int("main_result_count", len(res.Objects)),
							zap.Any("error", r),
						)...,
					)
				}
			}()

			q.executeShadowModeAndCompareResults(cloneCtx, req, res, latency)
		}()
	}
	return res, err
}

func (q *shadowedListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
	return q.main.ExecuteStreamed(ctx, req, srv)
}

func (q *shadowedListObjectsQuery) checkShadowModeSampleRate() bool {
	return rand.Intn(100) < q.shadowPct // randomly enable shadow mode
}

// executeShadowMode executes the main and shadow functions in parallel, returning the result of the main function if shadow mode is not shadowEnabled or if the shadow function fails.
// It compares the results of the main and shadow functions, logging any differences.
// If the shadow function takes longer than shadowTimeout, it will be cancelled, and its result will be ignored, but the shadowTimeout event will be logged.
// This function is designed to be run in a separate goroutine to avoid blocking the main execution flow.
func (q *shadowedListObjectsQuery) executeShadowModeAndCompareResults(parentCtx context.Context, req *openfgav1.ListObjectsRequest, mainResult *ListObjectsResponse, latency time.Duration) {
	shadowCtx, shadowCancel := context.WithTimeout(parentCtx, q.shadowTimeout)
	defer shadowCancel()

	startTime := time.Now()
	shadowRes, errShadow := q.shadow.Execute(shadowCtx, req)
	shadowLatency := time.Since(startTime)

	var mainQueryCount uint32
	var mainResultObjects []string
	if mainResult != nil {
		mainQueryCount = mainResult.ResolutionMetadata.DatastoreQueryCount.Load()
		mainResultObjects = mainResult.Objects
	}

	if errShadow != nil {
		q.logger.WarnWithContext(parentCtx, "shadowed list objects error",
			loShadowLogFields(req,
				zap.Duration("main_latency", latency),
				zap.Duration("shadow_latency", shadowLatency),
				zap.Int("main_result_count", len(mainResultObjects)),
				zap.Any("error", errShadow),
			)...,
		)
		return
	}

	var resultShadowed []string
	var shadowQueryCount uint32
	if shadowRes != nil {
		resultShadowed = shadowRes.Objects
		shadowQueryCount = shadowRes.ResolutionMetadata.DatastoreQueryCount.Load()
	}

	mapResultMain := keyMapFromSlice(mainResultObjects)
	mapResultShadow := keyMapFromSlice(resultShadowed)

	// compare sorted string arrays - sufficient for equality check
	if !maps.Equal(mapResultMain, mapResultShadow) {
		delta := calculateDelta(mapResultMain, mapResultShadow)
		totalDelta := len(delta)
		// Limit the delta to maxDeltaItems
		if totalDelta > q.maxDeltaItems {
			delta = delta[:q.maxDeltaItems]
		}
		// log the differences if the shadow query failed or if the results are not equal
		q.logger.WarnWithContext(parentCtx, "shadowed list objects result difference",
			loShadowLogFields(req,
				zap.Bool("is_match", false),
				zap.Duration("main_latency", latency),
				zap.Duration("shadow_latency", shadowLatency),
				zap.Int("main_result_count", len(mainResultObjects)),
				zap.Int("shadow_result_count", len(resultShadowed)),
				zap.Int("total_delta", totalDelta),
				zap.Any("delta", delta),
				zap.Uint32("main_datastore_query_count", mainQueryCount),
				zap.Uint32("shadow_datastore_query_count", shadowQueryCount),
			)...,
		)
	} else {
		q.logger.InfoWithContext(parentCtx, "shadowed list objects result matches",
			loShadowLogFields(req,
				zap.Bool("is_match", true),
				zap.Duration("main_latency", latency),
				zap.Duration("shadow_latency", shadowLatency),
				zap.Int("main_result_count", len(mainResultObjects)),
				zap.Uint32("main_datastore_query_count", mainQueryCount),
				zap.Uint32("shadow_datastore_query_count", shadowQueryCount),
			)...,
		)
	}
}

// checkShadowModePreconditions checks if the shadow mode preconditions are met:
//   - If the main result reaches the max result size, skip the shadow query.
//   - If the main query takes too long, skip the shadow query.
//   - If the shadow mode sample rate is not met, skip the shadow query.
func (q *shadowedListObjectsQuery) checkShadowModePreconditions(ctx context.Context, req *openfgav1.ListObjectsRequest, res *ListObjectsResponse, latency time.Duration) bool {
	if loq, ok := q.main.(*ListObjectsQuery); ok {
		// don't run if the main result reaches max result size q.main.listObjectsMaxResults
		// that means there are more results than the shadow query can return,
		// so it is impossible to compare the results
		if len(res.Objects) == int(loq.listObjectsMaxResults) {
			q.logger.DebugWithContext(ctx, "shadowed list objects query skipped due to max results reached",
				loShadowLogFields(req)...,
			)
			return false
		}

		if !res.ResolutionMetadata.ShouldRunShadowQuery.Load() {
			q.logger.DebugWithContext(ctx, "shadowed list objects query skipped due to infinite weight query",
				loShadowLogFields(req)...,
			)
			return false
		}

		// When a list_objects query takes a significant amount of time to complete (approaching its overall timeout),
		// it often indicates an exhaustive traversal or that it's processing a large dataset.
		// In such cases, running a parallel shadow query and comparing its results (which do not guarantee order)
		// against a potentially slow or truncated main query result is often meaningless and can lead to false negatives in correctness comparisons.
		// Therefore, we skip the shadow query if the main query is already close to its deadline.
		if latency > (loq.listObjectsDeadline - 100*time.Millisecond) {
			q.logger.DebugWithContext(ctx, "shadowed list objects query skipped due to high latency of the main query",
				loShadowLogFields(req, zap.Duration("latency", latency))...,
			)
			return false
		}
	}

	return q.checkShadowModeSampleRate()
}

func loShadowLogFields(req *openfgav1.ListObjectsRequest, fields ...zap.Field) []zap.Field {
	return append([]zap.Field{
		zap.String("func", ListObjectsShadowExecute),
		zap.Any("request", req),
		zap.String("store_id", req.GetStoreId()),
		zap.String("model_id", req.GetAuthorizationModelId()),
	}, fields...)
}

// keyMapFromSlice creates a map from a slice of strings, where each string is a key in the map.
func keyMapFromSlice(slice []string) map[string]struct{} {
	result := make(map[string]struct{}, len(slice))
	for _, item := range slice {
		result[item] = struct{}{}
	}
	return result
}

// calculateDelta calculates the delta between two maps of string keys.
func calculateDelta(mapResultMain map[string]struct{}, mapResultShadow map[string]struct{}) []string {
	delta := make([]string, 0, len(mapResultMain)+len(mapResultShadow))
	// Find objects in shadow but not in main
	for key := range mapResultMain {
		if _, exists := mapResultShadow[key]; !exists {
			delta = append(delta, "-"+key) // object in main but not in shadow
		}
	}
	for key := range mapResultShadow {
		if _, exists := mapResultMain[key]; !exists {
			delta = append(delta, "+"+key) // object in shadow but not in main
		}
	}
	// Sort the delta for consistent result
	slices.Sort(delta)
	return delta
}
