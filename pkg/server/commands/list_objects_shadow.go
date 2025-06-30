package commands

import (
	"context"
	"errors"
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
	random        *rand.Rand
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

type ShadowListObjectsQueryConfig struct {
	shadowEnabled bool          // A boolean flag to globally enable or disable the shadow mode for list_objects queries. When false, the shadow query will not be executed.
	shadowPct     int           // An integer representing the shadowPct of list_objects requests that will also trigger the shadow query. This allows for controlled rollout and data collection without impacting all requests. Value should be between 0 and 100.
	shadowTimeout time.Duration // A time.Duration specifying the maximum amount of time to wait for the shadow list_objects query to complete. If the shadow query exceeds this shadowTimeout, it will be cancelled, and its result will be ignored, but the shadowTimeout event will be logged.
	maxDeltaItems int           // The maximum number of items to log in the delta between the main and shadow results. This prevents excessive logging in case of large differences.
	logger        logger.Logger
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
	shadowConfig *ShadowListObjectsQueryConfig,
	opts ...ListObjectsQueryOption,
) (ListObjectsResolver, error) {
	if shadowConfig != nil && shadowConfig.shadowEnabled {
		return newShadowedListObjectsQuery(ds, checkResolver, shadowConfig, opts...)
	}

	return NewListObjectsQuery(ds, checkResolver, opts...)
}

// newShadowedListObjectsQuery creates a new ListObjectsResolver that runs two queries in parallel: one with optimizations and one without.
func newShadowedListObjectsQuery(
	ds storage.RelationshipTupleReader,
	checkResolver graph.CheckResolver,
	shadowConfig *ShadowListObjectsQueryConfig,
	opts ...ListObjectsQueryOption,
) (ListObjectsResolver, error) {
	if shadowConfig == nil {
		return nil, errors.New("shadowConfig must be set")
	}
	standard, err := NewListObjectsQuery(ds, checkResolver,
		// force disable optimizations
		slices.Concat(opts, []ListObjectsQueryOption{WithListObjectsOptimizationEnabled(false)})...,
	)
	if err != nil {
		return nil, err
	}
	optimized, err := NewListObjectsQuery(ds, checkResolver,
		// enable optimizations
		slices.Concat(opts, []ListObjectsQueryOption{WithListObjectsOptimizationEnabled(true)})...,
	)
	if err != nil {
		return nil, err
	}

	result := &shadowedListObjectsQuery{
		main:          standard,
		shadow:        optimized,
		shadowPct:     shadowConfig.shadowPct,
		shadowTimeout: shadowConfig.shadowTimeout,
		logger:        shadowConfig.logger,
		random:        rand.New(rand.NewSource(time.Now().UnixNano())),
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
	res, err := q.main.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	// If shadow mode is not shadowEnabled, just execute the main query
	if q.checkShadowModeSampleRate() {
		q.wg.Add(1) // only used for testing signals
		go func() {
			defer func() {
				if r := recover(); r != nil {
					q.logger.ErrorWithContext(cloneCtx, "panic recovered",
						zap.String("func", ListObjectsShadowExecute),
						zap.Any("request", req),
						zap.String("store_id", req.GetStoreId()),
						zap.String("model_id", req.GetAuthorizationModelId()),
						zap.Any("error", r),
					)
				}
			}()
			defer q.wg.Done() // only used for testing signals

			cloneRes := res.Clone()
			q.executeShadowModeAndCompareResults(cloneCtx, req, &cloneRes)
		}()
	}
	return res, err
}

func (q *shadowedListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
	return q.main.ExecuteStreamed(ctx, req, srv)
}

func (q *shadowedListObjectsQuery) checkShadowModeSampleRate() bool {
	return q.random.Intn(100) < q.shadowPct // randomly enable shadow mode
}

// executeShadowMode executes the main and shadow functions in parallel, returning the result of the main function if shadow mode is not shadowEnabled or if the shadow function fails.
// It compares the results of the main and shadow functions, logging any differences.
// If the shadow function takes longer than shadowTimeout, it will be cancelled, and its result will be ignored, but the shadowTimeout event will be logged.
// This function is designed to be run in a separate goroutine to avoid blocking the main execution flow.
func (q *shadowedListObjectsQuery) executeShadowModeAndCompareResults(parentCtx context.Context, req *openfgav1.ListObjectsRequest, res *ListObjectsResponse) {
	var commonFields = []zap.Field{
		zap.String("func", ListObjectsShadowExecute),
		zap.Any("request", req),
		zap.String("store_id", req.GetStoreId()),
		zap.String("model_id", req.GetAuthorizationModelId()),
	}
	defer func() {
		if r := recover(); r != nil {
			q.logger.ErrorWithContext(parentCtx, "panic recovered",
				append(commonFields, zap.Any("error", r))...,
			)
		}
	}()

	if res == nil { // should never happen, but to protect against nil dereference
		return
	}

	// don't run if the main result reaches max result size q.main.listObjectsMaxResults
	// that means there are more results than the shadow query can return,
	// so it is impossible to compare the results
	if loq, ok := q.main.(*ListObjectsQuery); ok {
		if len(res.Objects) == int(loq.listObjectsMaxResults) {
			q.logger.DebugWithContext(parentCtx, "shadowed list objects query skipped due to max results reached",
				// common args
				commonFields...,
			)
			return
		}
	}

	shadowCtx, shadowCancel := context.WithTimeout(parentCtx, q.shadowTimeout)
	defer shadowCancel()

	shadowRes, errShadow := q.shadow.Execute(shadowCtx, req)

	if errShadow != nil {
		q.logger.WarnWithContext(parentCtx, "shadowed list objects error",
			append(commonFields, zap.Any("error", errShadow))...,
		)
		return
	}

	objects := copyAndSortArr(res.Objects)
	var shadowObj []string
	if shadowRes != nil {
		shadowObj = copyAndSortArr(shadowRes.Objects)
	}
	// compare sorted string arrays - sufficient for equality check
	if !slices.Equal(objects, shadowObj) {
		delta := calculateDelta(objects, shadowObj)
		if len(delta) > q.maxDeltaItems {
			delta = delta[:q.maxDeltaItems] // limit the delta to maxDeltaItems
		}
		// log the differences if the shadow query failed or if the results are not equal
		q.logger.InfoWithContext(parentCtx, "shadowed list objects result difference",
			append(commonFields,
				zap.Int("main_result_count", len(objects)),
				zap.Int("shadow_result_count", len(shadowObj)),
				zap.Any("delta", delta),
			)...,
		)
	} else {
		q.logger.DebugWithContext(parentCtx, "shadowed list objects result matches",
			append(commonFields,
				zap.Int("result_count", len(objects)),
			)...,
		)
	}
}

// calculateDelta compares two slices of strings and returns a slice of strings representing the delta.
// Each string in the delta is prefixed with "+" if it exists in the shadow but not in the main,
// or "-" if it exists in the main but not in the shadow.
// Used for logging differences between the main and shadow results in the shadowed list objects query.
func calculateDelta(inputMain []string, inputShadow []string) []string {
	mainObjects := make(map[string]struct{}, len(inputMain))
	shadowObjects := make(map[string]struct{}, len(inputShadow))

	delta := make([]string, 0, len(inputMain)+len(inputShadow))
	// Create maps to track objects in main and shadow
	for _, obj := range inputMain {
		mainObjects[obj] = struct{}{}
	}
	for _, obj := range inputShadow {
		shadowObjects[obj] = struct{}{}
	}
	// Find objects in shadow but not in main
	for _, obj := range inputMain {
		if _, exists := shadowObjects[obj]; !exists {
			delta = append(delta, "-"+obj) // object in main but not in shadow
		}
	}
	for _, obj := range inputShadow {
		if _, exists := mainObjects[obj]; !exists {
			delta = append(delta, "+"+obj) // object in shadow but not in main
		}
	}

	return delta
}

// copyAndSortArr creates a copy of the input slice, sorts it, and returns the sorted slice.
func copyAndSortArr(input []string) []string {
	result := make([]string, len(input))
	copy(result, input)
	slices.Sort(result)
	return result
}
