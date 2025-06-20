package commands

import (
	"context"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	"go.uber.org/zap"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/storage"
)

type shadowedListObjectsQuery struct {
	standard  ListObjectsQuery
	optimized ListObjectsQuery
	logger    logger.Logger
	config    *ShadowListObjectsQueryConfig
}

type ShadowListObjectsQueryOption func(d *ShadowListObjectsQueryConfig)

func WithShadowListObjectsQueryEnabled(enabled bool) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.enabled = enabled
	}
}

func WithShadowListObjectsQuerySamplePercentage(samplePercentage int) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.percentage = samplePercentage
	}
}

func WithShadowListObjectsQueryTimeout(timeout time.Duration) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.timeout = timeout
	}
}

func WithShadowListObjectsQueryLogger(logger logger.Logger) ShadowListObjectsQueryOption {
	return func(c *ShadowListObjectsQueryConfig) {
		c.logger = logger
	}
}

type ShadowListObjectsQueryConfig struct {
	enabled    bool          // A boolean flag to globally enable or disable the shadow mode for list_objects queries. When false, the shadow query will not be executed.
	percentage int           // An integer representing the percentage of list_objects requests that will also trigger the shadow query. This allows for controlled rollout and data collection without impacting all requests. Value should be between 0 and 100.
	timeout    time.Duration // A time.Duration specifying the maximum amount of time to wait for the shadow list_objects query to complete. If the shadow query exceeds this timeout, it will be cancelled, and its result will be ignored, but the timeout event will be logged.
	logger     logger.Logger
}

func NewShadowListObjectsQueryConfig(opts ...ShadowListObjectsQueryOption) *ShadowListObjectsQueryConfig {
	result := &ShadowListObjectsQueryConfig{
		enabled: false,
		logger:  logger.NewNoopLogger(),
	}
	for _, opt := range opts {
		opt(result)
	}
	return result
}

func NewListObjectsQueryWithShadowConfig(
	ds storage.RelationshipTupleReader,
	checkResolver graph.CheckResolver,
	shadowConfig *ShadowListObjectsQueryConfig,
	opts ...ListObjectsQueryOption,
) (ListObjectsQuery, error) {
	if shadowConfig != nil && shadowConfig.enabled {
		return newShadowedListObjectsQuery(ds, checkResolver, shadowConfig, opts...)
	}

	return newListObjectsQuery(ds, checkResolver, opts...)
}

func newShadowedListObjectsQuery(
	ds storage.RelationshipTupleReader,
	checkResolver graph.CheckResolver,
	shadowConfig *ShadowListObjectsQueryConfig,
	opts ...ListObjectsQueryOption,
) (ListObjectsQuery, error) {
	standard, err := newListObjectsQuery(ds, checkResolver,
		// force disable optimizations
		slices.Concat(opts, []ListObjectsQueryOption{WithListObjectsOptimizationEnabled(false)})...,
	)
	if err != nil {
		return nil, err
	}
	optimized, err := newListObjectsQuery(ds, checkResolver,
		// enable optimizations
		slices.Concat(opts, []ListObjectsQueryOption{WithListObjectsOptimizationEnabled(true)})...,
	)
	if err != nil {
		return nil, err
	}

	if shadowConfig == nil {
		shadowConfig = NewShadowListObjectsQueryConfig()
	}

	result := &shadowedListObjectsQuery{
		standard:  standard,
		optimized: optimized,
		logger:    standard.(*listObjectsQuery).logger, // borrow the logger from standard
		config:    shadowConfig,
	}

	return result, nil
}

func (q *shadowedListObjectsQuery) Execute(
	ctx context.Context,
	req *openfgav1.ListObjectsRequest,
) (*ListObjectsResponse, error) {

	if !q.checkShadowModeSampleRate() {
		return q.standard.Execute(ctx, req)
	}

	shadowCtx, shadowCancel := context.WithTimeout(ctx, q.config.timeout)
	defer shadowCancel()

	latency, latencyOptimized, result, resultOptimized, err, errOptimized := runInParallel(
		func() (*ListObjectsResponse, error) {
			defer shadowCancel() // cancel shadow ctx once standard is done
			return q.standard.Execute(ctx, req)
		},
		func() (*ListObjectsResponse, error) {
			return q.optimized.Execute(shadowCtx, req)
		},
	)

	if err != nil {
		return nil, err
	}

	if errOptimized != nil {
		q.logger.Error("shadowed list objects error", zap.Error(errOptimized))
		return result, nil
	}

	q.logger.Info("shadowed list objects",
		zap.Bool("equal", reflect.DeepEqual(result, resultOptimized)),
		zap.Any("result", result),
		zap.Any("resultOptimized", resultOptimized),
		zap.Duration("latency", latency),
		zap.Duration("latencyOptimized", latencyOptimized))

	return result, nil
}

func (q *shadowedListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {

	if !q.checkShadowModeSampleRate() {
		return q.standard.ExecuteStreamed(ctx, req, srv)
	}

	shadowCtx, shadowCancel := context.WithTimeout(ctx, q.config.timeout)
	defer shadowCancel()

	latency, latencyOptimized, result, resultOptimized, err, errOptimized := runInParallel(
		func() (*ListObjectsResolutionMetadata, error) {
			defer shadowCancel() // cancel shadow ctx once standard is done
			return q.standard.ExecuteStreamed(ctx, req, srv)
		},
		func() (*ListObjectsResolutionMetadata, error) {
			return q.optimized.ExecuteStreamed(shadowCtx, req, srv)
		},
	)

	if err != nil {
		return nil, err
	}

	if errOptimized != nil {
		q.logger.Error("shadowed list objects streamed error", zap.Error(errOptimized))
		return result, nil
	}

	q.logger.Info("shadowed list objects streamed",
		zap.Bool("equal", reflect.DeepEqual(&result, &resultOptimized)),
		zap.Any("result", result),
		zap.Any("resultOptimized", resultOptimized),
		zap.Duration("latency", latency),
		zap.Duration("latencyOptimized", latencyOptimized))

	return result, nil
}

func (q *shadowedListObjectsQuery) checkShadowModeSampleRate() bool {
	percentage := q.config.percentage
	return int(time.Now().UnixNano()%100) < percentage // randomly enable shadow mode
}

// helper to run two functions in parallel and collect their results and latencies
func runInParallel[T any](
	fn1 func() (T, error),
	fn2 func() (T, error),
) (latency1, latency2 time.Duration, result1, result2 T, err1, err2 error) {
	var wg sync.WaitGroup
	start := time.Now()

	wg.Add(2)
	go func() {
		defer wg.Done()
		result1, err1 = fn1()
		latency1 = time.Since(start)
	}()
	go func() {
		defer wg.Done()
		result2, err2 = fn2()
		latency2 = time.Since(start)
	}()
	wg.Wait()
	return
}
