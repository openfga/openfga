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
}

func NewShadowedListObjectsQuery(
	ds storage.RelationshipTupleReader,
	checkResolver graph.CheckResolver,
	opts ...ListObjectsQueryOption,
) (ListObjectsQuery, error) {
	standard, err := NewListObjectsQuery(ds, checkResolver, opts...)
	if err != nil {
		return nil, err
	}
	optimizedOpts := slices.Concat(opts) // TODO : , WithListObjectsOptimizationEnabled(true))
	optimized, err := NewListObjectsQuery(ds, checkResolver, optimizedOpts...)
	if err != nil {
		return nil, err
	}
	return &shadowedListObjectsQuery{
		standard:  standard,
		optimized: optimized,
		logger:    standard.(*listObjectsQuery).logger,
	}, nil
}

func (q *shadowedListObjectsQuery) Execute(
	ctx context.Context,
	req *openfgav1.ListObjectsRequest,
) (*ListObjectsResponse, error) {

	if !q.isShadowModeEnabled(ctx) {
		return q.standard.Execute(ctx, req)
	}

	latency, latencyOptimized, result, resultOptimized, err, errOptimized := runInParallel(
		func() (*ListObjectsResponse, error) { return q.standard.Execute(ctx, req) },
		func() (*ListObjectsResponse, error) { return q.optimized.Execute(ctx, req) },
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

	if !q.isShadowModeEnabled(ctx) {
		return q.standard.ExecuteStreamed(ctx, req, srv)
	}

	latency, latencyOptimized, result, resultOptimized, err, errOptimized := runInParallel(
		func() (*ListObjectsResolutionMetadata, error) { return q.standard.ExecuteStreamed(ctx, req, srv) },
		func() (*ListObjectsResolutionMetadata, error) { return q.optimized.ExecuteStreamed(ctx, req, srv) },
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

func (q *shadowedListObjectsQuery) isShadowModeEnabled(ctx context.Context) bool {
	enabled, ok := ctx.Value("list-objects-optimization").(bool)
	return ok && enabled
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
