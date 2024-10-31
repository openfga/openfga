package commands

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/server/config"

	"github.com/openfga/openfga/pkg/server/errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

type BatchCheckQuery struct {
	cacheController     cachecontroller.CacheController
	checkResolver       graph.CheckResolver
	datastore           storage.RelationshipTupleReader
	logger              logger.Logger
	maxChecksAllowed    uint32
	maxConcurrentChecks uint32
	typesys             *typesystem.TypeSystem
}

type BatchCheckCommandParams struct {
	AuthorizationModelID string
	Checks               []*openfgav1.BatchCheckItem
	Consistency          openfgav1.ConsistencyPreference
	StoreID              string
}

type BatchCheckOutcome struct {
	CheckResponse *graph.ResolveCheckResponse
	Duration      time.Duration
	Err           error
}

type BatchCheckMetadata struct {
	TotalQueries uint32
}

type BatchCheckQueryOption func(*BatchCheckQuery)

func WithBatchCheckCommandCacheController(cc cachecontroller.CacheController) BatchCheckQueryOption {
	return func(bq *BatchCheckQuery) {
		bq.cacheController = cc
	}
}

func WithBatchCheckCommandLogger(l logger.Logger) BatchCheckQueryOption {
	return func(bq *BatchCheckQuery) {
		bq.logger = l
	}
}

func WithBatchCheckMaxConcurrentChecks(maxConcurrentChecks uint32) BatchCheckQueryOption {
	return func(bq *BatchCheckQuery) {
		bq.maxConcurrentChecks = maxConcurrentChecks
	}
}

func WithBatchCheckMaxChecksPerBatch(maxChecks uint32) BatchCheckQueryOption {
	return func(bq *BatchCheckQuery) {
		bq.maxChecksAllowed = maxChecks
	}
}

func NewBatchCheckCommand(datastore storage.RelationshipTupleReader, checkResolver graph.CheckResolver, typesys *typesystem.TypeSystem, opts ...BatchCheckQueryOption) *BatchCheckQuery {
	cmd := &BatchCheckQuery{
		logger:              logger.NewNoopLogger(),
		datastore:           datastore,
		cacheController:     cachecontroller.NewNoopCacheController(),
		checkResolver:       checkResolver,
		typesys:             typesys,
		maxChecksAllowed:    config.DefaultMaxChecksPerBatchCheck,
		maxConcurrentChecks: config.DefaultMaxConcurrentChecksPerBatchCheck,
	}

	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

func (bq *BatchCheckQuery) Execute(ctx context.Context, params *BatchCheckCommandParams) (map[string]*BatchCheckOutcome, *BatchCheckMetadata, error) {
	if len(params.Checks) > int(bq.maxChecksAllowed) {
		return nil, nil, errors.ValidationError(
			fmt.Errorf("batchCheck received %d checks, the maximum allowed is %d ", len(params.Checks), bq.maxChecksAllowed),
		)
	}

	if err := validateNoDuplicateCorrelationIDs(params.Checks); err != nil {
		return nil, nil, err
	}

	// the keys to this map are the correlation_id associated with each check
	var resultMap = map[string]*BatchCheckOutcome{}
	lock := sync.Mutex{}

	totalQueryCount := atomic.Uint32{}

	ctx, cancel := context.WithCancel(ctx)
	pool := concurrency.NewPool(ctx, int(bq.maxConcurrentChecks))
	defer func() {
		cancel()
	}()
	for _, check := range params.Checks {
		pool.Go(func(ctx context.Context) error {
			// This might have to be built on a per-check basis to get the metadata right
			checkQuery := NewCheckCommand(
				bq.datastore,
				bq.checkResolver,
				bq.typesys,
				WithCheckCommandLogger(bq.logger),
				WithCacheController(bq.cacheController),
			)

			checkParams := &CheckCommandParams{
				StoreID:          params.StoreID,
				TupleKey:         check.GetTupleKey(),
				ContextualTuples: check.GetContextualTuples(),
				Context:          check.GetContext(),
				Consistency:      params.Consistency,
			}

			start := time.Now()

			response, _, err := checkQuery.Execute(ctx, checkParams)

			lock.Lock()
			resultMap[check.GetCorrelationId()] = &BatchCheckOutcome{
				CheckResponse: response,
				Duration:      time.Since(start),
				Err:           err,
			}
			lock.Unlock()

			totalQueryCount.Add(response.GetResolutionMetadata().DatastoreQueryCount)

			return nil
		})
	}

	_ = pool.Wait()

	return resultMap, &BatchCheckMetadata{TotalQueries: totalQueryCount.Load()}, nil
}

func validateNoDuplicateCorrelationIDs(checks []*openfgav1.BatchCheckItem) error {
	seen := map[string]bool{}

	for _, check := range checks {
		if seen[check.GetCorrelationId()] {
			return errors.ValidationError(
				fmt.Errorf("received duplicate correlation id: %s", check.GetCorrelationId()),
			)
		}

		seen[check.GetCorrelationId()] = true
	}

	return nil
}
