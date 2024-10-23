package commands

import (
	"context"
	"sync"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

const defaultMaxConcurrentChecksPerBatch = 25

type BatchCheckQuery struct {
	cacheController            cachecontroller.CacheController
	checkResolver              graph.CheckResolver
	datastore                  storage.RelationshipTupleReader
	logger                     logger.Logger
	maxConcurrentChecks        uint32
	maxConcurrentReadsPerCheck uint32
	resolveNodeLimit           uint32
	typesys                    *typesystem.TypeSystem
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

func NewBatchCheckCommand(datastore storage.RelationshipTupleReader, checkResolver graph.CheckResolver, typesys *typesystem.TypeSystem, opts ...BatchCheckQueryOption) *BatchCheckQuery {
	cmd := &BatchCheckQuery{
		logger:              logger.NewNoopLogger(),
		datastore:           datastore,
		checkResolver:       checkResolver,
		typesys:             typesys,
		maxConcurrentChecks: defaultMaxConcurrentChecksPerBatch,
	}

	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

func (bq *BatchCheckQuery) Execute(ctx context.Context, params *BatchCheckCommandParams) (map[string]*BatchCheckOutcome, error) {
	// This check query will be run against every check in the batch
	checkQuery := NewCheckCommand(
		bq.datastore,
		bq.checkResolver,
		bq.typesys,
		WithCheckCommandLogger(bq.logger),
		WithCacheController(bq.cacheController),
	)

	// the keys to this map are the correlation_id associated with each check
	var resultMap = map[string]*BatchCheckOutcome{}
	lock := sync.Mutex{}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Kick off all checks concurrently, there is no limit here
	// Concurrency limiting is left to check's ResolveNodeLimit and MaxConcurrentReads
	var wg sync.WaitGroup
	for _, check := range params.Checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			checkParams := &CheckCommandParams{
				StoreID:          params.StoreID,
				TupleKey:         check.TupleKey,
				ContextualTuples: check.ContextualTuples,
				Context:          check.Context,
				Consistency:      params.Consistency,
			}
			start := time.Now()

			response, _, err := checkQuery.Execute(ctx, checkParams)

			// lock the results map and add a new entry
			lock.Lock()
			resultMap[check.CorrelationId] = &BatchCheckOutcome{
				CheckResponse: response,
				Duration:      time.Since(start),
				Err:           err,
			}
			lock.Unlock()
		}()
	}
	wg.Wait()

	return resultMap, nil
}
