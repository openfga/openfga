package commands

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

const defaultMaxConcurrentChecksPerBatch = 50

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

type BatchCheckQueryOption func(*BatchCheckQuery)

func WithBatchCheckCommandMaxConcurrentChecks(m uint32) BatchCheckQueryOption {
	return func(c *BatchCheckQuery) {
		c.maxConcurrentChecks = m
	}
}

func WithBatchCheckCommandCacheController(cc cachecontroller.CacheController) BatchCheckQueryOption {
	return func(bq *BatchCheckQuery) {
		bq.cacheController = cc
	}
}

func WithBatchCheckResolveNodeLimit(resolveLimit uint32) BatchCheckQueryOption {
	return func(bq *BatchCheckQuery) {
		bq.resolveNodeLimit = resolveLimit
	}
}

func WithBatchCheckMaxConcurrentReadsPerCheck(maxReads uint32) BatchCheckQueryOption {
	return func(bq *BatchCheckQuery) {
		bq.maxConcurrentReadsPerCheck = maxReads
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

// Execute here needs new return types as well.
// TODO
func (bq *BatchCheckQuery) Execute(ctx context.Context, params BatchCheckCommandParams) (*graph.ResolveCheckResponse, *graph.ResolveCheckRequestMetadata, error) {
	//cacheInvalidationTime := time.Time{}
	//
	//if req.GetConsistency() != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
	//	cacheInvalidationTime = bq.cacheController.DetermineInvalidation(ctx, req.GetStoreId())
	//}

	// for validation:
	// validate the overall batch check request
	// then within each loop validate the check itself
	// you get the validate check request for free from check, probably just have to refactor
	// it a bit since it relies on a request instead of a struct
	//err := validateCheckRequest(ctx, req, c.typesys)
	// if fail write to an errors chan and return the error in the resultant map

	//checkCommand := NewCheckCommand(
	//	)
	// Spin up 1 check command
	checkQuery := NewCheckCommand(
		bq.datastore,
		bq.checkResolver,
		bq.typesys,
		WithCheckCommandLogger(bq.logger),
		WithCacheController(bq.cacheController),
		WithCheckCommandMaxConcurrentReads(bq.maxConcurrentReadsPerCheck),
		WithCheckCommandResolveNodeLimit(bq.resolveNodeLimit),
	)

	// but then execute in many goroutines and accumulate results
	resultsChan := make(chan graph.ResolveCheckResponse, len(params.Checks))
	errorsChan := make(chan error, len(params.Checks))
	pool := concurrency.NewPool(ctx, int(bq.maxConcurrentChecks))

	checkQuery.Execute(ctx, req)
}
