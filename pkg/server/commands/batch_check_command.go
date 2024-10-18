package commands

import (
	"context"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	"time"

	"github.com/openfga/openfga/pkg/logger"
)

//const (
//	defaultResolveNodeLimit           = 25
//	defaultMaxConcurrentReadsForCheck = math.MaxUint32
//)

type BatchCheckQuery struct {
	logger          logger.Logger
	checkResolver   graph.CheckResolver
	typesys         *typesystem.TypeSystem
	datastore       storage.RelationshipTupleReader
	cacheController cachecontroller.CacheController

	//resolveNodeLimit    uint32
	maxConcurrentChecks uint32
	//maxConcurrentReads  uint32
}

type BatchCheckQueryOption func(*BatchCheckQuery)

func WithBatchCheckCommandMaxConcurrentChecks(m uint32) BatchCheckQueryOption {
	return func(c *BatchCheckQuery) {
		c.maxConcurrentChecks = m
	}
}

func WithCheckCommandCacheController(cc cachecontroller.CacheController) BatchCheckQueryOption {
	return func(bq *BatchCheckQuery) {
		bq.cacheController = cc
	}

}

func NewBatchCheckCommand(datastore storage.RelationshipTupleReader, checkResolver graph.CheckResolver, typesys *typesystem.TypeSystem, opts ...BatchCheckQueryOption) *BatchCheckQuery {
	cmd := &BatchCheckQuery{
		logger:        logger.NewNoopLogger(),
		datastore:     datastore,
		checkResolver: checkResolver,
		typesys:       typesys,
	}

	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

// Execute here needs new return types as well.
func (bq *BatchCheckQuery) Execute(ctx context.Context, req *openfgav1.BatchCheckRequest) (*graph.ResolveCheckResponse, *graph.ResolveCheckRequestMetadata, error) {
	err := req.Validate()
	if err != nil {
		return nil, nil, err
	}

	cacheInvalidationTime := time.Time{}

	if req.GetConsistency() != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		cacheInvalidationTime = bq.cacheController.DetermineInvalidation(ctx, req.GetStoreId())
	}

	// for validation:
	// validate the overall batch check request
	// then within each loop validate the check itself
	// you get the validate check request for free from check, probably just have to refactor
	// it a bit since it relies on a request instead of a struct
	//err := validateCheckRequest(ctx, req, c.typesys)
	// if fail write to an errors chan and return the error in the resultant map

	//checkCommand := NewCheckCommand(
	//	)
	resp, checkRequestMetadata, err := NewCheckCommand(
		bq.datastore,
		bq.checkResolver,
		bq.typesys,
		WithCheckCommandLogger(s.logger), // logger?
		WithCheckCommandMaxConcurrentReads(s.maxConcurrentReadsForCheck), // these?
		WithCheckCommandResolveNodeLimit(s.resolveNodeLimit),             // these?
		WithCacheController(s.cacheController),                           // these?
	).Execute(ctx, req)

}
