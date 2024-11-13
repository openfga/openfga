package commands

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/server/config"
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
	Err           error
}

type BatchCheckMetadata struct {
	DatastoreQueryCount uint32
}

type BatchCheckValidationError struct {
	Message string
}

func (e BatchCheckValidationError) Error() string {
	return e.Message
}

type CorrelationID string
type CacheKey string

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

func (bq *BatchCheckQuery) Execute(ctx context.Context, params *BatchCheckCommandParams) (map[CorrelationID]*BatchCheckOutcome, *BatchCheckMetadata, error) {
	if len(params.Checks) > int(bq.maxChecksAllowed) {
		return nil, nil, &BatchCheckValidationError{
			Message: fmt.Sprintf("batchCheck received %d checks, the maximum allowed is %d ", len(params.Checks), bq.maxChecksAllowed),
		}
	}

	if len(params.Checks) == 0 {
		return nil, nil, &BatchCheckValidationError{
			Message: "batch check requires at least one check to evaluate, no checks were received",
		}
	}

	if err := validateCorrelationIDs(params.Checks); err != nil {
		return nil, nil, err
	}

	// build a cache key map before concurrency
	// if resultMap.load(cacheKey) is ok, just attach that to the correlation id
	// can even pre-allocate the results map in a non-sync manner I think
	correlationToCacheKey := make(map[CorrelationID]CacheKey)
	for _, check := range params.Checks {
		// now prebuild the map
		correlationToCacheKey[CorrelationID(check.GetCorrelationId())] =
			generateCacheKeyFromCheck(check, params.StoreID, bq.typesys.GetAuthorizationModelID())
	}

	var resultMap = new(sync.Map)
	var totalQueryCount atomic.Uint32

	pool := concurrency.NewPool(ctx, int(bq.maxConcurrentChecks))
	for _, check := range params.Checks {
		cacheKey := correlationToCacheKey[CorrelationID(check.GetCorrelationId())]

		pool.Go(func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				// TODO do it in here too
				resultMap.Store(cacheKey, &BatchCheckOutcome{
					Err: ctx.Err(),
				})
				return nil
			default:
			}

			// if this check has already been run, we can bail
			// we reconcile after the worker pool is finished
			_, ok := resultMap.Load(cacheKey)
			if ok {
				return nil
			}

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

			response, _, err := checkQuery.Execute(ctx, checkParams)

			resultMap.Store(cacheKey, &BatchCheckOutcome{
				CheckResponse: response,
				Err:           err,
			})

			totalQueryCount.Add(response.GetResolutionMetadata().DatastoreQueryCount)

			return nil
		})
	}

	_ = pool.Wait()

	results := map[CorrelationID]*BatchCheckOutcome{}

	for id, key := range correlationToCacheKey {
		response, ok := resultMap.Load(key)
		if !ok {
			panic("this is bad")
		}
		results[id] = response.(*BatchCheckOutcome)
	}

	return results, &BatchCheckMetadata{DatastoreQueryCount: totalQueryCount.Load()}, nil
}

func validateCorrelationIDs(checks []*openfgav1.BatchCheckItem) error {
	seen := map[string]struct{}{}

	for _, check := range checks {
		if check.GetCorrelationId() == "" {
			return &BatchCheckValidationError{
				Message: fmt.Sprintf("received empty correlation id for tuple: %s", check.GetTupleKey()),
			}
		}

		_, ok := seen[check.GetCorrelationId()]
		if ok {
			return &BatchCheckValidationError{
				Message: fmt.Sprintf("received duplicate correlation id: %s", check.GetCorrelationId()),
			}
		}

		seen[check.GetCorrelationId()] = struct{}{}
	}

	return nil
}

func generateCacheKeyFromCheck(check *openfgav1.BatchCheckItem, storeID string, authModelID string) CacheKey {
	tupleKey := check.GetTupleKey()
	cacheKeyParams := &graph.CacheKeyParams{
		StoreID:     storeID,
		AuthModelID: authModelID,
		TupleKey: &openfgav1.TupleKey{
			User:     tupleKey.GetUser(),
			Relation: tupleKey.GetRelation(),
			Object:   tupleKey.GetObject(),
		},
		ContextualTuples: check.GetContextualTuples().GetTupleKeys(),
		Context:          check.GetContext(),
	}

	cacheKey, _ := graph.GenerateCacheKey(cacheKeyParams)
	return CacheKey(cacheKey)
}
