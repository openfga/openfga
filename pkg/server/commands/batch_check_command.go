package commands

import (
	"context"
	"fmt"
	"log"
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

	// Before processing the batch, deduplicate the checks based on their unique cache key
	// After all routines have finished, we will map each individual check response to all associated CorrelationIDs
	cacheKeyToCorrelationIDs := make(map[CacheKey][]CorrelationID)
	cacheKeyToCheck := make(map[CacheKey]*openfgav1.BatchCheckItem)
	for _, check := range params.Checks {
		key := generateCacheKeyFromCheck(check, params.StoreID, bq.typesys.GetAuthorizationModelID())
		cacheKeyToCheck[key] = check
		cacheKeyToCorrelationIDs[key] = append(cacheKeyToCorrelationIDs[key], CorrelationID(check.GetCorrelationId()))
	}

	var resultMap = new(sync.Map)
	var totalQueryCount atomic.Uint32

	pool := concurrency.NewPool(ctx, int(bq.maxConcurrentChecks))
	for key, check := range cacheKeyToCheck {
		pool.Go(func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				resultMap.Store(key, &BatchCheckOutcome{
					Err: ctx.Err(),
				})
				return nil
			default:
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

			resultMap.Store(key, &BatchCheckOutcome{
				CheckResponse: response,
				Err:           err,
			})

			totalQueryCount.Add(response.GetResolutionMetadata().DatastoreQueryCount)

			return nil
		})
	}

	_ = pool.Wait()

	results := map[CorrelationID]*BatchCheckOutcome{}

	duplicateCount := 0
	// Each cacheKey can have > 1 associated CorrelationID
	for cacheKey, ids := range cacheKeyToCorrelationIDs {
		duplicateCount = duplicateCount + len(ids) - 1 // there should always be at least 1 id
		res, _ := resultMap.Load(cacheKey)

		outcome := res.(*BatchCheckOutcome)
		for _, id := range ids {
			// map all associated CorrelationIDs to this outcome
			results[id] = outcome
		}
	}

	if duplicateCount > 0 {
		// TODO: telemetry "there were N duplicates"
		log.Printf("\nJUSTIN DUPLICATES: %d\n", duplicateCount)
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
