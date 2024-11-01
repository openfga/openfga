package commands

import (
	"context"
	"fmt"
	"sync"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/server/config"

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

	if err := validateCorrelationIDs(params.Checks); err != nil {
		return nil, nil, err
	}

	var resultMap = new(sync.Map)

	pool := concurrency.NewPool(ctx, int(bq.maxConcurrentChecks))
	for _, check := range params.Checks {
		pool.Go(func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				resultMap.Store(check.GetCorrelationId(), &BatchCheckOutcome{
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

			resultMap.Store(check.GetCorrelationId(), &BatchCheckOutcome{
				CheckResponse: response,
				Err:           err,
			})

			return nil
		})
	}

	_ = pool.Wait()

	results := map[CorrelationID]*BatchCheckOutcome{}
	var totalQueryCount uint32

	resultMap.Range(func(k, v interface{}) bool {
		// Convert types since sync.Map is `any`
		outcome := v.(*BatchCheckOutcome)
		results[CorrelationID(k.(string))] = outcome

		totalQueryCount += outcome.CheckResponse.GetResolutionMetadata().DatastoreQueryCount
		return true
	})

	return results, &BatchCheckMetadata{DatastoreQueryCount: totalQueryCount}, nil
}

func validateCorrelationIDs(checks []*openfgav1.BatchCheckItem) error {
	seen := map[string]struct{}{}

	for _, check := range checks {
		if check.GetCorrelationId() == "" {
			return &BatchCheckValidationError{
				Message: fmt.Sprintf("received empty correlation id: %s", check.GetCorrelationId()),
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
