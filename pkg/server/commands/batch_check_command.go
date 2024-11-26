package commands

import (
	"context"
	"fmt"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/logger"
)

type BatchCheckQuery struct {
	logger              logger.Logger
	maxChecksAllowed    uint32
	maxConcurrentChecks uint32
	client              openfgav1.OpenFGAServiceClient
}

type BatchCheckCommandParams struct {
	AuthorizationModelID string
	Checks               []*openfgav1.BatchCheckItem
	Consistency          openfgav1.ConsistencyPreference
	StoreID              string
}

type BatchCheckOutcome struct {
	CheckResponse *openfgav1.CheckResponse
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

func NewBatchCheckCommand(client openfgav1.OpenFGAServiceClient, opts ...BatchCheckQueryOption) *BatchCheckQuery {
	cmd := &BatchCheckQuery{
		logger:              logger.NewNoopLogger(),
		client:              client,
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

			resp, err := bq.client.Check(ctx, &openfgav1.CheckRequest{
				StoreId:              params.StoreID,
				TupleKey:             check.GetTupleKey(),
				AuthorizationModelId: params.AuthorizationModelID,
				ContextualTuples:     check.GetContextualTuples(),
				Context:              check.GetContext(),
				Consistency:          params.Consistency,
			})

			resultMap.Store(check.GetCorrelationId(), &BatchCheckOutcome{
				CheckResponse: resp,
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

		// TODO: does the overall query count within the BatchCheck matter or are we ok with the tracking
		// we now get from the individual Check calls?
		// totalQueryCount += outcome.CheckResponse.GetResolutionMetadata().DatastoreQueryCount
		return true
	})

	return results, &BatchCheckMetadata{DatastoreQueryCount: totalQueryCount}, nil
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
