package commands

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

type BatchCheckQuery struct {
	logger              logger.Logger
	maxChecksAllowed    uint32
	maxConcurrentChecks uint32
	checker             Checker
}

type BatchCheckCommandParams struct {
	AuthorizationModelID string
	Checks               []*openfgav1.BatchCheckItem
	Consistency          openfgav1.ConsistencyPreference
	StoreID              string
}

type BatchCheckOutcome struct {
	Allowed             bool
	DatastoreQueryCount uint32
	DatastoreItemCount  uint64
	Duration            time.Duration
	Err                 error
}

type BatchCheckMetadata struct {
	DispatchThrottleCount  uint32
	DispatchCount          uint32
	DatastoreQueryCount    uint32
	DatastoreItemCount     uint64
	DatastoreThrottleCount uint32
	DuplicateCheckCount    int
	PrimaryCheckerCount    uint32
	FallbackCount          uint32
}

type BatchCheckValidationError struct {
	Message string
}

func (e BatchCheckValidationError) Error() string {
	return e.Message
}

type CorrelationID string

type checkAndCorrelationIDs struct {
	Check          *openfgav1.BatchCheckItem
	CorrelationIDs []CorrelationID
}

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

func NewBatchCheckCommand(checker Checker, opts ...BatchCheckQueryOption) *BatchCheckQuery {
	cmd := &BatchCheckQuery{
		logger:              logger.NewNoopLogger(),
		maxChecksAllowed:    config.DefaultMaxChecksPerBatchCheck,
		maxConcurrentChecks: config.DefaultMaxConcurrentChecksPerBatchCheck,
		checker:             checker,
	}

	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

func (bq *BatchCheckQuery) Execute(ctx context.Context, params *BatchCheckCommandParams) (map[CorrelationID]*BatchCheckOutcome, *BatchCheckMetadata, error) {
	if len(params.Checks) > int(bq.maxChecksAllowed) {
		return nil, nil, &BatchCheckValidationError{
			Message: "batchCheck received " + strconv.Itoa(len(params.Checks)) + " checks, the maximum allowed is " + strconv.Itoa(int(bq.maxChecksAllowed)),
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
	cacheKeyMap := make(map[keys.Key]*checkAndCorrelationIDs)
	for _, check := range params.Checks {
		key := generateCacheKeyFromCheck(check, params.StoreID, params.AuthorizationModelID)

		if item, ok := cacheKeyMap[key]; ok {
			item.CorrelationIDs = append(item.CorrelationIDs, CorrelationID(check.GetCorrelationId()))
		} else {
			cacheKeyMap[key] = &checkAndCorrelationIDs{
				Check:          check,
				CorrelationIDs: []CorrelationID{CorrelationID(check.GetCorrelationId())},
			}
		}
	}

	var resultMap = new(sync.Map)
	var totalQueryCount atomic.Uint32
	var totalDispatchCount atomic.Uint32
	var dispatchThrottleCount atomic.Uint32
	var totalItemCount atomic.Uint64
	var datastoreThrottleCount atomic.Uint32
	var primaryCheckerCount atomic.Uint32
	var fallbackCount atomic.Uint32

	pool := concurrency.NewPool(ctx, int(bq.maxConcurrentChecks))
	for key, item := range cacheKeyMap {
		check := item.Check
		pool.Go(func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				resultMap.Store(key, &BatchCheckOutcome{
					Err: ctx.Err(),
				})
				return nil
			default:
			}

			checkParams := &CheckCommandParams{
				StoreID:          params.StoreID,
				TupleKey:         check.GetTupleKey(),
				ContextualTuples: check.GetContextualTuples(),
				Context:          check.GetContext(),
				Consistency:      params.Consistency,
			}

			res, err := bq.checker.Execute(ctx, checkParams)
			if res == nil {
				res = &CheckResult{}
			}
			resultMap.Store(key, &BatchCheckOutcome{
				Allowed:             res.Allowed,
				DatastoreQueryCount: res.DatastoreQueryCount,
				DatastoreItemCount:  res.DatastoreItemCount,
				Duration:            res.Duration,
				Err:                 err,
			})
			totalDispatchCount.Add(res.DispatchCount)
			if res.DispatchThrottled {
				dispatchThrottleCount.Add(1)
			}
			totalQueryCount.Add(res.DatastoreQueryCount)
			totalItemCount.Add(res.DatastoreItemCount)
			if res.WasThrottled {
				datastoreThrottleCount.Add(1)
			}
			if res.UsedFallback {
				fallbackCount.Add(1)
			} else {
				primaryCheckerCount.Add(1)
			}
			return nil
		})
	}

	_ = pool.Wait()

	results := map[CorrelationID]*BatchCheckOutcome{}

	// Each cacheKey can have > 1 associated CorrelationID
	for cacheKey, checkItem := range cacheKeyMap {
		res, _ := resultMap.Load(cacheKey)
		outcome := res.(*BatchCheckOutcome)

		for _, id := range checkItem.CorrelationIDs {
			// map all associated CorrelationIDs to this outcome
			results[id] = outcome
		}
	}

	return results, &BatchCheckMetadata{
		DispatchThrottleCount:  dispatchThrottleCount.Load(),
		DatastoreQueryCount:    totalQueryCount.Load(),
		DatastoreItemCount:     totalItemCount.Load(),
		DatastoreThrottleCount: datastoreThrottleCount.Load(),
		DispatchCount:          totalDispatchCount.Load(),
		DuplicateCheckCount:    len(params.Checks) - len(cacheKeyMap),
		PrimaryCheckerCount:    primaryCheckerCount.Load(),
		FallbackCount:          fallbackCount.Load(),
	}, nil
}

func validateCorrelationIDs(checks []*openfgav1.BatchCheckItem) error {
	seen := map[string]struct{}{}

	for _, check := range checks {
		if check.GetCorrelationId() == "" {
			return &BatchCheckValidationError{
				Message: "received empty correlation id for tuple: " + check.GetTupleKey().String(),
			}
		}

		_, ok := seen[check.GetCorrelationId()]
		if ok {
			return &BatchCheckValidationError{
				Message: "received duplicate correlation id: " + check.GetCorrelationId(),
			}
		}

		seen[check.GetCorrelationId()] = struct{}{}
	}

	return nil
}

func generateCacheKeyFromCheck(check *openfgav1.BatchCheckItem, storeID, authModelID string) keys.Key {
	checkTupleKey := check.GetTupleKey()
	key := storage.CheckCacheKey(
		storeID,
		checkTupleKey.GetObject(),
		checkTupleKey.GetRelation(),
		checkTupleKey.GetUser(),
		storage.InvariantCacheKey(
			storeID,
			authModelID,
			check.GetContext(),
			check.GetContextualTuples().GetTupleKeys()...,
		),
	)
	return key
}
