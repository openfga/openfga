package commands

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

type BatchCheckQuery struct {
	sharedCheckResources *shared.SharedCheckResources
	cacheSettings        config.CacheSettings
	checkResolver        graph.CheckResolver
	datastore            storage.RelationshipTupleReader
	logger               logger.Logger
	maxChecksAllowed     uint32
	maxConcurrentChecks  uint32
	typesys              *typesystem.TypeSystem
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
	DuplicateCheckCount int
}

type BatchCheckValidationError struct {
	Message string
}

func (e BatchCheckValidationError) Error() string {
	return e.Message
}

type CorrelationID string
type CacheKey string

type checkAndCorrelationIDs struct {
	Check          *openfgav1.BatchCheckItem
	CorrelationIDs []CorrelationID
}

type BatchCheckQueryOption func(*BatchCheckQuery)

func WithBatchCheckCacheOptions(sharedCheckResources *shared.SharedCheckResources, cacheSettings config.CacheSettings) BatchCheckQueryOption {
	return func(c *BatchCheckQuery) {
		c.sharedCheckResources = sharedCheckResources
		c.cacheSettings = cacheSettings
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
		checkResolver:       checkResolver,
		typesys:             typesys,
		maxChecksAllowed:    config.DefaultMaxChecksPerBatchCheck,
		maxConcurrentChecks: config.DefaultMaxConcurrentChecksPerBatchCheck,
		cacheSettings:       config.NewDefaultCacheSettings(),
		sharedCheckResources: &shared.SharedCheckResources{
			CacheController: cachecontroller.NewNoopCacheController(),
		},
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
	cacheKeyMap := make(map[CacheKey]*checkAndCorrelationIDs)
	for _, check := range params.Checks {
		key, err := generateCacheKeyFromCheck(check, params.StoreID, bq.typesys.GetAuthorizationModelID())
		if err != nil {
			bq.logger.Error("batch check cache key computation failed with error", zap.Error(err))
			return nil, nil, err
		}

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

			checkQuery := NewCheckCommand(
				bq.datastore,
				bq.checkResolver,
				bq.typesys,
				WithCheckCommandLogger(bq.logger),
				WithCheckCommandCache(bq.sharedCheckResources, bq.cacheSettings),
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
		DatastoreQueryCount: totalQueryCount.Load(),
		DuplicateCheckCount: len(params.Checks) - len(cacheKeyMap),
	}, nil
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

func generateCacheKeyFromCheck(check *openfgav1.BatchCheckItem, storeID string, authModelID string) (CacheKey, error) {
	tupleKey := check.GetTupleKey()
	cacheKeyParams := &storage.CheckCacheKeyParams{
		StoreID:              storeID,
		AuthorizationModelID: authModelID,
		TupleKey: &openfgav1.TupleKey{
			User:     tupleKey.GetUser(),
			Relation: tupleKey.GetRelation(),
			Object:   tupleKey.GetObject(),
		},
		ContextualTuples: check.GetContextualTuples().GetTupleKeys(),
		Context:          check.GetContext(),
	}

	hasher := xxhash.New()
	err := storage.WriteCheckCacheKey(hasher, cacheKeyParams)
	if err != nil {
		return "", err
	}

	keyStr := strconv.FormatUint(hasher.Sum64(), 10)
	return CacheKey(keyStr), nil
}
