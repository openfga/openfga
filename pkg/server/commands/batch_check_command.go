package commands

import (
	"context"
	"fmt"
	"sync"

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
	AssociatedIDs []CorrelationID
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

	if len(params.Checks) == 0 {
		return nil, nil, &BatchCheckValidationError{
			Message: "batch check requires at least one check to evaluate, no checks were received",
		}
	}

	if err := validateCorrelationIDs(params.Checks); err != nil {
		return nil, nil, err
	}

	/*
		cacheKey: {
			result: &Outcome{}
			correlation_ids: []
		}
	*/
	var resultMap = new(sync.Map)

	// cacheKeyToCorrelationIDs is used to short-circuit in the event we
	// receive duplicate checks with different correlation_ids
	// it has the structure { key: [list of correlation ids] }
	// var cacheKeyToCorrelationIDs = new(sync.Map)

	pool := concurrency.NewPool(ctx, int(bq.maxConcurrentChecks))
	for _, check := range params.Checks {
		pool.Go(func(ctx context.Context) error {
			tupleKey := check.GetTupleKey()
			cacheKeyParams := &graph.CacheKeyParams{
				StoreID:     params.StoreID,
				AuthModelID: bq.typesys.GetAuthorizationModelID(),
				TupleKey: &openfgav1.TupleKey{
					User:     tupleKey.GetUser(),
					Relation: tupleKey.GetRelation(),
					Object:   tupleKey.GetObject(),
				},
				ContextualTuples: check.GetContextualTuples().GetTupleKeys(),
				Context:          check.GetContext(),
			}

			// TODO this really shouldn't error
			cacheKey, _ := graph.GenerateCacheKey(cacheKeyParams)
			select {
			case <-ctx.Done():
				// TODO: extract this into its own struct method with comments to explain why this is all needed
				// If there's an existing cache entry, we've seen this check before
				existing, ok := resultMap.Load(cacheKey)
				if ok {
					outcome := existing.(*BatchCheckOutcome)
					// append this check's ID to the list of correlation IDs
					outcome.AssociatedIDs = append(outcome.AssociatedIDs, CorrelationID(check.GetCorrelationId()))
					resultMap.Store(cacheKey, outcome)
				} else {
					// We have not seen this check before, store a new outcome and correlation_id list
					resultMap.Store(cacheKey, &BatchCheckOutcome{
						Err:           ctx.Err(),
						AssociatedIDs: []CorrelationID{CorrelationID(check.GetCorrelationId())},
					})
				}
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
			//
			// tupleKey := check.GetTupleKey()
			// cacheKeyParams := &graph.CacheKeyParams{
			//	StoreID:     params.StoreID,
			//	AuthModelID: bq.typesys.GetAuthorizationModelID(),
			//	TupleKey: &openfgav1.TupleKey{
			//		User:     tupleKey.GetUser(),
			//		Relation: tupleKey.GetRelation(),
			//		Object:   tupleKey.GetObject(),
			//	},
			//	ContextualTuples: check.GetContextualTuples().GetTupleKeys(),
			//	Context:          check.GetContext(),
			//}
			//
			//// TODO this really shouldn't error
			// cacheKey, _ := graph.GenerateCacheKey(cacheKeyParams)

			// if this is ok that means we've done this check already in this batch
			// existingCorrelationIDs, ok := cacheKeyToCorrelationIDs.Load(cacheKey)
			//// cachkey: [list of correlation ids]
			// if ok {
			//	mu.Lock()
			//	log.Printf("Correlation id: %s", check.CorrelationId)
			//	idList := existingCorrelationIDs.([]string)
			//	idList = append(idList, check.GetCorrelationId()) // race
			//	mu.Unlock()
			//	cacheKeyToCorrelationIDs.Store(cacheKey, idList)
			//
			//	// we will map this duplicate check to its already-determined result after all routines have finished
			//	return nil
			//}
			//
			//// save this to this batch's cache
			// cacheKeyToCorrelationIDs.Store(cacheKey, []string{check.GetCorrelationId()})

			// I think this is still going to race
			existing, ok := resultMap.Load(cacheKey)
			if ok {
				outcome := existing.(*BatchCheckOutcome)
				// append this check's ID to the list of correlation IDs
				outcome.AssociatedIDs = append(outcome.AssociatedIDs, CorrelationID(check.GetCorrelationId()))
				resultMap.Store(cacheKey, outcome)
				return nil
			}

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
				AssociatedIDs: []CorrelationID{CorrelationID(check.GetCorrelationId())},
			})

			return nil
		})
	}

	err := pool.Wait()
	if err != nil {
		panic(err)
	}

	results := map[CorrelationID]*BatchCheckOutcome{}
	var totalQueryCount uint32

	resultMap.Range(func(k, v interface{}) bool {
		// Convert types since sync.Map is `any`
		outcome := v.(*BatchCheckOutcome)
		for _, id := range outcome.AssociatedIDs {
			results[id] = outcome
		}

		totalQueryCount += outcome.CheckResponse.GetResolutionMetadata().DatastoreQueryCount
		return true
	})

	// OLD
	// resultMap.Range(func(k, v interface{}) bool {
	//	// Convert types since sync.Map is `any`
	//	outcome := v.(*BatchCheckOutcome)
	//	results[CorrelationID(k.(string))] = outcome
	//
	//	totalQueryCount += outcome.CheckResponse.GetResolutionMetadata().DatastoreQueryCount
	//	return true
	// })

	// Now go through the checks which were dupes
	// cacheKeyToCorrelationIDs.Range(func(_, correlationIds interface{}) bool {
	//	ids := correlationIds.([]string)
	//	log.Printf("JUSTIN the ids being looped over: %+v", ids)
	//
	//	// this means there were no dupes
	//	if len(ids) == 1 {
	//		return true
	//	}
	//
	//	// the first ID written to the cache list is necessarily present in the results
	//	original := ids[0]
	//
	//	for _, dupe := range ids[1:] {
	//		log.Printf("Returning cached result for id=%s, was a dupe of id=%s", dupe, original)
	//		results[CorrelationID(dupe)] = results[CorrelationID(original)]
	//	}
	//	return true
	// })

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
