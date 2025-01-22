package commands

import (
	"context"
	"errors"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const (
	defaultMaxConcurrentReadsForCheck = math.MaxUint32
)

type CheckQuery struct {
	logger               logger.Logger
	checkResolver        graph.CheckResolver
	typesys              *typesystem.TypeSystem
	datastore            storage.RelationshipTupleReader
	sharedCheckResources *shared.SharedCheckResources
	cacheSettings        config.CacheSettings
	maxConcurrentReads   uint32
	shouldCacheIterators bool
}

type CheckCommandParams struct {
	StoreID          string
	TupleKey         *openfgav1.CheckRequestTupleKey
	ContextualTuples *openfgav1.ContextualTupleKeys
	Context          *structpb.Struct
	Consistency      openfgav1.ConsistencyPreference
}

type CheckQueryOption func(*CheckQuery)

func WithCheckCommandMaxConcurrentReads(m uint32) CheckQueryOption {
	return func(c *CheckQuery) {
		c.maxConcurrentReads = m
	}
}

func WithCheckCommandLogger(l logger.Logger) CheckQueryOption {
	return func(c *CheckQuery) {
		c.logger = l
	}
}

func WithCheckCommandCache(sharedCheckResources *shared.SharedCheckResources, cacheSettings config.CacheSettings) CheckQueryOption {
	return func(c *CheckQuery) {
		c.sharedCheckResources = sharedCheckResources
		c.cacheSettings = cacheSettings
	}
}

// TODO accept CheckCommandParams so we can build the datastore object right away.
func NewCheckCommand(datastore storage.RelationshipTupleReader, checkResolver graph.CheckResolver, typesys *typesystem.TypeSystem, opts ...CheckQueryOption) *CheckQuery {
	cmd := &CheckQuery{
		logger:               logger.NewNoopLogger(),
		datastore:            datastore,
		checkResolver:        checkResolver,
		typesys:              typesys,
		maxConcurrentReads:   defaultMaxConcurrentReadsForCheck,
		shouldCacheIterators: false,
		cacheSettings:        config.NewDefaultCacheSettings(),
		sharedCheckResources: &shared.SharedCheckResources{
			CacheController: cachecontroller.NewNoopCacheController(),
		},
	}

	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

func (c *CheckQuery) Execute(ctx context.Context, params *CheckCommandParams) (*graph.ResolveCheckResponse, *graph.ResolveCheckRequestMetadata, error) {
	err := validateCheckRequest(c.typesys, params.TupleKey, params.ContextualTuples)
	if err != nil {
		return nil, nil, err
	}

	cacheInvalidationTime := time.Time{}

	if params.Consistency != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		cacheInvalidationTime = c.sharedCheckResources.CacheController.DetermineInvalidationTime(ctx, params.StoreID)
	}

	resolveCheckRequest, err := graph.NewResolveCheckRequest(
		graph.ResolveCheckRequestParams{
			StoreID:                   params.StoreID,
			TupleKey:                  tuple.ConvertCheckRequestTupleKeyToTupleKey(params.TupleKey),
			Context:                   params.Context,
			ContextualTuples:          params.ContextualTuples,
			Consistency:               params.Consistency,
			LastCacheInvalidationTime: cacheInvalidationTime,
			AuthorizationModelID:      c.typesys.GetAuthorizationModelID(),
		},
	)

	if err != nil {
		return nil, nil, err
	}

	requestDatastore := storagewrappers.NewRequestStorageWrapperForCheckAPI(c.datastore, params.ContextualTuples.GetTupleKeys(), c.maxConcurrentReads, c.sharedCheckResources, c.cacheSettings, c.logger)

	ctx = typesystem.ContextWithTypesystem(ctx, c.typesys)
	ctx = storage.ContextWithRelationshipTupleReader(ctx, requestDatastore)

	startTime := time.Now()

	resp, err := c.checkResolver.ResolveCheck(ctx, resolveCheckRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && resolveCheckRequest.GetRequestMetadata().WasThrottled.Load() {
			return nil, nil, &ThrottledError{Cause: err}
		}

		return nil, nil, err
	}

	resp.ResolutionMetadata.Duration = time.Since(startTime)
	resp.ResolutionMetadata.DatastoreQueryCount = requestDatastore.GetMetrics().DatastoreQueryCount

	return resp, resolveCheckRequest.GetRequestMetadata(), nil
}

func validateCheckRequest(typesys *typesystem.TypeSystem, tupleKey *openfgav1.CheckRequestTupleKey, contextualTuples *openfgav1.ContextualTupleKeys) error {
	// The input tuple Key should be validated loosely.
	if err := validation.ValidateUserObjectRelation(typesys, tuple.ConvertCheckRequestTupleKeyToTupleKey(tupleKey)); err != nil {
		return &InvalidRelationError{Cause: err}
	}

	// But contextual tuples need to be validated more strictly, the same as an input to a Write Tuple request.
	for _, ctxTuple := range contextualTuples.GetTupleKeys() {
		if err := validation.ValidateTupleForWrite(typesys, ctxTuple); err != nil {
			return &InvalidTupleError{Cause: err}
		}
	}
	return nil
}
