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
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const (
	defaultResolveNodeLimit           = 25
	defaultMaxConcurrentReadsForCheck = math.MaxUint32
)

type CheckQuery struct {
	logger          logger.Logger
	checkResolver   graph.CheckResolver
	typesys         *typesystem.TypeSystem
	datastore       *storagewrappers.InstrumentedOpenFGAStorage
	cacheController cachecontroller.CacheController

	resolveNodeLimit   uint32
	maxConcurrentReads uint32
}

type CheckCommandParams struct {
	StoreID          string
	TupleKey         *openfgav1.CheckRequestTupleKey
	ContextualTuples *openfgav1.ContextualTupleKeys
	Context          *structpb.Struct
	Consistency      openfgav1.ConsistencyPreference
}

type CheckQueryOption func(*CheckQuery)

func WithCheckCommandResolveNodeLimit(nl uint32) CheckQueryOption {
	return func(c *CheckQuery) {
		c.resolveNodeLimit = nl
	}
}

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

func WithCacheController(ctrl cachecontroller.CacheController) CheckQueryOption {
	return func(c *CheckQuery) {
		c.cacheController = ctrl
	}
}

func NewCheckCommand(datastore storage.RelationshipTupleReader, checkResolver graph.CheckResolver, typesys *typesystem.TypeSystem, opts ...CheckQueryOption) *CheckQuery {
	cmd := &CheckQuery{
		logger:             logger.NewNoopLogger(),
		datastore:          storagewrappers.NewInstrumentedOpenFGAStorage(datastore),
		checkResolver:      checkResolver,
		typesys:            typesys,
		cacheController:    cachecontroller.NewNoopCacheController(),
		resolveNodeLimit:   defaultResolveNodeLimit,
		maxConcurrentReads: defaultMaxConcurrentReadsForCheck,
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
		cacheInvalidationTime = c.cacheController.DetermineInvalidation(ctx, params.StoreID)
	}

	resolveCheckRequest := graph.ResolveCheckRequest{
		StoreID:              params.StoreID,
		AuthorizationModelID: c.typesys.GetAuthorizationModelID(), // the resolved model ID
		TupleKey:             tuple.ConvertCheckRequestTupleKeyToTupleKey(params.TupleKey),
		ContextualTuples:     params.ContextualTuples.GetTupleKeys(),
		Context:              params.Context,
		VisitedPaths:         make(map[string]struct{}),
		RequestMetadata:      graph.NewCheckRequestMetadata(c.resolveNodeLimit),
		Consistency:          params.Consistency,
		// avoid having to read from cache consistently by propagating it
		LastCacheInvalidationTime: cacheInvalidationTime,
	}

	ctx = buildCheckContext(ctx, c.typesys, c.datastore, c.maxConcurrentReads, resolveCheckRequest.GetContextualTuples())

	startTime := time.Now()
	resp, err := c.checkResolver.ResolveCheck(ctx, &resolveCheckRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && resolveCheckRequest.GetRequestMetadata().WasThrottled.Load() {
			return nil, nil, &ThrottledError{Cause: err}
		}

		return nil, nil, err
	}

	datastoreQueryCount := c.datastore.GetMetrics().DatastoreQueryCount
	resp.ResolutionMetadata.DatastoreQueryCount = datastoreQueryCount
	resp.ResolutionMetadata.Duration = time.Since(startTime)

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

func buildCheckContext(ctx context.Context, typesys *typesystem.TypeSystem, datastore storage.RelationshipTupleReader, maxconcurrentreads uint32, contextualTuples []*openfgav1.TupleKey) context.Context {
	ctx = typesystem.ContextWithTypesystem(ctx, typesys)

	// TODO the order is wrong, see https://github.com/openfga/openfga/issues/1394
	ctx = storage.ContextWithRelationshipTupleReader(ctx,
		storagewrappers.NewBoundedConcurrencyTupleReader(
			storagewrappers.NewCombinedTupleReader(
				datastore,
				contextualTuples,
			),
			maxconcurrentreads,
		),
	)
	return ctx
}
