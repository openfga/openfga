package commands

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/openfga/openfga/internal/cachecontroller"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/middleware/validator"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"

	"github.com/openfga/openfga/pkg/logger"
)

const (
	defaultResolveNodeLimit           = 25
	defaultMaxConcurrentReadsForCheck = math.MaxUint32
)

type CheckQuery struct {
	logger          logger.Logger
	checkResolver   graph.CheckResolver
	typesys         *typesystem.TypeSystem
	datastore       storage.RelationshipTupleReader
	cacheController cachecontroller.CacheController

	resolveNodeLimit   uint32
	maxConcurrentReads uint32
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
		datastore:          datastore,
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

func (c *CheckQuery) Execute(ctx context.Context, req *openfgav1.CheckRequest) (*graph.ResolveCheckResponse, *graph.ResolveCheckRequestMetadata, error) {
	err := validateCheckRequest(ctx, req, c.typesys)
	if err != nil {
		return nil, nil, err
	}

	cacheInvalidationTime := time.Time{}

	if req.GetConsistency() != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		cacheInvalidationTime = c.cacheController.DetermineInvalidation(ctx, req.GetStoreId())
	}

	resolveCheckRequest := graph.ResolveCheckRequest{
		StoreID:              req.GetStoreId(),
		AuthorizationModelID: c.typesys.GetAuthorizationModelID(), // the resolved model ID
		TupleKey:             tuple.ConvertCheckRequestTupleKeyToTupleKey(req.GetTupleKey()),
		ContextualTuples:     req.GetContextualTuples().GetTupleKeys(),
		Context:              req.GetContext(),
		VisitedPaths:         make(map[string]struct{}),
		RequestMetadata:      graph.NewCheckRequestMetadata(c.resolveNodeLimit),
		Consistency:          req.GetConsistency(),
		// avoid having to read from cache consistently by propagating it
		LastCacheInvalidationTime: cacheInvalidationTime,
	}

	// check request: {StoreID:01J9Y7T0M8JWC147V5ZBE83AS2
	// AuthorizationModelID:01J9Y7T0MA99T6WWX4MM4148YE TupleKey:user:"user:anne"
	// relation:"admin" object:"repo:openfga" ContextualTuples:[]
	// Context:<nil> RequestMetadata:0x140009d4be8 VisitedPaths:map[] Consistency:UNSPECIFIED}
	//c.logger.Warn(fmt.Sprintf("justin check request: %+v", resolveCheckRequest))

	ctx = buildCheckContext(ctx, c.typesys, c.datastore, c.maxConcurrentReads, resolveCheckRequest.GetContextualTuples())

	c.logger.Warn(fmt.Sprintf("what type of check resolver? %+v", c.checkResolver))
	resp, err := c.checkResolver.ResolveCheck(ctx, &resolveCheckRequest)
	if err != nil {
		return nil, nil, translateError(resolveCheckRequest.GetRequestMetadata(), err)
	}
	return resp, resolveCheckRequest.GetRequestMetadata(), nil
}

func validateCheckRequest(ctx context.Context, req *openfgav1.CheckRequest, typesys *typesystem.TypeSystem) error {
	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}

	// The input tuple Key should be validated loosely.
	if err := validation.ValidateUserObjectRelation(typesys, tuple.ConvertCheckRequestTupleKeyToTupleKey(req.GetTupleKey())); err != nil {
		return serverErrors.ValidationError(err)
	}

	// But contextual tuples need to be validated more strictly, the same as an input to a Write Tuple request.
	for _, ctxTuple := range req.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTupleForWrite(typesys, ctxTuple); err != nil {
			return serverErrors.HandleTupleValidateError(err)
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

func translateError(reqMetadata *graph.ResolveCheckRequestMetadata, err error) error {
	if errors.Is(err, graph.ErrResolutionDepthExceeded) {
		return serverErrors.AuthorizationModelResolutionTooComplex
	}

	if errors.Is(err, condition.ErrEvaluationFailed) {
		return serverErrors.ValidationError(err)
	}

	if errors.Is(err, context.DeadlineExceeded) && reqMetadata.WasThrottled.Load() {
		return serverErrors.ThrottledTimeout
	}

	return serverErrors.HandleError("", err)
}
