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
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const (
	defaultMaxConcurrentReadsForCheck = math.MaxUint32
)

// CheckCommand interface wraps the Execute method for performing a check operation.
type CheckCommand interface {
	Execute(ctx context.Context) (*graph.ResolveCheckResponse, *graph.ResolveCheckRequestMetadata, error)
}

type CheckQuery struct {
	params                     CheckCommandParams
	logger                     logger.Logger
	checkResolver              graph.CheckResolver
	sharedCheckResources       *shared.SharedDatastoreResources
	cacheSettings              config.CacheSettings
	maxConcurrentReads         uint32
	shouldCacheIterators       bool
	datastoreThrottleThreshold int
	datastoreThrottleDuration  time.Duration
	datastoreWithTupleCache    *storagewrappers.RequestStorageWrapper
}

var _ CheckCommand = (*CheckQuery)(nil)

type CheckCommandParams struct {
	StoreID          string
	TupleKey         *openfgav1.CheckRequestTupleKey
	ContextualTuples *openfgav1.ContextualTupleKeys
	Context          *structpb.Struct
	Consistency      openfgav1.ConsistencyPreference
	Typesys          *typesystem.TypeSystem
}

// CheckCommandConfig server config required to run a CheckCommand.
type CheckCommandConfig struct {
	datastore     storage.RelationshipTupleReader
	checkResolver graph.CheckResolver
	options       []CheckQueryOption
}

// NewCheckCommandConfig creates a new CheckCommandConfig.
func NewCheckCommandConfig(datastore storage.RelationshipTupleReader, checkResolver graph.CheckResolver, options ...CheckQueryOption) CheckCommandConfig {
	return CheckCommandConfig{
		datastore:     datastore,
		checkResolver: checkResolver,
		options:       options,
	}
}

// CheckCommandServerConfig holds the full server configuration including shadow mode settings.
type CheckCommandServerConfig struct {
	checkCfg  CheckCommandConfig
	shadowCfg ShadowCheckCommandConfig
}

type CheckCommandServerConfigOption func(*CheckCommandServerConfig)

func WithShadowCheckCommandConfig(shadowCfg ShadowCheckCommandConfig) CheckCommandServerConfigOption {
	return func(c *CheckCommandServerConfig) {
		c.shadowCfg = shadowCfg
	}
}

// NewCheckCommandServerConfig creates a new CheckCommandServerConfig.
func NewCheckCommandServerConfig(main CheckCommandConfig, opts ...CheckCommandServerConfigOption) CheckCommandServerConfig {
	serverConfig := CheckCommandServerConfig{
		checkCfg: main,
	}

	for _, opt := range opts {
		opt(&serverConfig)
	}
	return serverConfig
}

// NewCheckCommandWithServerConfig creates a CheckCommand that automatically handles shadow mode
// based on the server configuration. This is the main entry point that should be used
// throughout the codebase instead of the individual factory functions.
// TODO : replace public NewCheckCommand with this.
func NewCheckCommandWithServerConfig(serverConfig CheckCommandServerConfig, params CheckCommandParams) CheckCommand {
	return newCheckCommandWithShadowConfig(serverConfig.checkCfg, serverConfig.shadowCfg, params)
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

func WithCheckCommandCache(sharedCheckResources *shared.SharedDatastoreResources, cacheSettings config.CacheSettings) CheckQueryOption {
	return func(c *CheckQuery) {
		c.sharedCheckResources = sharedCheckResources
		c.cacheSettings = cacheSettings
	}
}

func WithCheckDatastoreThrottler(threshold int, duration time.Duration) CheckQueryOption {
	return func(c *CheckQuery) {
		c.datastoreThrottleThreshold = threshold
		c.datastoreThrottleDuration = duration
	}
}

func NewCheckCommand(datastore storage.RelationshipTupleReader, checkResolver graph.CheckResolver, params CheckCommandParams, opts ...CheckQueryOption) *CheckQuery {
	cmd := &CheckQuery{
		params:               params,
		logger:               logger.NewNoopLogger(),
		checkResolver:        checkResolver,
		maxConcurrentReads:   defaultMaxConcurrentReadsForCheck,
		shouldCacheIterators: false,
		cacheSettings:        config.NewDefaultCacheSettings(),
		sharedCheckResources: &shared.SharedDatastoreResources{
			CacheController: cachecontroller.NewNoopCacheController(),
		},
	}

	for _, opt := range opts {
		opt(cmd)
	}

	var ctxTupleKeys []*openfgav1.TupleKey
	if params.ContextualTuples != nil {
		ctxTupleKeys = params.ContextualTuples.GetTupleKeys()
	}
	cmd.datastoreWithTupleCache = storagewrappers.NewRequestStorageWrapperWithCache(
		datastore,
		ctxTupleKeys,
		&storagewrappers.Operation{
			Method:            apimethod.Check,
			Concurrency:       cmd.maxConcurrentReads,
			ThrottleThreshold: cmd.datastoreThrottleThreshold,
			ThrottleDuration:  cmd.datastoreThrottleDuration,
		},
		storagewrappers.DataResourceConfiguration{
			Resources:     cmd.sharedCheckResources,
			CacheSettings: cmd.cacheSettings,
		},
	)
	return cmd
}

func (c *CheckQuery) Execute(ctx context.Context) (*graph.ResolveCheckResponse, *graph.ResolveCheckRequestMetadata, error) {
	err := validateCheckRequest(c.params.Typesys, c.params.TupleKey, c.params.ContextualTuples)
	if err != nil {
		return nil, nil, err
	}

	cacheInvalidationTime := time.Time{}

	if c.params.Consistency != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		cacheInvalidationTime = c.sharedCheckResources.CacheController.DetermineInvalidationTime(ctx, c.params.StoreID)
	}

	resolveCheckRequest, err := graph.NewResolveCheckRequest(
		graph.ResolveCheckRequestParams{
			StoreID:                   c.params.StoreID,
			TupleKey:                  tuple.ConvertCheckRequestTupleKeyToTupleKey(c.params.TupleKey),
			Context:                   c.params.Context,
			ContextualTuples:          c.params.ContextualTuples,
			Consistency:               c.params.Consistency,
			LastCacheInvalidationTime: cacheInvalidationTime,
			AuthorizationModelID:      c.params.Typesys.GetAuthorizationModelID(),
		},
	)

	if err != nil {
		return nil, nil, err
	}

	ctx = typesystem.ContextWithTypesystem(ctx, c.params.Typesys)
	ctx = storage.ContextWithRelationshipTupleReader(ctx, c.datastoreWithTupleCache)

	startTime := time.Now()
	resp, err := c.checkResolver.ResolveCheck(ctx, resolveCheckRequest)
	endTime := time.Since(startTime)

	// ResolveCheck might fail half way throughout (e.g. due to a timeout) and return a nil response.
	// Partial resolution metadata is still useful for obsevability.
	// From here on, we can assume that request metadata and response are not nil even if
	// there is an error present.
	if resp == nil {
		resp = &graph.ResolveCheckResponse{
			Allowed:            false,
			ResolutionMetadata: graph.ResolveCheckResponseMetadata{},
		}
	}

	resp.ResolutionMetadata.Duration = endTime
	dsMeta := c.datastoreWithTupleCache.GetMetadata()
	resp.ResolutionMetadata.DatastoreQueryCount = dsMeta.DatastoreQueryCount
	// Until dispatch throttling is deprecated, merge the results of both
	resolveCheckRequest.GetRequestMetadata().WasThrottled.CompareAndSwap(false, dsMeta.WasThrottled)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && resolveCheckRequest.GetRequestMetadata().WasThrottled.Load() {
			return resp, resolveCheckRequest.GetRequestMetadata(), &ThrottledError{Cause: err}
		}

		return resp, resolveCheckRequest.GetRequestMetadata(), err
	}

	return resp, resolveCheckRequest.GetRequestMetadata(), nil
}

func validateCheckRequest(typesys *typesystem.TypeSystem, tupleKey *openfgav1.CheckRequestTupleKey, contextualTuples *openfgav1.ContextualTupleKeys) error {
	if typesys == nil {
		return errors.New("typesystem is required")
	}
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
