package graph

import (
	"context"
	"fmt"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/dispatcher"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"google.golang.org/protobuf/types/known/structpb"
	"log"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/karlseguin/ccache/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/keys"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
)

const (
	defaultMaxCacheSize     = 10000
	defaultCacheTTL         = 10 * time.Second
	defaultResolveNodeLimit = 25
)

var (
	checkCacheTotalCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "check_cache_total_count",
		Help:      "The total number of calls to ResolveCheck.",
	})

	checkCacheHitCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "check_cache_hit_count",
		Help:      "The total number of cache hits for ResolveCheck.",
	})
)

// CachedResolveCheckResponse is very similar to ResolveCheckResponse except we
// do not store the ResolutionData. This is due to the fact that the resolution metadata
// will be incorrect as data is served from cache instead of actual database read.
type CachedResolveCheckResponse struct {
	Allowed bool
}

func (c *CachedResolveCheckResponse) convertToResolveCheckResponse() *openfgav1.BaseResponse {
	response := openfgav1.BaseResponse_CheckResponse{CheckResponse: &openfgav1.CheckResponse{
		Allowed: c.Allowed,
	}}
	base := openfgav1.BaseResponse{BaseResponse: &response}
	return &base
}

func newCachedResolveCheckResponse(r *openfgav1.CheckResponse) *CachedResolveCheckResponse {
	return &CachedResolveCheckResponse{
		Allowed: r.Allowed,
	}
}

// CachedCheckResolver attempts to resolve check sub-problems via prior computations before
// delegating the request to some underlying CheckResolver.
type CachedCheckResolver struct {
	delegate     dispatcher.Dispatcher
	cache        *ccache.Cache[*CachedResolveCheckResponse]
	maxCacheSize int64
	cacheTTL     time.Duration
	logger       logger.Logger
	// allocatedCache is used to denote whether the cache is allocated by this struct.
	// If so, CachedCheckResolver is responsible for cleaning up.
	allocatedCache bool
}

var _ dispatcher.Dispatcher = (*CachedCheckResolver)(nil)

// CachedCheckResolverOpt defines an option that can be used to change the behavior of cachedCheckResolver
// instance.
type CachedCheckResolverOpt func(*CachedCheckResolver)

// WithMaxCacheSize sets the maximum size of the Check resolution cache. After this
// maximum size is met, then cache keys will start being evicted with an LRU policy.
func WithMaxCacheSize(size int64) CachedCheckResolverOpt {
	return func(ccr *CachedCheckResolver) {
		ccr.maxCacheSize = size
	}
}

// WithCacheTTL sets the TTL (as a duration) for any single Check cache key value.
func WithCacheTTL(ttl time.Duration) CachedCheckResolverOpt {
	return func(ccr *CachedCheckResolver) {
		ccr.cacheTTL = ttl
	}
}

// WithExistingCache sets the cache to the specified cache.
// Note that the original cache will not be stopped as it may still be used by others. It is up to the caller
// to check whether the original cache should be stopped.
func WithExistingCache(cache *ccache.Cache[*CachedResolveCheckResponse]) CachedCheckResolverOpt {
	return func(ccr *CachedCheckResolver) {
		ccr.cache = cache
	}
}

// WithLogger sets the logger for the cached check resolver
func WithLogger(logger logger.Logger) CachedCheckResolverOpt {
	return func(ccr *CachedCheckResolver) {
		ccr.logger = logger
	}
}

// NewCachedCheckResolver constructs a CheckResolver that delegates Check resolution to the provided delegate,
// but before delegating the query to the delegate a cache-key lookup is made to see if the Check sub-problem
// has already recently been computed. If the Check sub-problem is in the cache, then the response is returned
// immediately and no re-computation is necessary.
// NOTE: the ResolveCheck's resolution data will be set as the default values as we actually did no database lookup
func NewCachedCheckResolver(opts ...CachedCheckResolverOpt) *CachedCheckResolver {
	checker := &CachedCheckResolver{
		maxCacheSize: defaultMaxCacheSize,
		cacheTTL:     defaultCacheTTL,
		logger:       logger.NewNoopLogger(),
	}
	checker.delegate = checker

	for _, opt := range opts {
		opt(checker)
	}

	if checker.cache == nil {
		checker.allocatedCache = true
		checker.cache = ccache.New(
			ccache.Configure[*CachedResolveCheckResponse]().MaxSize(checker.maxCacheSize),
		)
	}

	return checker
}

// SetDelegate sets this CachedCheckResolver's dispatch delegate.
func (c *CachedCheckResolver) SetDelegate(delegate dispatcher.Dispatcher) {
	c.delegate = delegate
}

// GetDelegate returns this CachedCheckResolver's dispatch delegate.
func (c *CachedCheckResolver) GetDelegate() dispatcher.Dispatcher {
	return c.delegate
}

// Close will deallocate resource allocated by the CachedCheckResolver
// It will not deallocate cache if it has been passed in from WithExistingCache
func (c *CachedCheckResolver) Close() {
	if c.allocatedCache {
		c.cache.Stop()
		c.cache = nil
	}
}

func (c *CachedCheckResolver) Dispatch(
	ctx context.Context,
	request *openfgav1.BaseRequest,
	metadata *openfgav1.DispatchMetadata,
	additionalParameters any,
) (*openfgav1.BaseResponse, *openfgav1.DispatchMetadata, error) {
	log.Printf("Cached Dispatcher - %s running in %s", request.GetDispatchedCheckRequest(), serverconfig.ServerName)
	req := request.GetDispatchedCheckRequest()
	ctx, span := tracer.Start(ctx, "ResolveCheck")
	defer span.End()
	span.SetAttributes(attribute.String("resolver_type", "CachedCheckResolver"))
	span.SetAttributes(attribute.String("tuple_key", req.GetTupleKey().String()))

	checkCacheTotalCounter.Inc()

	result, err := structpb.NewStruct(map[string]interface{}{})
	cacheKey, err := CheckRequestCacheKey(result, req)
	if err != nil {
		c.logger.Error("cache key computation failed with error", zap.Error(err))
		telemetry.TraceError(span, err)
		return nil, nil, err
	}

	cachedResp := c.cache.Get(cacheKey)
	if cachedResp != nil && !cachedResp.Expired() {
		checkCacheHitCounter.Inc()
		span.SetAttributes(attribute.Bool("is_cached", true))
		return cachedResp.Value().convertToResolveCheckResponse(), nil, nil
	}
	span.SetAttributes(attribute.Bool("is_cached", false))

	resp, metadata, err := c.delegate.Dispatch(ctx, request, metadata, additionalParameters)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, nil, err
	}

	c.cache.Set(cacheKey, newCachedResolveCheckResponse(resp.GetCheckResponse()), c.cacheTTL)
	return resp, metadata, nil
}

// CheckRequestCacheKey converts the ResolveCheckRequest into a canonical cache key that can be
// used for Check resolution cache key lookups in a stable way.
//
// For one store and model ID, the same tuple provided with the same contextual tuples and context
// should produce the same cache key. Contextual tuple order and context parameter order is ignored,
// only the contents are compared.
func CheckRequestCacheKey(ctx *structpb.Struct, req *openfgav1.DispatchedCheckRequest) (string, error) {
	hasher := keys.NewCacheKeyHasher(xxhash.New())

	tupleKey := req.GetTupleKey()
	key := fmt.Sprintf("%s/%s/%s#%s@%s",
		req.StoreId,
		req.AuthorizationModelId,
		tupleKey.GetObject(),
		tupleKey.GetRelation(),
		tupleKey.GetUser(),
	)

	if err := hasher.WriteString(key); err != nil {
		return "", err
	}

	// here, and for context below, avoid hashing if we don't need to
	contextualTuples := req.GetContextualTuples()
	if len(contextualTuples) > 0 {
		if err := keys.NewTupleKeysHasher(contextualTuples...).Append(hasher); err != nil {
			return "", err
		}
	}

	if ctx != nil {
		err := keys.NewContextHasher(ctx).Append(hasher)
		if err != nil {
			return "", err
		}
	}

	return strconv.FormatUint(hasher.Key().ToUInt64(), 10), nil
}
