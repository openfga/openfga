package graph

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
)

const (
	defaultMaxCacheSize = 10000
	defaultCacheTTL     = 10 * time.Second
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

// CachedCheckResolver attempts to resolve check sub-problems via prior computations before
// delegating the request to some underlying CheckResolver.
type CachedCheckResolver struct {
	delegate CheckResolver
	cache    storage.InMemoryCache[any]
	cacheTTL time.Duration
	logger   logger.Logger
	// allocatedCache is used to denote whether the cache is allocated by this struct.
	// If so, CachedCheckResolver is responsible for cleaning up.
	allocatedCache bool
}

var _ CheckResolver = (*CachedCheckResolver)(nil)

// CachedCheckResolverOpt defines an option that can be used to change the behavior of cachedCheckResolver
// instance.
type CachedCheckResolverOpt func(*CachedCheckResolver)

// WithCacheTTL sets the TTL (as a duration) for any single Check cache key value.
func WithCacheTTL(ttl time.Duration) CachedCheckResolverOpt {
	return func(ccr *CachedCheckResolver) {
		ccr.cacheTTL = ttl
	}
}

// WithExistingCache sets the cache to the specified cache.
// Note that the original cache will not be stopped as it may still be used by others. It is up to the caller
// to check whether the original cache should be stopped.
func WithExistingCache(cache storage.InMemoryCache[any]) CachedCheckResolverOpt {
	return func(ccr *CachedCheckResolver) {
		ccr.cache = cache
	}
}

// WithLogger sets the logger for the cached check resolver.
func WithLogger(logger logger.Logger) CachedCheckResolverOpt {
	return func(ccr *CachedCheckResolver) {
		ccr.logger = logger
	}
}

// NewCachedCheckResolver constructs a CheckResolver that delegates Check resolution to the provided delegate,
// but before delegating the query to the delegate a cache-key lookup is made to see if the Check sub-problem
// has already recently been computed. If the Check sub-problem is in the cache, then the response is returned
// immediately and no re-computation is necessary.
// NOTE: the ResolveCheck's resolution data will be set as the default values as we actually did no database lookup.
func NewCachedCheckResolver(opts ...CachedCheckResolverOpt) (*CachedCheckResolver, error) {
	checker := &CachedCheckResolver{
		cacheTTL: defaultCacheTTL,
		logger:   logger.NewNoopLogger(),
	}
	checker.delegate = checker

	for _, opt := range opts {
		opt(checker)
	}

	if checker.cache == nil {
		checker.allocatedCache = true
		cacheOptions := []storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](defaultMaxCacheSize),
		}

		var err error
		checker.cache, err = storage.NewInMemoryLRUCache[any](cacheOptions...)
		if err != nil {
			return nil, err
		}
	}

	return checker, nil
}

// SetDelegate sets this CachedCheckResolver's dispatch delegate.
func (c *CachedCheckResolver) SetDelegate(delegate CheckResolver) {
	c.delegate = delegate
}

// GetDelegate returns this CachedCheckResolver's dispatch delegate.
func (c *CachedCheckResolver) GetDelegate() CheckResolver {
	return c.delegate
}

// Close will deallocate resource allocated by the CachedCheckResolver
// It will not deallocate cache if it has been passed in from WithExistingCache.
func (c *CachedCheckResolver) Close() {
	if c.allocatedCache {
		c.cache.Stop()
	}
}

type CheckResponseCacheEntry struct {
	LastModified  time.Time
	CheckResponse *ResolveCheckResponse
}

func (c *CachedCheckResolver) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	span := trace.SpanFromContext(ctx)

	cacheKey, err := CheckRequestCacheKey(req)
	if err != nil {
		c.logger.Error("cache key computation failed with error", zap.Error(err))
		telemetry.TraceError(span, err)
		return nil, err
	}

	tryCache := req.Consistency != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY

	if tryCache {
		checkCacheTotalCounter.Inc()
		if cachedResp := c.cache.Get(cacheKey); cachedResp != nil {
			res := cachedResp.(*CheckResponseCacheEntry)
			isValid := res.LastModified.After(req.LastCacheInvalidationTime)
			c.logger.Debug("CachedCheckResolver found cache key",
				zap.String("store_id", req.GetStoreID()),
				zap.String("authorization_model_id", req.GetAuthorizationModelID()),
				zap.String("tuple_key", req.GetTupleKey().String()),
				zap.Bool("isValid", isValid))

			span.SetAttributes(attribute.Bool("cached", isValid))
			if isValid {
				checkCacheHitCounter.Inc()
				// return a copy to avoid races across goroutines
				return res.CheckResponse.clone(), nil
			}
		} else {
			c.logger.Debug("CachedCheckResolver not found cache key",
				zap.String("store_id", req.GetStoreID()),
				zap.String("authorization_model_id", req.GetAuthorizationModelID()),
				zap.String("tuple_key", req.GetTupleKey().String()))
		}
	}

	// not in cache, or consistency options experimental flag is set, and consistency param set to HIGHER_CONSISTENCY
	resp, err := c.delegate.ResolveCheck(ctx, req)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	clonedResp := resp.clone()

	c.cache.Set(cacheKey, &CheckResponseCacheEntry{LastModified: time.Now(), CheckResponse: clonedResp}, c.cacheTTL)
	return resp, nil
}

func CheckRequestCacheKey(req *ResolveCheckRequest) (string, error) {
	params := &storage.CheckCacheKeyParams{
		StoreID:              req.GetStoreID(),
		AuthorizationModelID: req.GetAuthorizationModelID(),
		TupleKey:             req.GetTupleKey(),
		ContextualTuples:     req.GetContextualTuples(),
		Context:              req.GetContext(),
	}

	return storage.GetCheckCacheKey(params)
}
