package graph

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/karlseguin/ccache/v3"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

const (
	defaultMaxCacheSize     = 10000
	defaultCacheTTL         = 10 * time.Second
	defaultResolveNodeLimit = 25
)

var (
	checkCacheTotalCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "check_cache_total_count",
		Help: "The total number of calls to ResolveCheck.",
	})

	checkCacheHitCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "check_cache_hit_count",
		Help: "The total number of cache hits for ResolveCheck.",
	})
)

// CachedResolveCheckResponse is very similar to ResolveCheckResponse except we
// do not store the ResolutionData. This is due to the fact that the resolution metadata
// will be incorrect as data is served from cache instead of actual database read.
type CachedResolveCheckResponse struct {
	Allowed bool
}

func (c *CachedResolveCheckResponse) convertToResolveCheckResponse() *ResolveCheckResponse {
	return &ResolveCheckResponse{
		Allowed: c.Allowed,
		ResolutionMetadata: &ResolutionMetadata{
			Depth:               defaultResolveNodeLimit,
			DatastoreQueryCount: 0,
		},
	}
}

func newCachedResolveCheckResponse(r *ResolveCheckResponse) *CachedResolveCheckResponse {
	return &CachedResolveCheckResponse{
		Allowed: r.Allowed,
	}
}

// CachedCheckResolver attempts to resolve check sub-problems via prior computations before
// delegating the request to some underlying CheckResolver.
type CachedCheckResolver struct {
	delegate     CheckResolver
	cache        *ccache.Cache[*CachedResolveCheckResponse]
	maxCacheSize int64
	cacheTTL     time.Duration
	logger       logger.Logger
	// allocatedCache is used to denote whether the cache is allocated by this struct.
	// If so, CachedCheckResolver is responsible for cleaning up.
	allocatedCache bool
}

var _ CheckResolver = (*CachedCheckResolver)(nil)

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
func NewCachedCheckResolver(delegate CheckResolver, opts ...CachedCheckResolverOpt) *CachedCheckResolver {
	checker := &CachedCheckResolver{
		delegate:     delegate,
		maxCacheSize: defaultMaxCacheSize,
		cacheTTL:     defaultCacheTTL,
		logger:       logger.NewNoopLogger(),
	}

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

// Close will deallocate resource allocated by the CachedCheckResolver
// It will not deallocate cache if it has been passed in from WithExistingCache
func (c *CachedCheckResolver) Close() {
	if c.allocatedCache {
		c.cache.Stop()
		c.cache = nil
	}
}

func (c *CachedCheckResolver) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	checkCacheTotalCounter.Inc()

	cacheKey, err := checkRequestCacheKey(req)
	if err != nil {
		c.logger.Error("cache key computation failed with error", zap.Error(err))
		return nil, err
	}

	cachedResp := c.cache.Get(cacheKey)
	if cachedResp != nil && !cachedResp.Expired() {
		checkCacheHitCounter.Inc()
		return cachedResp.Value().convertToResolveCheckResponse(), nil
	}

	resp, err := c.delegate.ResolveCheck(ctx, req)
	if err != nil {
		return nil, err
	}

	c.cache.Set(cacheKey, newCachedResolveCheckResponse(resp), c.cacheTTL)
	return resp, nil
}

// checkRequestCacheKey converts the ResolveCheckRequest into a canonical cache key that can be
// used for Check resolution cache key lookups in a stable way.
//
// For one store and model ID, the same tuple provided with the same contextual tuples will produce the same
// cache key. Contextual tuple order is ignored, only the contents are compared.
func checkRequestCacheKey(req *ResolveCheckRequest) (string, error) {
	hasher := NewHasher(xxhash.New())

	tupleKey := req.GetTupleKey()
	key := fmt.Sprintf("%s/%s/%s#%s@%s",
		req.GetStoreID(),
		req.GetAuthorizationModelID(),
		tupleKey.GetObject(),
		tupleKey.GetRelation(),
		tupleKey.GetUser(),
	)

	if err := hasher.WriteString(key); err != nil {
		return "", err
	}

	// here, avoid hashing if we don't need to
	if len(req.GetContextualTuples()) > 0 {
		if err := NewTupleKeysHasher(req.GetContextualTuples()...).Append(hasher); err != nil {
			return "", err
		}
	}

	return strconv.FormatUint(hasher.Key(), 10), nil
}
