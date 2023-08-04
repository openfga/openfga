package graph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/karlseguin/ccache/v3"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
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
// do not store the ResolutionData
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

// CachedCheckResolver implements the CheckResolver interface in way that attempts to resolve
// Check sub-problems via prior computations before delegating the request to some underlying
// CheckResolver.
type CachedCheckResolver struct {
	delegate     CheckResolver
	cache        *ccache.Cache[*CachedResolveCheckResponse]
	maxCacheSize int64
	cacheTTL     time.Duration
	logger       logger.Logger
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

// WithExistingCache sets the cache to the provided cache
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
		// this means there were no cache supplied
		checker.cache = ccache.New(
			ccache.Configure[*CachedResolveCheckResponse]().MaxSize(checker.maxCacheSize),
		)
	}

	return checker
}

func (c *CachedCheckResolver) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {

	checkCacheTotalCounter.Inc()

	cacheKey, err := checkRequestCacheKey(req)
	if err != nil {
		c.logger.Error("cache key computation failed with error", zap.Error(err))
		return c.delegate.ResolveCheck(ctx, req)
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
// used for Check resolution cache key lookups.
//
// We use the 'gob' package to produce a deterministic byte ordering for the contextual tuples provided
// in the request body. The same tuple provided with the same contextual tuples should produce the same
// cache key.
func checkRequestCacheKey(req *ResolveCheckRequest) (string, error) {

	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(req.GetTupleKey()); err != nil {
		return "", err
	}

	tuplesCacheKey := b.String()

	var c bytes.Buffer
	if err := gob.NewEncoder(&c).Encode(req.GetContextualTuples()); err != nil {
		return "", err
	}

	contextualTuplesCacheKey := c.String()

	key := fmt.Sprintf("%s/%s/%s/%s/%s",
		req.GetStoreID(),
		req.GetAuthorizationModelID(),
		tuple.TupleKeyToString(req.GetTupleKey()),
		tuplesCacheKey,
		contextualTuplesCacheKey,
	)

	return base64.StdEncoding.EncodeToString([]byte(key)), nil
}
