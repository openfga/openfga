package graph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/karlseguin/ccache/v3"
	"github.com/openfga/openfga/pkg/tuple"
)

const (
	defaultMaxCacheSize = 10000
	defaultCacheTTL     = 10 * time.Second
)

// CachedCheckResolver implements the CheckResolver interface in way that attempts to resolve
// Check subproblems via prior computations before delegating the request to some underlying
// CheckResolver.
type CachedCheckResolver struct {
	delegate     CheckResolver
	cache        *ccache.Cache[*ResolveCheckResponse]
	maxCacheSize int64
	cacheTTL     time.Duration
}

var _ CheckResolver = (*CachedCheckResolver)(nil)

// CachedCheckResolverOpt defines an option that can be used to change the behavior of cachedCeckResolver
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

func WithExistingCache(cache *ccache.Cache[*ResolveCheckResponse]) CachedCheckResolverOpt {
	return func(ccr *CachedCheckResolver) {
		ccr.cache = cache
	}
}

// NewCachedCheckResolver constructs a CheckResolver that delegates Check resolution to the provided delegate,
// but before delegating the query to the delegate a cache-key lookup is made to see if the Check subproblem
// has already recently been computed. If the Check subproblem is in the cache, then the response is returned
// immediately and no recomputation is necessary.
func NewCachedCheckResolver(delegate CheckResolver, opts ...CachedCheckResolverOpt) *CachedCheckResolver {
	checker := &CachedCheckResolver{
		delegate:     delegate,
		maxCacheSize: defaultMaxCacheSize,
		cacheTTL:     defaultCacheTTL,
	}
	checker.cache = ccache.New(
		ccache.Configure[*ResolveCheckResponse]().MaxSize(checker.maxCacheSize),
	)

	for _, opt := range opts {
		opt(checker)
	}

	return checker
}

func (c *CachedCheckResolver) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {

	dispatchedCheckCounter.Inc()

	cacheKey, err := checkRequestCacheKey(req)
	if err != nil {
		return nil, fmt.Errorf("cache key computation failed with error: %w", err)
	}

	cachedResp := c.cache.Get(cacheKey)
	if cachedResp != nil && !cachedResp.Expired() {
		dispatchedCheckCachedCounter.Inc()
		return cachedResp.Value(), nil
	}

	resp, err := c.delegate.ResolveCheck(ctx, req)
	if err != nil {
		return nil, err
	}

	c.cache.Set(cacheKey, resp, c.cacheTTL)
	return resp, nil
}

// checkRequestCacheKey converts the ResolveCheckRequest into a canonical cache key that can be
// used for Check resolution cache key lookups.
//
// We use the 'gob' package to produce a determinstic byte ordering for the contextual tuples provided
// in the request body. The same tuple provided with the same contextual tuples should produce the same
// cache key.
func checkRequestCacheKey(req *ResolveCheckRequest) (string, error) {

	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(req.GetTupleKey()); err != nil {
		return "", err
	}

	contextualTuplesCacheKey := b.String()

	key := fmt.Sprintf("%s/%s/%s/%s",
		req.GetStoreID(),
		req.GetAuthorizationModelID(),
		tuple.TupleKeyToString(req.GetTupleKey()),
		contextualTuplesCacheKey,
	)

	return base64.StdEncoding.EncodeToString([]byte(key)), nil
}
