package graph

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/keys"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
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

// CacheKeyParams is all the necessary pieces to create a unique-per-check cache key.
type CacheKeyParams struct {
	StoreID          string
	AuthModelID      string
	TupleKey         *openfgav1.TupleKey
	ContextualTuples []*openfgav1.TupleKey
	Context          *structpb.Struct
}

// CachedCheckResolver attempts to resolve check sub-problems via prior computations before
// delegating the request to some underlying CheckResolver.
type CachedCheckResolver struct {
	delegate     CheckResolver
	cache        storage.InMemoryCache[any]
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
		cacheOptions := []storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](checker.maxCacheSize),
		}
		checker.cache = storage.NewInMemoryLRUCache[any](cacheOptions...)
	}

	return checker
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
			span.SetAttributes(attribute.Bool("cached", isValid))
			if isValid {
				checkCacheHitCounter.Inc()
				// return a copy to avoid races across goroutines
				return res.CheckResponse.clone(), nil
			}
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
	params := &CacheKeyParams{
		StoreID:          req.GetStoreID(),
		AuthModelID:      req.GetAuthorizationModelID(),
		TupleKey:         req.GetTupleKey(),
		ContextualTuples: req.GetContextualTuples(),
		Context:          req.GetContext(),
	}

	return GenerateCacheKey(params)
}

// GenerateCacheKey converts the elements of a Check into a canonical cache key that can be
// used for Check resolution cache key lookups in a stable way.
//
// For one store and model ID, the same tuple provided with the same contextual tuples and context
// should produce the same cache key. Contextual tuple order and context parameter order is ignored,
// only the contents are compared.
func GenerateCacheKey(params *CacheKeyParams) (string, error) {
	hasher := keys.NewCacheKeyHasher(xxhash.New())

	key := fmt.Sprintf("%s%s/%s/%s#%s@%s",
		storage.SubproblemCachePrefix, // this causes an import loop
		params.StoreID,
		params.AuthModelID,
		params.TupleKey.GetObject(),
		params.TupleKey.GetRelation(),
		params.TupleKey.GetUser(),
	)

	if err := hasher.WriteString(key); err != nil {
		return "", err
	}

	// here, and for context below, avoid hashing if we don't need to
	if len(params.ContextualTuples) > 0 {
		if err := keys.NewTupleKeysHasher(params.ContextualTuples...).Append(hasher); err != nil {
			return "", err
		}
	}

	if params.Context != nil {
		err := keys.NewContextHasher(params.Context).Append(hasher)
		if err != nil {
			return "", err
		}
	}

	return strconv.FormatUint(hasher.Key().ToUInt64(), 10), nil
}
