package storagewrappers

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/tuple"
)

var cachedReaderTracer = otel.Tracer("openfga/pkg/storage/storagewrappers/cached_reader")

// DefaultDrainTimeout is the default timeout for background iterator drain operations.
const DefaultDrainTimeout = 30 * time.Second

// CachedTupleReader wraps a RelationshipTupleReader to provide iterator caching.
// Cache is checked BEFORE any database call.
type CachedTupleReader struct {
	delegate     storage.RelationshipTupleReader
	cache        storage.InMemoryCache[any]
	maxSize      int // Configurable max cache entries
	ttl          time.Duration
	drainTimeout time.Duration // Timeout for background drain operations
	sf           *singleflight.Group
	wg           *sync.WaitGroup
}

// Ensure CachedTupleReader implements RelationshipTupleReader.
var _ storage.RelationshipTupleReader = (*CachedTupleReader)(nil)

// NewCachedTupleReader creates a new CachedTupleReader.
// The drainTimeout parameter controls how long background drain operations can run.
// If drainTimeout is 0, DefaultDrainTimeout (30s) is used.
func NewCachedTupleReader(
	_ context.Context, // Kept for API compatibility, but no longer used
	delegate storage.RelationshipTupleReader,
	cache storage.InMemoryCache[any],
	maxSize int,
	ttl time.Duration,
	sf *singleflight.Group,
	wg *sync.WaitGroup,
	drainTimeout time.Duration,
) *CachedTupleReader {
	if maxSize <= 0 {
		maxSize = maxCachedElements // Default to 1000
	}
	if drainTimeout <= 0 {
		drainTimeout = DefaultDrainTimeout
	}
	// Initialize a singleflight.Group for this CachedTupleReader if not provided, which
	// ensures only one cachingIterator created from this CachedTupleReader drains at a time.
	// However, other CachedTupleReaders (e.g., from concurrent requests) may duplicate the
	// draining effort; ideally, a singleflight.Group should be provided that is shared across
	// all requests (at the server level) to prevent this.
	if sf == nil {
		sf = &singleflight.Group{}
	}
	return &CachedTupleReader{
		delegate:     delegate,
		cache:        cache,
		maxSize:      maxSize,
		ttl:          ttl,
		drainTimeout: drainTimeout,
		sf:           sf,
		wg:           wg,
	}
}

// ReadUsersetTuples reads userset tuples with caching.
func (c *CachedTupleReader) ReadUsersetTuples(
	ctx context.Context,
	storeID string,
	filter storage.ReadUsersetTuplesFilter,
	opts storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	ctx, span := cachedReaderTracer.Start(ctx, "v2cache.ReadUsersetTuples",
		trace.WithAttributes(attribute.Bool("cached", false)),
	)
	defer span.End()

	// Skip cache for higher consistency
	if opts.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return c.delegate.ReadUsersetTuples(ctx, storeID, filter, opts)
	}

	// Build cache key (includes conditions)
	cacheKey := storage.ReadUsersetTuplesKey(storeID, filter)

	span.SetAttributes(
		attribute.String("object", filter.Object),
		attribute.String("relation", filter.Relation),
		attribute.StringSlice("conditions", filter.Conditions),
		attribute.Int("type_restriction_count", len(filter.AllowedUserTypeRestrictions)),
	)

	objectType, _ := tuple.SplitObject(filter.Object)

	invalidEntityKey := buildInvalidationKey(storeID, filter.Object, filter.Relation)

	// Track total cache operations (before cache check, like V1)
	v2IterCacheTotal.WithLabelValues("ReadUsersetTuples").Inc()

	// CHECK CACHE FIRST - before any database call
	if iter := c.tryGetFromCache(cacheKey, storeID, objectType, filter.Relation, "ReadUsersetTuples", []keys.Key{invalidEntityKey}, opts.SortAsc); iter != nil {
		span.SetAttributes(attribute.Bool("cached", true))
		return iter, nil
	}

	// CACHE MISS - execute database call

	dbIter, err := c.delegate.ReadUsersetTuples(ctx, storeID, filter, opts)
	if err != nil {
		return nil, err
	}

	// Return caching iterator
	return newCachingIterator(
		dbIter, c.cache, cacheKey, c.maxSize, c.ttl, c.drainTimeout,
		c.sf, c.wg, objectType, filter.Relation, "ReadUsersetTuples",
	), nil
}

// Read reads tuples with caching.
func (c *CachedTupleReader) Read(
	ctx context.Context,
	storeID string,
	filter storage.ReadFilter,
	opts storage.ReadOptions,
) (storage.TupleIterator, error) {
	ctx, span := cachedReaderTracer.Start(ctx, "v2cache.Read",
		trace.WithAttributes(attribute.Bool("cached", false)),
	)
	defer span.End()

	if opts.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return c.delegate.Read(ctx, storeID, filter, opts)
	}

	cacheKey := storage.ReadKey(storeID, filter)

	span.SetAttributes(
		attribute.String("object", filter.Object),
		attribute.String("relation", filter.Relation),
		attribute.String("user", filter.User),
		attribute.StringSlice("conditions", filter.Conditions),
	)

	objectType, _ := tuple.SplitObject(filter.Object)
	invalidEntityKey := buildInvalidationKey(storeID, filter.Object, filter.Relation)

	// Track total cache operations (before cache check, like V1)
	v2IterCacheTotal.WithLabelValues("Read").Inc()

	if iter := c.tryGetFromCache(cacheKey, storeID, objectType, filter.Relation, "Read", []keys.Key{invalidEntityKey}, opts.SortAsc); iter != nil {
		span.SetAttributes(attribute.Bool("cached", true))
		return iter, nil
	}

	// CACHE MISS - execute database call

	dbIter, err := c.delegate.Read(ctx, storeID, filter, opts)
	if err != nil {
		return nil, err
	}

	return newCachingIterator(
		dbIter, c.cache, cacheKey, c.maxSize, c.ttl, c.drainTimeout,
		c.sf, c.wg, objectType, filter.Relation, "Read",
	), nil
}

// ReadStartingWithUser reads tuples starting with a user, with caching.
func (c *CachedTupleReader) ReadStartingWithUser(
	ctx context.Context,
	storeID string,
	filter storage.ReadStartingWithUserFilter,
	opts storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	ctx, span := cachedReaderTracer.Start(ctx, "v2cache.ReadStartingWithUser",
		trace.WithAttributes(attribute.Bool("cached", false)),
	)
	defer span.End()

	if opts.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return c.delegate.ReadStartingWithUser(ctx, storeID, filter, opts)
	}

	cacheKey := storage.ReadStartingWithUserKey(storeID, filter)

	span.SetAttributes(
		attribute.String("object_type", filter.ObjectType),
		attribute.String("relation", filter.Relation),
		attribute.StringSlice("conditions", filter.Conditions),
		attribute.Int("user_filter_count", len(filter.UserFilter)),
	)

	invalidEntityKeys := buildInvalidationKeysForUser(storeID, filter.UserFilter, filter.ObjectType)

	// Track total cache operations (before cache check, like V1)
	v2IterCacheTotal.WithLabelValues("ReadStartingWithUser").Inc()

	if iter := c.tryGetFromCache(cacheKey, storeID, filter.ObjectType, filter.Relation, "ReadStartingWithUser", invalidEntityKeys, opts.SortAsc); iter != nil {
		span.SetAttributes(attribute.Bool("cached", true))
		return iter, nil
	}

	// CACHE MISS - execute database call

	dbIter, err := c.delegate.ReadStartingWithUser(ctx, storeID, filter, opts)
	if err != nil {
		return nil, err
	}

	return newCachingIterator(
		dbIter, c.cache, cacheKey, c.maxSize, c.ttl, c.drainTimeout,
		c.sf, c.wg, filter.ObjectType, filter.Relation, "ReadStartingWithUser",
	), nil
}

// tryGetFromCache checks for cache hit with invalidation support.
// Returns LockFreeCachedIterator if found, not invalidated, and ordered-compatible.
// Monotonic upgrade: a sorted entry satisfies unsorted requests, but an unsorted entry
// does not satisfy a sorted request (returns nil so the caller re-queries and upgrades).
func (c *CachedTupleReader) tryGetFromCache(
	cacheKey keys.Key, storeID, objectType, relation, operation string,
	invalidEntityKeys []keys.Key,
	wantSorted bool,
) storage.TupleIterator {
	entry := c.cache.Get(cacheKey)
	if entry == nil {
		return nil
	}

	cached, ok := entry.(*V2IteratorCacheEntry)
	if !ok {
		return nil
	}

	// Check store-level invalidation
	if c.isStoreInvalidated(storeID, cached.LastModified) {
		c.cache.Delete(cacheKey)
		return nil
	}

	// Check entity-level invalidation
	for _, invalidKey := range invalidEntityKeys {
		if c.isCacheEntryInvalidated(invalidKey, cached.LastModified) {
			c.cache.Delete(cacheKey)
			return nil
		}
	}

	// Monotonic upgrade: if the caller wants sorted results but the cached entry is
	// unsorted, treat as a miss. Do NOT delete — the existing unsorted entry remains
	// valid for unsorted requests. The re-query will store a sorted entry via cache.Set,
	// upgrading the entry once and satisfying both sorted and unsorted callers thereafter.
	if wantSorted && !cached.Ordered {
		return nil
	}

	v2IterCacheHits.WithLabelValues(operation).Inc()
	return NewLockFreeCachedIterator(cached.Entries, objectType, relation, cached.Ordered)
}

// isStoreInvalidated returns whether the entire store's cache has been invalidated since lastModified.
func (c *CachedTupleReader) isStoreInvalidated(storeID string, lastModified time.Time) bool {
	return c.isCacheEntryInvalidated(storage.InvalidIteratorCacheKey(storeID), lastModified)
}

// isCacheEntryInvalidated returns whether an invalidation cache entry at invalidKey was
// written after a cache entry's lastModified time, indicating the cache entry is stale.
func (c *CachedTupleReader) isCacheEntryInvalidated(invalidKey keys.Key, lastModified time.Time) bool {
	entry := c.cache.Get(invalidKey)
	if entry == nil {
		return false
	}
	invalidEntry, ok := entry.(*storage.InvalidEntityCacheEntry)
	if !ok {
		return false
	}
	return invalidEntry.LastModified.After(lastModified)
}

func buildInvalidationKey(storeID, object, relation string) keys.Key {
	return storage.InvalidIteratorByObjectRelationCacheKey(storeID, object, relation)
}

func buildInvalidationKeysForUser(storeID string, userFilters []*openfgav1.ObjectRelation, objectType string) []keys.Key {
	ks := make([]keys.Key, 0, len(userFilters))
	for _, f := range userFilters {
		var user string
		if rel := f.GetRelation(); rel != "" {
			user = f.GetObject() + "#" + rel
		} else {
			user = f.GetObject()
		}
		ks = append(ks, storage.InvalidIteratorByUserObjectTypeCacheKey(storeID, user, objectType))
	}
	return ks
}

// Delegate methods that don't need caching.

// ReadUserTuple reads a single user tuple (no caching needed).
func (c *CachedTupleReader) ReadUserTuple(ctx context.Context, store string, filter storage.ReadUserTupleFilter, opts storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	return c.delegate.ReadUserTuple(ctx, store, filter, opts)
}

// ReadPage reads a page of tuples (no caching needed).
func (c *CachedTupleReader) ReadPage(ctx context.Context, store string, filter storage.ReadFilter, opts storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	return c.delegate.ReadPage(ctx, store, filter, opts)
}
