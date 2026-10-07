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
	method       string

	// inflight tracks cache-miss reads currently in flight, keyed by the read
	// cache key. The first caller for a key becomes the "leader" and stores its
	// entry; concurrent callers ("followers") that miss the cache while the leader
	// is in flight block on the entry until the leader settles, then serve from the
	// cache the leader populated. This coalesces the recursive resolver's concurrent
	// fan-out of the same tupleset to a single datastore read. See missRead.
	inflight sync.Map // map[string]*inflightRead
}

// inflightRead coordinates followers waiting on a leader's cache-miss read.
type inflightRead struct {
	// done is closed exactly once, when the leader has reached a terminal state
	// (cache populated, or caching abandoned). Followers block on it, then check
	// the cache.
	done chan struct{}
}

// Ensure CachedTupleReader implements RelationshipTupleReader.
var _ storage.RelationshipTupleReader = (*CachedTupleReader)(nil)

type CachedTupleReaderOpt func(*CachedTupleReader)

// WithMethod is used in metric differentiation to tell us which caller (e.g. Check) this is.
func WithMethod(method string) CachedTupleReaderOpt {
	return func(c *CachedTupleReader) {
		c.method = method
	}
}

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
	opts ...CachedTupleReaderOpt,
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
	c := &CachedTupleReader{
		delegate:     delegate,
		cache:        cache,
		maxSize:      maxSize,
		ttl:          ttl,
		drainTimeout: drainTimeout,
		sf:           sf,
		wg:           wg,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
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
	tuplesCacheTotalCounter.WithLabelValues("ReadUsersetTuples", c.method).Inc()

	// CHECK CACHE FIRST - before any database call
	if iter := c.tryGetFromCache(cacheKey, storeID, objectType, filter.Relation, "ReadUsersetTuples", []keys.Key{invalidEntityKey}); iter != nil {
		span.SetAttributes(attribute.Bool("cached", true))
		return iter, nil
	}

	// CACHE MISS - coalesce concurrent identical reads so the recursive resolver's
	// fan-out of the same tupleset results in a single datastore read. The leader
	// still returns a streaming CachingIterator; only concurrent followers wait.
	return c.missRead(ctx, cacheKey,
		func() storage.TupleIterator {
			return c.tryGetFromCache(cacheKey, storeID, objectType, filter.Relation, "ReadUsersetTuples", []keys.Key{invalidEntityKey})
		},
		func(readCtx context.Context) (storage.TupleIterator, error) {
			return c.delegate.ReadUsersetTuples(readCtx, storeID, filter, opts)
		},
		objectType, filter.Relation, "ReadUsersetTuples",
	)
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
	tuplesCacheTotalCounter.WithLabelValues("Read", c.method).Inc()

	if iter := c.tryGetFromCache(cacheKey, storeID, objectType, filter.Relation, "Read", []keys.Key{invalidEntityKey}); iter != nil {
		span.SetAttributes(attribute.Bool("cached", true))
		return iter, nil
	}

	// CACHE MISS - coalesce concurrent identical reads so the recursive resolver's
	// fan-out of the same tupleset results in a single datastore read. The leader
	// still returns a streaming CachingIterator; only concurrent followers wait.
	return c.missRead(ctx, cacheKey,
		func() storage.TupleIterator {
			return c.tryGetFromCache(cacheKey, storeID, objectType, filter.Relation, "Read", []keys.Key{invalidEntityKey})
		},
		func(readCtx context.Context) (storage.TupleIterator, error) {
			return c.delegate.Read(readCtx, storeID, filter, opts)
		},
		objectType, filter.Relation, "Read",
	)
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
	tuplesCacheTotalCounter.WithLabelValues("ReadStartingWithUser", c.method).Inc()

	if iter := c.tryGetFromCache(cacheKey, storeID, filter.ObjectType, filter.Relation, "ReadStartingWithUser", invalidEntityKeys); iter != nil {
		span.SetAttributes(attribute.Bool("cached", true))
		return iter, nil
	}

	// CACHE MISS - coalesce concurrent identical reads so the recursive resolver's
	// fan-out of the same tupleset results in a single datastore read. The leader
	// still returns a streaming CachingIterator; only concurrent followers wait.
	return c.missRead(ctx, cacheKey,
		func() storage.TupleIterator {
			return c.tryGetFromCache(cacheKey, storeID, filter.ObjectType, filter.Relation, "ReadStartingWithUser", invalidEntityKeys)
		},
		func(readCtx context.Context) (storage.TupleIterator, error) {
			return c.delegate.ReadStartingWithUser(readCtx, storeID, filter, opts)
		},
		filter.ObjectType, filter.Relation, "ReadStartingWithUser",
	)
}

// missRead handles a cache miss with follower coalescing.
//
// The recursive check resolver fans out many identical reads of the same tupleset
// (e.g. workgroup:X#child) concurrently. Without coalescing, each races the
// not-yet-populated cache, misses, and hits the datastore — the intra-request read
// amplification observed in production.
//
// The first caller for a key becomes the "leader": it performs the read and
// returns a streaming CachingIterator to its OWN caller, populating the cache as a
// side effect (eager flush on exhaustion, or background drain on Stop). Crucially
// the leader keeps streaming — its read is not converted into a blocking full
// materialization — so short-circuit evaluation (e.g. a union operand answering
// early) is preserved. Concurrent callers that miss while the leader is in flight
// become "followers": they block until the leader settles, then serve from the
// cache the leader populated. If the leader did not cache (result exceeded maxSize,
// or its read failed), followers fall back to their own direct streaming read.
func (c *CachedTupleReader) missRead(
	ctx context.Context,
	cacheKey keys.Key,
	getFromCache func() storage.TupleIterator,
	doRead func(context.Context) (storage.TupleIterator, error),
	objectType, relation, operation string,
) (storage.TupleIterator, error) {
	keyStr := cacheKey.String()
	entry := &inflightRead{done: make(chan struct{})}

	if existing, loaded := c.inflight.LoadOrStore(keyStr, entry); loaded {
		// Follower: a leader is (or was) reading this key. Wait for it to settle,
		// then serve from the cache it populated. Because the inflight entry is
		// retained for the reader's (request's) lifetime, a straggler that arrives
		// AFTER the leader already settled still finds the entry, observes done as
		// already closed, and serves from cache — instead of racing a not-yet-seen
		// deletion and issuing a redundant read (the TOCTOU gap a delete-on-settle
		// scheme would leave open between the caller's cache-miss check and this
		// LoadOrStore).
		leader := existing.(*inflightRead)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-leader.done:
		}
		if iter := getFromCache(); iter != nil {
			return iter, nil
		}
		// Leader settled without caching (result too large, or read error). Fall
		// back to a direct, non-coalesced streaming read of our own.
		return c.streamingMiss(ctx, cacheKey, doRead, objectType, relation, operation)
	}

	// Leader: perform the read. On failure, close done so followers stop waiting
	// and fall back. The inflight entry is deliberately NOT removed: the map is
	// scoped to a single request (a fresh CachedTupleReader is built per Check —
	// see CheckQueryV2.resolve), so retaining it bounds coalescing to the
	// intra-request window while guaranteeing stragglers coalesce onto the cache
	// rather than re-reading.
	dbIter, err := doRead(ctx)
	if err != nil {
		close(entry.done)
		return nil, err
	}

	// Return a streaming caching iterator wired to close done once it reaches a
	// terminal state (cache populated via eager flush / background drain, or
	// caching abandoned). Followers block on done until then. The iterator invokes
	// onSettled at most once (guarded by settleOnce), so done is closed exactly
	// once on this path.
	return newCachingIterator(
		dbIter, c.cache, cacheKey, c.maxSize, c.ttl, c.drainTimeout,
		c.sf, c.wg, objectType, relation, operation, c.method,
		withOnSettled(func() { close(entry.done) }),
	), nil
}

// streamingMiss performs a direct, non-coalesced cache-miss read and wraps it in a
// caching iterator. Used for the follower fallback path (leader did not cache).
func (c *CachedTupleReader) streamingMiss(
	ctx context.Context,
	cacheKey keys.Key,
	doRead func(context.Context) (storage.TupleIterator, error),
	objectType, relation, operation string,
) (storage.TupleIterator, error) {
	dbIter, err := doRead(ctx)
	if err != nil {
		return nil, err
	}
	return newCachingIterator(
		dbIter, c.cache, cacheKey, c.maxSize, c.ttl, c.drainTimeout,
		c.sf, c.wg, objectType, relation, operation, c.method,
	), nil
}

// tryGetFromCache checks for cache hit with invalidation support.
// Returns LockFreeCachedIterator if found and not invalidated.
func (c *CachedTupleReader) tryGetFromCache(
	cacheKey keys.Key, storeID, objectType, relation, operation string,
	invalidEntityKeys []keys.Key,
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

	tuplesCacheHitCounter.WithLabelValues(operation, c.method).Inc()
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
