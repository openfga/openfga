package cachecontroller

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
)

var (
	tracer = otel.Tracer("internal/cachecontroller")

	cacheTotalCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "cachecontroller_cache_total_count",
		Help:      "The total number of cachecontroller requests.",
	})

	cacheHitCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "cachecontroller_cache_hit_count",
		Help:      "The total number of cache hits from cachecontroller requests.",
	})

	findChangesAndInvalidateHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "cachecontroller_find_changes_and_invalidate_histogram",
		Help:                            "The duration (in ms) required for cache controller to find changes and invalidate labeled by whether invalidation is required and buckets of changes size.",
		Buckets:                         []float64{1, 5, 10, 25, 50, 80, 100, 150, 200, 300, 1000, 2000, 5000},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"invalidation_required", "changes_size"})
)

type CacheController interface {
	// DetermineInvalidation returns the timestamp of the last write for the specified store.
	// It may return a cached timestamp.
	DetermineInvalidation(ctx context.Context, storeID string) time.Time
}

type NoopCacheController struct{}

func (c *NoopCacheController) DetermineInvalidation(_ context.Context, _ string) time.Time {
	return time.Time{}
}

func NewNoopCacheController() CacheController {
	return &NoopCacheController{}
}

// InMemoryCacheController will invalidate cache iterator (InMemoryCache) and sub problem cache (CachedCheckResolver) entries
// that are more recent than the last write for the specified store.
// Note that the invalidation is done asynchronously, and only after a Check request is received.
// It will be eventually consistent.
type InMemoryCacheController struct {
	ds    storage.OpenFGADatastore
	cache storage.InMemoryCache[any]

	// ttl for the entry that keeps the last timestamp for a Write for a storeID.
	ttl                   time.Duration
	iteratorCacheTTL      time.Duration
	changelogBuckets      []uint
	inflightInvalidations sync.Map
}

func NewCacheController(ds storage.OpenFGADatastore, cache storage.InMemoryCache[any], ttl time.Duration, iteratorCacheTTL time.Duration) CacheController {
	c := &InMemoryCacheController{
		ds:                    ds,
		cache:                 cache,
		ttl:                   ttl,
		iteratorCacheTTL:      iteratorCacheTTL,
		changelogBuckets:      []uint{0, 25, 50, 75, 100},
		inflightInvalidations: sync.Map{},
	}

	return c
}

// DetermineInvalidation returns the timestamp of the last write for the specified store.
// It may return a cached timestamp.
// If the timestamp is not known, it will asynchronously find the last Write and store it, and also invalidate some or all entries in the cache.
func (c *InMemoryCacheController) DetermineInvalidation(
	ctx context.Context,
	storeID string,
) time.Time {
	_, span := tracer.Start(ctx, "cacheController.DetermineInvalidation", trace.WithAttributes(attribute.Bool("cached", false)))
	defer span.End()
	cacheTotalCounter.Inc()

	cacheKey := storage.GetChangelogCacheKey(storeID)
	cacheResp := c.cache.Get(cacheKey)
	if cacheResp != nil {
		entry := cacheResp.(*storage.ChangelogCacheEntry)
		cacheHitCounter.Inc()
		span.SetAttributes(attribute.Bool("cached", true))
		return entry.LastModified
	}

	// if the cache key cannot be found, we asynchronously
	// find the last Write and store it, and also invalidate some or all entries in the cache.
	_, present := c.inflightInvalidations.LoadOrStore(storeID, struct{}{})
	if !present {
		span.SetAttributes(attribute.Bool("check_invalidation", true))

		go func() {
			// we do not want to propagate context to avoid early cancellation
			// and pollute span.
			c.findChangesAndInvalidate(context.Background(), storeID, span)
			c.inflightInvalidations.Delete(storeID)
		}()
	}
	// if we cannot get lock, there is already invalidation going on.  As such,
	// we don't want to spin a new go routine to do invalidation.

	return time.Time{}
}

func (c *InMemoryCacheController) findChanges(ctx context.Context, storeID string) ([]*openfgav1.TupleChange, string, error) {
	opts := storage.ReadChangesOptions{
		SortDesc: true,
		Pagination: storage.PaginationOptions{
			PageSize: storage.DefaultPageSize,
			From:     "",
		}}
	return c.ds.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, opts)
}

func (c *InMemoryCacheController) findChangesAndInvalidate(ctx context.Context, storeID string, parentSpan trace.Span) {
	start := time.Now()
	ctx, span := tracer.Start(ctx, "cacheController.findChangesAndInvalidate")
	defer span.End()

	link := trace.LinkFromContext(ctx)
	parentSpan.AddLink(link)

	cacheKey := storage.GetChangelogCacheKey(storeID)
	// TODO: this should have a deadline since it will hold up everything if it doesn't return
	// could also be implemented as a fire and forget mechanism and subsequent requests can grab the result
	// re-evaluate at a later time.
	changes, _, err := c.findChanges(ctx, storeID)
	if err != nil {
		telemetry.TraceError(span, err)
		// do not allow any cache read until next refresh
		c.invalidateIteratorCache(storeID)
		return
	}

	lastWrite := changes[0]

	entry := &storage.ChangelogCacheEntry{
		LastModified: lastWrite.GetTimestamp().AsTime(),
	}

	lastInvalidationOccurred := c.cache.Get(cacheKey) != nil

	// set changelog entry as soon as possible so that subsequent cache lookups can find the entry
	c.cache.Set(cacheKey, entry, c.ttl)

	timestampOfLastInvalidation := time.Now().Add(-c.ttl)

	if lastInvalidationOccurred && entry.LastModified.Before(timestampOfLastInvalidation) {
		// no new changes, no need to perform invalidations
		span.SetAttributes(attribute.Bool("invalidations", false))
		findChangesAndInvalidateHistogram.WithLabelValues("false", utils.Bucketize(uint(len(changes)), c.changelogBuckets)).Observe(float64(time.Since(start).Milliseconds()))
		return
	}

	// iterate from the oldest to most recent to determine if the last change is part of the current batch
	idx := len(changes) - 1
	for ; idx >= 0; idx-- {
		if !lastInvalidationOccurred || changes[idx].GetTimestamp().AsTime().After(timestampOfLastInvalidation) {
			break
		}
	}
	// all changes happened after the last invalidation, thus we should revoke all the cached iterators for the store
	if idx == len(changes)-1 {
		c.invalidateIteratorCache(storeID)
	} else {
		// only a subset of changes are new, revoke the respective ones
		lastModified := time.Now()
		for ; idx >= 0; idx-- {
			t := changes[idx].GetTupleKey()
			c.invalidateIteratorCacheByObjectRelation(storeID, t.GetObject(), t.GetRelation(), lastModified)
			// We invalidate all iterators for the tuple's user and object type, regardless of the relation.
			c.invalidateIteratorCacheByUserAndObjectType(storeID, t.GetUser(), tuple.GetType(t.GetObject()), lastModified)
		}
	}
	span.SetAttributes(attribute.Bool("invalidations", true))
	findChangesAndInvalidateHistogram.WithLabelValues("true", utils.Bucketize(uint(len(changes)), c.changelogBuckets)).Observe(float64(time.Since(start).Milliseconds()))
}

// invalidateIteratorCache writes a new key to the cache with a very long TTL.
// An alternative implementation could delete invalid keys, but this approach is faster (see graph.findInCache).
func (c *InMemoryCacheController) invalidateIteratorCache(storeID string) {
	c.cache.Set(storage.GetInvalidIteratorCacheKey(storeID), &storage.InvalidEntityCacheEntry{LastModified: time.Now()}, math.MaxInt)
}

// invalidateIteratorCacheByObjectRelation writes a new key to the cache.
// An alternative implementation could delete invalid keys, but this approach is faster (see graph.findInCache).
func (c *InMemoryCacheController) invalidateIteratorCacheByObjectRelation(storeID, object, relation string, ts time.Time) {
	// GetInvalidIteratorByObjectRelationCacheKeys returns only 1 instance
	c.cache.Set(storage.GetInvalidIteratorByObjectRelationCacheKeys(storeID, object, relation)[0], &storage.InvalidEntityCacheEntry{LastModified: ts}, c.iteratorCacheTTL)
}

// invalidateIteratorCacheByUserAndObjectType writes a new key to the cache.
// An alternative implementation could delete invalid keys, but this approach is faster (see graph.findInCache).
func (c *InMemoryCacheController) invalidateIteratorCacheByUserAndObjectType(storeID, user, objectType string, ts time.Time) {
	c.cache.Set(storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{user}, objectType)[0], &storage.InvalidEntityCacheEntry{LastModified: ts}, c.iteratorCacheTTL)
}
