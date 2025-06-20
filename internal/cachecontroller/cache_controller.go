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
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/logger"
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

	cacheInvalidationCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "cachecontroller_cache_invalidation_count",
		Help:      "The total number of invalidations performed by the cache controller.",
	})

	findChangesAndInvalidateHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "cachecontroller_find_changes_and_invalidate_histogram",
		Help:                            "The duration (in ms) required for cache controller to find changes and invalidate labeled by whether invalidation is required and buckets of changes size.",
		Buckets:                         []float64{1, 5, 10, 25, 50, 80, 100, 150, 200, 300, 1000, 2000, 5000},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"invalidation_type"})
)

type CacheController interface {
	// DetermineInvalidationTime returns the timestamp of the last write for the specified store if it was in cache,
	// Else it returns Zero time and triggers InvalidateIfNeeded().
	DetermineInvalidationTime(ctx context.Context, storeID string) time.Time

	// InvalidateIfNeeded checks to see if an invalidation is currently in progress for a store,
	// and if not it will spawn a goroutine to invalidate cached records conditionally
	// based on timestamp. It may invalidate all cache records, some, or none.
	InvalidateIfNeeded(storeID string, parentSpan trace.Span)
}

type NoopCacheController struct{}

func (c *NoopCacheController) DetermineInvalidationTime(_ context.Context, _ string) time.Time {
	return time.Time{}
}

func (c *NoopCacheController) InvalidateIfNeeded(_ string, _ trace.Span) {
}

func NewNoopCacheController() CacheController {
	return &NoopCacheController{}
}

// InMemoryCacheControllerOpt defines an option that can be used to change the behavior of InMemoryCacheController
// instance.
type InMemoryCacheControllerOpt func(*InMemoryCacheController)

// WithLogger sets the logger for InMemoryCacheController.
func WithLogger(logger logger.Logger) InMemoryCacheControllerOpt {
	return func(inm *InMemoryCacheController) {
		inm.logger = logger
	}
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
	logger                logger.Logger
}

func NewCacheController(ds storage.OpenFGADatastore, cache storage.InMemoryCache[any], ttl time.Duration, iteratorCacheTTL time.Duration, opts ...InMemoryCacheControllerOpt) CacheController {
	c := &InMemoryCacheController{
		ds:                    ds,
		cache:                 cache,
		ttl:                   ttl,
		iteratorCacheTTL:      iteratorCacheTTL,
		changelogBuckets:      []uint{0, 25, 50, 75, 100},
		inflightInvalidations: sync.Map{},
		logger:                logger.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// DetermineInvalidationTime returns the timestamp of the last write for the specified store if it was in cache,
// Else it returns Zero time and triggers InvalidateIfNeeded().
func (c *InMemoryCacheController) DetermineInvalidationTime(
	ctx context.Context,
	storeID string,
) time.Time {
	_, span := tracer.Start(ctx, "cacheController.DetermineInvalidationTime", trace.WithAttributes(attribute.Bool("cached", false)))
	defer span.End()
	cacheTotalCounter.Inc()

	cacheKey := storage.GetChangelogCacheKey(storeID)
	cacheResp := c.cache.Get(cacheKey)
	c.logger.Debug("InMemoryCacheController DetermineInvalidationTime cache attempt",
		zap.String("store_id", storeID),
		zap.Bool("hit", cacheResp != nil),
	)
	if cacheResp != nil {
		entry := cacheResp.(*storage.ChangelogCacheEntry)
		cacheHitCounter.Inc()
		span.SetAttributes(attribute.Bool("cached", true))
		return entry.LastModified
	}

	c.InvalidateIfNeeded(storeID, span)

	return time.Time{}
}

func (c *InMemoryCacheController) findChangesDescending(ctx context.Context, storeID string) ([]*openfgav1.TupleChange, string, error) {
	opts := storage.ReadChangesOptions{
		SortDesc: true,
		Pagination: storage.PaginationOptions{
			PageSize: storage.DefaultPageSize,
			From:     "",
		}}
	return c.ds.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, opts)
}

func (c *InMemoryCacheController) InvalidateIfNeeded(storeID string, span trace.Span) {
	_, present := c.inflightInvalidations.LoadOrStore(storeID, struct{}{})
	if present {
		// If invalidation is already in process, abort.
		return
	}

	span.SetAttributes(attribute.Bool("cache_controller_invalidation", true))

	go func() {
		// we do not want to propagate context to avoid early cancellation
		// and pollute span.
		c.findChangesAndInvalidateIfNecessary(context.Background(), storeID, span)
		c.inflightInvalidations.Delete(storeID)
	}()
}

// findChangesAndInvalidateIfNecessary checks the most recent entry in this store's changelog against the most
// recent cached changelog entry. If the most recent changelog entry is older than the cached changelog timestamp,
// no invalidation is necessary and we return. If not, we locate changelog records that have been around for longer
// than the cache's TTL and invalidate them.
func (c *InMemoryCacheController) findChangesAndInvalidateIfNecessary(ctx context.Context, storeID string, parentSpan trace.Span) {
	start := time.Now()
	ctx, span := tracer.Start(ctx, "cacheController.findChangesAndInvalidateIfNecessary")
	defer span.End()

	link := trace.LinkFromContext(ctx)
	parentSpan.AddLink(link)

	// TODO: this should have a deadline since it will hold up everything if it doesn't return
	// could also be implemented as a fire and forget mechanism and subsequent requests can grab the result
	// re-evaluate at a later time.
	// Note that changes are sorted most-recent first
	changes, _, err := c.findChangesDescending(ctx, storeID)
	if err != nil {
		telemetry.TraceError(span, err)
		// do not allow any cache read until next refresh
		c.invalidateIteratorCache(storeID)
		return
	}

	mostRecentChanges := changes[0]
	entry := &storage.ChangelogCacheEntry{
		LastModified: mostRecentChanges.GetTimestamp().AsTime(),
	}

	cacheKey := storage.GetChangelogCacheKey(storeID)
	lastCacheRecord := c.cache.Get(cacheKey)

	// set changelog entry as soon as possible so that subsequent cache lookups can find the entry
	c.cache.Set(cacheKey, entry, c.ttl)

	invalidationType := "none"
	timestampOfLastInvalidation := time.Time{}
	if lastCacheRecord != nil {
		decodedRecord, ok := lastCacheRecord.(*storage.ChangelogCacheEntry)
		if ok {
			// if the change log cache is available and valid, use the last modified
			// time to have better consistency. Otherwise, the timestampOfLastInvalidation will
			// be the beginning of time which imply the need to invalidate one or more records.
			timestampOfLastInvalidation = decodedRecord.LastModified
		} else {
			c.logger.Error("Unable to cast lastCacheRecord properly", zap.String("cacheKey", cacheKey))
		}
	}

	if !timestampOfLastInvalidation.After(entry.LastModified) {
		// no new changes, no need to perform invalidations
		span.SetAttributes(attribute.Bool("invalidations", false))
		c.logger.Debug("InMemoryCacheController findChangesAndInvalidateIfNecessary no invalidation as entry.LastModified before last verified",
			zap.String("store_id", storeID),
			zap.Time("entry.LastModified", entry.LastModified),
			zap.Time("timestampOfLastInvalidation", timestampOfLastInvalidation))

		findChangesAndInvalidateHistogram.WithLabelValues(invalidationType).Observe(float64(time.Since(start).Milliseconds()))
		return
	}

	timestampOfLastIteratorInvalidation := time.Now().Add(-c.iteratorCacheTTL)

	// need to consider there might just be 1 change
	// iterate from the oldest to most recent to determine if the last change is part of the current batch
	// Remember that idx[0] is the most recent change while idx[len(changes)-1] is the oldest change because
	// changes is ordered from most recent to oldest.
	idx := len(changes) - 1
	for ; idx >= 0; idx-- {
		// idx marks the changes the first change after the timestampOfLastIteratorInvalidation.
		// therefore, we want to use the changes happens at/after this time to invalidate cache.
		//
		// Note that we only want to add invalidation entries for changes with timestamp >= now - iterator cache's TTL
		// because anything older than that time would not live in the iterator cache anyway.
		if changes[idx].GetTimestamp().AsTime().After(timestampOfLastIteratorInvalidation) {
			break
		}
	}

	// all changes happened after the last invalidation, thus we should revoke all the cached iterators for the store.
	if idx == len(changes)-1 {
		cacheInvalidationCounter.Inc()
		invalidationType = "full"
		c.invalidateIteratorCache(storeID)
	} else {
		// only a subset of changes are new, revoke the respective ones.
		lastModified := time.Now()

		// only increment if we're going to enter the invalidation for loop below
		if idx >= 0 {
			cacheInvalidationCounter.Inc()
			invalidationType = "partial"
		}
		for ; idx >= 0; idx-- {
			t := changes[idx].GetTupleKey()
			c.invalidateIteratorCacheByObjectRelation(storeID, t.GetObject(), t.GetRelation(), lastModified)
			// We invalidate all iterators for the tuple's user and object type, regardless of the relation.
			c.invalidateIteratorCacheByUserAndObjectType(storeID, t.GetUser(), tuple.GetType(t.GetObject()), lastModified)
		}
	}

	c.logger.Debug("InMemoryCacheController findChangesAndInvalidateIfNecessary invalidation",
		zap.String("store_id", storeID),
		zap.Time("entry.LastModified", entry.LastModified),
		zap.Time("timestampOfLastIteratorInvalidation", timestampOfLastIteratorInvalidation),
		zap.String("invalidationType", invalidationType))
	span.SetAttributes(attribute.String("invalidationType", invalidationType))
	findChangesAndInvalidateHistogram.WithLabelValues(invalidationType).Observe(float64(time.Since(start).Milliseconds()))
}

// invalidateIteratorCache writes a new key to the cache with a very long TTL.
// An alternative implementation could delete invalid keys, but this approach is faster (see storagewrappers.findInCache).
func (c *InMemoryCacheController) invalidateIteratorCache(storeID string) {
	c.cache.Set(storage.GetInvalidIteratorCacheKey(storeID), &storage.InvalidEntityCacheEntry{LastModified: time.Now()}, math.MaxInt)
}

// invalidateIteratorCacheByObjectRelation writes a new key to the cache.
// An alternative implementation could delete invalid keys, but this approach is faster (see storagewrappers.findInCache).
func (c *InMemoryCacheController) invalidateIteratorCacheByObjectRelation(storeID, object, relation string, ts time.Time) {
	c.cache.Set(storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, object, relation), &storage.InvalidEntityCacheEntry{LastModified: ts}, c.iteratorCacheTTL)
}

// invalidateIteratorCacheByUserAndObjectType writes a new key to the cache.
// An alternative implementation could delete invalid keys, but this approach is faster (see storagewrappers.findInCache).
func (c *InMemoryCacheController) invalidateIteratorCacheByUserAndObjectType(storeID, user, objectType string, ts time.Time) {
	c.cache.Set(storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{user}, objectType)[0], &storage.InvalidEntityCacheEntry{LastModified: ts}, c.iteratorCacheTTL)
}
