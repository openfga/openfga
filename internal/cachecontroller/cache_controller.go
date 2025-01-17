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
	"github.com/openfga/openfga/internal/utils"
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
	DetermineInvalidation(ctx context.Context, storeID string) time.Time
}

type NoopCacheController struct{}

func (c *NoopCacheController) DetermineInvalidation(_ context.Context, _ string) time.Time {
	return time.Time{}
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
	ds                    storage.OpenFGADatastore
	cache                 storage.InMemoryCache[any]
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

func (c *InMemoryCacheController) DetermineInvalidation(
	ctx context.Context,
	storeID string,
) time.Time {
	_, span := tracer.Start(ctx, "cacheController.DetermineInvalidation", trace.WithAttributes(attribute.Bool("cached", false)))
	defer span.End()
	cacheTotalCounter.Inc()

	cacheKey := storage.GetChangelogCacheKey(storeID)
	cacheResp := c.cache.Get(cacheKey)
	c.logger.Debug("InMemoryCacheController DetermineInvalidation cache hit", zap.String("store_id", storeID), zap.Bool("hit", cacheResp != nil))
	if cacheResp != nil {
		entry := cacheResp.(*storage.ChangelogCacheEntry)
		cacheHitCounter.Inc()
		span.SetAttributes(attribute.Bool("cached", true))
		return entry.LastModified
	}

	_, present := c.inflightInvalidations.LoadOrStore(storeID, struct{}{})
	if !present {
		span.SetAttributes(attribute.Bool("check_invalidation", true))
		// if the cache cannot be found, we want to invalidate entries in the background
		// so that it does not block the answer path.
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

	entry := &storage.ChangelogCacheEntry{
		LastModified: changes[0].GetTimestamp().AsTime(),
	}

	lastInvalidationOccurred := c.cache.Get(cacheKey) != nil

	// set changelog entry as soon as possible for subsequent cache
	// lookups have the entry and not have to wait on the existing singleflight group
	c.cache.Set(cacheKey, entry, c.ttl)

	timestampOfLastInvalidation := time.Now().Add(-c.ttl)

	if lastInvalidationOccurred && entry.LastModified.Before(timestampOfLastInvalidation) {
		// no new changes, no need to perform invalidations
		span.SetAttributes(attribute.Bool("invalidations", false))
		c.logger.Debug("InMemoryCacheController findChangesAndInvalidate invalidation as entry.LastModified before last verified",
			zap.String("store_id", storeID),
			zap.Time("entry.LastModified", entry.LastModified),
			zap.Time("timestampOfLastInvalidation", timestampOfLastInvalidation))

		findChangesAndInvalidateHistogram.WithLabelValues("false", utils.Bucketize(uint(len(changes)), c.changelogBuckets)).Observe(float64(time.Since(start).Milliseconds()))
		return
	}

	// need to consider there might just be 1 change
	// iterate from the oldest to most recent to determine if the last change is part of the current batch
	idx := len(changes) - 1
	for ; idx >= 0; idx-- {
		if !lastInvalidationOccurred || changes[idx].GetTimestamp().AsTime().After(timestampOfLastInvalidation) {
			break
		}
	}

	partialInvalidation := true

	// all changes are new, thus we should revoke the whole query cache
	if idx == len(changes)-1 {
		partialInvalidation = false
		c.invalidateIteratorCache(storeID)
	} else {
		// only a subset of changes are new, revoke the respective ones
		lastModified := time.Now()
		for ; idx >= 0; idx-- {
			t := changes[idx].GetTupleKey()
			c.invalidateIteratorCacheByObjectRelation(storeID, t.GetObject(), t.GetRelation(), lastModified)
			c.invalidateIteratorCacheByObjectTypeRelation(storeID, t.GetUser(), tuple.GetType(t.GetObject()), lastModified)
		}
	}

	c.logger.Debug("InMemoryCacheController findChangesAndInvalidate invalidation",
		zap.String("store_id", storeID),
		zap.Time("entry.LastModified", entry.LastModified),
		zap.Time("timestampOfLastInvalidation", timestampOfLastInvalidation),
		zap.Bool("partialInvalidation", partialInvalidation))
	span.SetAttributes(attribute.Bool("invalidations", true))
	findChangesAndInvalidateHistogram.WithLabelValues("true", utils.Bucketize(uint(len(changes)), c.changelogBuckets)).Observe(float64(time.Since(start).Milliseconds()))
}

func (c *InMemoryCacheController) invalidateIteratorCache(storeID string) {
	c.cache.Set(storage.GetInvalidIteratorCacheKey(storeID), &storage.InvalidEntityCacheEntry{LastModified: time.Now()}, math.MaxInt)
}

func (c *InMemoryCacheController) invalidateIteratorCacheByObjectRelation(storeID, object, relation string, ts time.Time) {
	// graph.storagewrapper is exclusively used for caching iterators used within check, which _always_ have object/relation defined
	// GetInvalidIteratorByObjectRelationCacheKeys returns only 1 instance
	c.cache.Set(storage.GetInvalidIteratorByObjectRelationCacheKeys(storeID, object, relation)[0], &storage.InvalidEntityCacheEntry{LastModified: ts}, c.iteratorCacheTTL)
}

func (c *InMemoryCacheController) invalidateIteratorCacheByObjectTypeRelation(storeID, user, objectType string, ts time.Time) {
	// graph.storagewrapper is exclusively used for caching iterators used within check, which _always_ have object/relation defined
	c.cache.Set(storage.GetInvalidIteratorByUserObjectTypeCacheKeys(storeID, []string{user}, objectType)[0], &storage.InvalidEntityCacheEntry{LastModified: ts}, c.iteratorCacheTTL)
}
