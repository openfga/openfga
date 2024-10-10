package cachecontroller

import (
	"context"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"

	"go.opentelemetry.io/otel"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/pkg/storage"
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

type InMemoryCacheController struct {
	ds               storage.OpenFGADatastore
	cache            storage.InMemoryCache[any]
	ttl              time.Duration
	iteratorCacheTTL time.Duration

	sf *singleflight.Group
}

func NewCacheController(ds storage.OpenFGADatastore, cache storage.InMemoryCache[any], ttl time.Duration, iteratorCacheTTL time.Duration) CacheController {
	c := &InMemoryCacheController{
		ds:               ds,
		cache:            cache,
		ttl:              ttl,
		iteratorCacheTTL: iteratorCacheTTL,
		sf:               &singleflight.Group{},
	}

	return c
}

func (c *InMemoryCacheController) DetermineInvalidation(
	ctx context.Context,
	storeID string,
) time.Time {
	ctx, span := tracer.Start(ctx, "cacheController.DetermineInvalidation")
	defer span.End()
	cacheTotalCounter.Inc()

	cacheKey := storage.GetChangelogCacheKey(storeID)
	cacheResp := c.cache.Get(cacheKey)
	if cacheResp != nil && !cacheResp.Expired && cacheResp.Value != nil {
		entry := cacheResp.Value.(*storage.ChangelogCacheEntry)
		cacheHitCounter.Inc()
		return entry.LastModified
	}

	lastModified, err, _ := c.sf.Do(storeID, func() (interface{}, error) {
		return c.findChangesAndInvalidate(ctx, storeID)
	})

	if err != nil {
		return time.Time{}
	}

	return lastModified.(time.Time)
}

func (c *InMemoryCacheController) findChanges(ctx context.Context, storeID string) ([]*openfgav1.TupleChange, []byte, error) {
	opts := storage.ReadChangesOptions{
		SortDesc: true,
		Pagination: storage.PaginationOptions{
			PageSize: storage.DefaultPageSize,
			From:     "",
		}}
	return c.ds.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, opts)
}

func (c *InMemoryCacheController) findChangesAndInvalidate(ctx context.Context, storeID string) (time.Time, error) {
	ctx, span := tracer.Start(ctx, "cacheController.Invalidations")
	defer span.End()
	cacheKey := storage.GetChangelogCacheKey(storeID)
	// this should have a deadline since it will hold everything
	changes, _, err := c.findChanges(ctx, storeID)
	if err != nil {
		// do not allow any cache read until next refresh
		c.invalidateIteratorCache(storeID)
		return time.Time{}, err
	}

	entry := &storage.ChangelogCacheEntry{
		LastModified: changes[0].GetTimestamp().AsTime(),
	}

	// set changelog entry as soon as possible for subsequent cache
	// lookups have the entry and not have to wait on the existing singleflight group
	c.cache.Set(cacheKey, entry, c.ttl)

	lastVerified := time.Now().Add(-c.ttl)

	// need to consider there might just be 1 change
	// iterate from the oldest to most recent to determine if the last change is part of the current batch
	idx := len(changes) - 1
	for ; idx >= 0; idx-- {
		if changes[idx].GetTimestamp().AsTime().After(lastVerified) {
			break
		}
	}
	// all changes are new, thus we should revoke the whole query cache
	if idx == len(changes)-1 {
		c.invalidateIteratorCache(storeID)
	} else {
		// only a subset of changes are new, revoke the respective ones
		lastModified := time.Now()
		for ; idx >= 0; idx-- {
			c.invalidateIteratorCacheByObjectRelation(storeID, changes[idx].GetTupleKey().GetObject(), changes[idx].GetTupleKey().GetRelation(), lastModified)
		}
	}

	return entry.LastModified, nil
}

func (c *InMemoryCacheController) invalidateIteratorCache(storeID string) {
	// These entries do not need to expire
	c.cache.Set(storage.GetInvalidIteratorCacheKey(storeID), &storage.InvalidEntityCacheEntry{LastModified: time.Now()}, math.MaxInt)
}

func (c *InMemoryCacheController) invalidateIteratorCacheByObjectRelation(storeID, object, relation string, ts time.Time) {
	// graph.storagewrapper is exclusively used for caching iterators used within check, which _always_ have object/relation defined
	c.cache.Set(storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, object, relation), &storage.InvalidEntityCacheEntry{LastModified: ts}, c.iteratorCacheTTL)
}
