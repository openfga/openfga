package cachecontroller

import (
	"context"
	"math"
	"time"

	"go.opentelemetry.io/otel"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/pkg/storage"
)

var tracer = otel.Tracer("internal/cachecontroller")

type CacheController struct {
	ds               storage.OpenFGADatastore
	cache            storage.InMemoryCache[any]
	ttl              time.Duration
	iteratorCacheTTL time.Duration

	sf *singleflight.Group
}

func NewCacheController(ds storage.OpenFGADatastore, cache storage.InMemoryCache[any], ttl time.Duration, iteratorCacheTTL time.Duration) *CacheController {
	c := &CacheController{
		ds:               ds,
		cache:            cache,
		ttl:              ttl,
		iteratorCacheTTL: iteratorCacheTTL,
		sf:               &singleflight.Group{},
	}

	return c
}

func (c *CacheController) DetermineInvalidation(
	ctx context.Context,
	storeID string,
) time.Time {
	ctx, span := tracer.Start(ctx, "cacheController.ResolveCheck")
	defer span.End()

	cacheKey := storage.GetChangelogCacheKey(storeID)
	cacheResp := c.cache.Get(cacheKey)
	if cacheResp != nil && !cacheResp.Expired && cacheResp.Value != nil {
		entry := cacheResp.Value.(*storage.ChangelogCacheEntry)
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

func (c *CacheController) findChanges(ctx context.Context, storeID string) ([]*openfgav1.TupleChange, []byte, error) {
	opts := storage.ReadChangesOptions{
		SortDesc: true,
		Pagination: storage.PaginationOptions{
			PageSize: storage.DefaultPageSize,
			From:     "",
		}}
	return c.ds.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, opts)
}

func (c *CacheController) findChangesAndInvalidate(ctx context.Context, storeID string) (time.Time, error) {
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
		for ; idx >= 0; idx-- {
			c.invalidateIteratorCacheByObjectRelation(storeID, changes[idx].GetTupleKey().GetObject(), changes[idx].GetTupleKey().GetRelation())
		}
	}

	return entry.LastModified, nil
}

func (c *CacheController) invalidateIteratorCache(storeID string) {
	// These entries do not need to expire
	c.cache.Set(storage.GetInvalidIteratorCacheKey(storeID), &storage.InvalidEntityCacheEntry{LastModified: time.Now()}, math.MaxInt)
}

func (c *CacheController) invalidateIteratorCacheByObjectRelation(storeID, object, relation string) {
	c.cache.Set(storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, object, relation), &storage.InvalidEntityCacheEntry{LastModified: time.Now()}, c.iteratorCacheTTL)
}
