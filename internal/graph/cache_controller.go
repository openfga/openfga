package graph

import (
	"context"
	"math"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/pkg/storage"
)

type CacheController struct {
	ds storage.OpenFGADatastore

	delegate CheckResolver
	cache    storage.InMemoryCache[any]
	sf       *singleflight.Group

	ttl time.Duration
}

var _ CheckResolver = (*CacheController)(nil)

// CacheControllerResolverOpt defines an option that can be used to change the behavior of cacheControllerResolver
// instance.
type CacheControllerResolverOpt func(*CacheController)

func WithDatastore(ds storage.OpenFGADatastore) CacheControllerResolverOpt {
	return func(c *CacheController) {
		c.ds = ds
	}
}

func WithCache(cache storage.InMemoryCache[any]) CacheControllerResolverOpt {
	return func(c *CacheController) {
		c.cache = cache
	}
}

func WithTTL(ttl time.Duration) CacheControllerResolverOpt {
	return func(c *CacheController) {
		c.ttl = ttl
	}
}
func NewCacheController(opts ...CacheControllerResolverOpt) *CacheController {
	c := &CacheController{
		ttl: defaultCacheTTL,
		sf:  &singleflight.Group{},
	}
	c.delegate = c

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// SetDelegate sets this CacheController's dispatch delegate.
func (c *CacheController) SetDelegate(delegate CheckResolver) {
	c.delegate = delegate
}

// GetDelegate returns this CacheController's dispatch delegate.
func (c *CacheController) GetDelegate() CheckResolver {
	return c.delegate
}

// Close will deallocate resource allocated by the CacheController
// It will not deallocate cache if it has been passed in from WithCache.
func (c *CacheController) Close() {
}

func (c *CacheController) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "cacheController.ResolveCheck")
	defer span.End()
	if req.GetConsistency() == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return c.delegate.ResolveCheck(ctx, req)
	}

	storeID := req.GetStoreID()
	cacheKey := storage.GetChangelogCacheKey(storeID)
	cacheResp := c.cache.Get(cacheKey)
	if cacheResp != nil && !cacheResp.Expired && cacheResp.Value != nil {
		entry := cacheResp.Value.(*storage.ChangelogCacheEntry)
		// since last verified is less than our pre-defined TTL, do nothing
		req.LastChangelogTime = entry.LastModified
		return c.delegate.ResolveCheck(ctx, req)
	}

	lastModified, err, _ := c.sf.Do(storeID, func() (interface{}, error) {
		return c.findChangesAndInvalidate(ctx, storeID)
	})

	if err == nil {
		req.LastChangelogTime = lastModified.(time.Time)
	}

	return c.delegate.ResolveCheck(ctx, req)
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
	c.cache.Set(storage.GetInvalidIteratorByObjectRelationCacheKey(storeID, object, relation), nil, c.ttl)
}
