package graph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/keys"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

var _ storage.OpenFGADatastore = (*cachedDatastore)(nil)

type cachedDatastore struct {
	storage.OpenFGADatastore

	cache        storage.InMemoryCache[any]
	maxCacheSize int64
	ttl          time.Duration
}

// NewCachedDatastore returns a wrapper over a datastore...
func NewCachedDatastore(inner storage.OpenFGADatastore, cache storage.InMemoryCache[any], maxSize int64, ttl time.Duration) *cachedDatastore {
	return &cachedDatastore{
		OpenFGADatastore: inner,
		cache:            cache,
		maxCacheSize:     maxSize,
		ttl:              ttl,
	}
}

func (c *cachedDatastore) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	iter, err := c.OpenFGADatastore.ReadUsersetTuples(ctx, store, filter, options)
	if err != nil {
		return nil, err
	}

	cacheKey := fmt.Sprintf("%s/%s#%s", store, filter.Object, filter.Relation)
	// todo(jpadilla): stable sort filter.AllowedUserTypeRestrictions
	for _, userset := range filter.AllowedUserTypeRestrictions {
		if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Relation); ok {
			cacheKey += "/" + userset.GetType() + ":%#" + userset.GetRelation()
		}
		if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Wildcard); ok {
			cacheKey += "/" + userset.GetType() + ":*"
		}
	}

	return c.newCachedIterator(iter, cacheKey), nil
}

func (c *cachedDatastore) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadOptions) (storage.TupleIterator, error) {
	iter, err := c.OpenFGADatastore.Read(ctx, store, tupleKey, options)
	if err != nil {
		return nil, err
	}
	cacheKey := fmt.Sprintf("%s/%s", store, tuple.TupleKeyToString(tupleKey))
	return c.newCachedIterator(iter, cacheKey), nil
}

func (c *cachedDatastore) newCachedIterator(iter storage.TupleIterator, key string) storage.TupleIterator {
	cacheKey := cacheKeyFor(key)
	cachedResp := c.cache.Get(cacheKey)
	isCached := cachedResp != nil && !cachedResp.Expired && cachedResp.Value != nil
	if isCached {
		return storage.NewStaticTupleIterator(cachedResp.Value.([]*openfgav1.Tuple))
	}

	return &cachedIterator{
		iter:         iter,
		tks:          make([]*openfgav1.Tuple, 0, c.maxCacheSize),
		cacheKey:     cacheKey,
		cache:        c.cache,
		maxCacheSize: c.maxCacheSize,
		ttl:          c.ttl,
	}
}

// Close closes the datastore and cleans up any residual resources.
func (c *cachedDatastore) Close() {
	c.cache.Stop()
	c.OpenFGADatastore.Close()
}

type cachedIterator struct {
	iter         storage.TupleIterator
	tks          []*openfgav1.Tuple
	cacheKey     string
	cache        storage.InMemoryCache[any]
	ttl          time.Duration
	maxCacheSize int64
	closeOnce    sync.Once
}

// Next see [Iterator.Next].
func (c *cachedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	tuple, err := c.iter.Next(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrIteratorDone) {
			c.flush()
		}
		return nil, err
	}

	if len(c.tks) < int(c.maxCacheSize) {
		c.tks = append(c.tks, tuple)
	}

	return tuple, nil
}

// Stop see [Iterator.Stop].
func (c *cachedIterator) Stop() {
	c.closeOnce.Do(func() {
		// todo(jpadilla): figure out how to handle when at capacity
		if len(c.tks) == int(c.maxCacheSize) {
			c.iter.Stop()
			return
		}

		go func() {
			for {
				_, err := c.Next(context.Background())
				if err != nil {
					c.flush()
					break
				}
			}

			c.iter.Stop()
		}()
	})
}

// Head see [Iterator.Head].
func (c *cachedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return c.iter.Head(ctx)
}

func (c *cachedIterator) flush() {
	if len(c.tks) > 0 {
		c.cache.Set(c.cacheKey, c.tks, c.ttl)
		c.tks = nil
	}
}

func cacheKeyFor(s string) string {
	hasher := keys.NewCacheKeyHasher(xxhash.New())

	if err := hasher.WriteString(s); err != nil {
		return ""
	}

	return strconv.FormatUint(hasher.Key().ToUInt64(), 10)
}
