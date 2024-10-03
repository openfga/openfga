package graph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cespare/xxhash/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/keys"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

var (
	// Ensures that Datastore implements the OpenFGADatastore interface.
	_ storage.OpenFGADatastore = (*CachedDatastore)(nil)

	tuplesCacheTotalCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "tuples_cache_total_count",
		Help:      "The total number of created cached iterator instances.",
	})

	tuplesCacheHitCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "tuples_cache_hit_count",
		Help:      "The total number of cache hits from cached iterator instances.",
	})

	tuplesCacheDiscardCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "tuples_cache_discard_count",
		Help:      "The total number of discards from cached iterator instances.",
	})

	tuplesCacheSizeHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "tuples_cache_size",
		Help:                            "The number of tuples cached.",
		Buckets:                         []float64{1, 10, 100, 1000, 5000, 10000},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})
)

const (
	QueryCachePrefix = "qc."
)

// iterFunc is a function closure that returns an iterator
// from the underlying datastore.
type iterFunc func(ctx context.Context) (storage.TupleIterator, error)

type CachedDatastore struct {
	storage.OpenFGADatastore

	cache         storage.InMemoryCache[any]
	maxResultSize int
	ttl           time.Duration
	sf            *singleflight.Group
}

// NewCachedDatastore returns a wrapper over a datastore that caches iterators in memory.
func NewCachedDatastore(
	inner storage.OpenFGADatastore,
	cache storage.InMemoryCache[any],
	maxSize int,
	ttl time.Duration,
) *CachedDatastore {
	return &CachedDatastore{
		OpenFGADatastore: inner,
		cache:            cache,
		maxResultSize:    maxSize,
		ttl:              ttl,
		sf:               &singleflight.Group{},
	}
}

// ReadUsersetTuples see [storage.RelationshipTupleReader].ReadUsersetTuples.
func (c *CachedDatastore) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
	options storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(
		ctx,
		"cache.ReadUsersetTuples",
		trace.WithAttributes(attribute.Bool("cached", false)),
	)
	defer span.End()

	iter := func(ctx context.Context) (storage.TupleIterator, error) {
		return c.OpenFGADatastore.ReadUsersetTuples(ctx, store, filter, options)
	}

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return iter(ctx)
	}

	var b strings.Builder
	b.WriteString(
		fmt.Sprintf("%srut/%s/%s#%s", QueryCachePrefix, store, filter.Object, filter.Relation),
	)

	var rb strings.Builder
	var wb strings.Builder

	for _, userset := range filter.AllowedUserTypeRestrictions {
		if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Relation); ok {
			rb.WriteString(fmt.Sprintf("/%s#%s", userset.GetType(), userset.GetRelation()))
		}
		if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Wildcard); ok {
			wb.WriteString(fmt.Sprintf("/%s:*", userset.GetType()))
		}
	}

	// wildcard should have precedence
	if wb.Len() > 0 {
		b.WriteString(wb.String())
	}

	if rb.Len() > 0 {
		b.WriteString(rb.String())
	}

	return c.newCachedIterator(ctx, iter, b.String())
}

// Read see [storage.RelationshipTupleReader].Read.
func (c *CachedDatastore) Read(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
	options storage.ReadOptions,
) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(
		ctx,
		"cache.Read",
		trace.WithAttributes(attribute.Bool("cached", false)),
	)
	defer span.End()

	iter := func(ctx context.Context) (storage.TupleIterator, error) {
		return c.OpenFGADatastore.Read(ctx, store, tupleKey, options)
	}

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return iter(ctx)
	}

	var b strings.Builder
	b.WriteString(
		fmt.Sprintf("%sr%s/%s", QueryCachePrefix, store, tuple.TupleKeyToString(tupleKey)),
	)
	return c.newCachedIterator(ctx, iter, b.String())
}

// newCachedIterator either returns a cached static iterator for a cache hit, or
// returns a new iterator that attempts to cache the results.
func (c *CachedDatastore) newCachedIterator(
	ctx context.Context,
	dsIterFunc iterFunc,
	key string,
) (storage.TupleIterator, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("cached_key", key))
	tuplesCacheTotalCounter.Inc()

	cacheKey := cacheKeyFor(key)

	if cacheKey == "" {
		return dsIterFunc(ctx)
	}

	cachedResp := c.cache.Get(cacheKey)
	isCached := cachedResp != nil && !cachedResp.Expired && cachedResp.Value != nil

	if isCached {
		tuplesCacheHitCounter.Inc()
		span.SetAttributes(attribute.Bool("cached", true))
		return storage.NewStaticTupleIterator(cachedResp.Value.([]*openfgav1.Tuple)), nil
	}

	iter, err := dsIterFunc(ctx)
	if err != nil {
		return nil, err
	}

	return &cachedIterator{
		iter:          iter,
		tuples:        make([]*openfgav1.Tuple, 0, c.maxResultSize),
		cacheKey:      cacheKey,
		cache:         c.cache,
		maxResultSize: c.maxResultSize,
		ttl:           c.ttl,
		sf:            c.sf,
	}, nil
}

// Close closes the datastore and cleans up any residual resources.
func (c *CachedDatastore) Close() {
	c.OpenFGADatastore.Close()
}

type cachedIterator struct {
	iter     storage.TupleIterator
	tuples   []*openfgav1.Tuple
	cacheKey string
	cache    storage.InMemoryCache[any]
	ttl      time.Duration

	// maxResultSize is the maximum number of tuples to cache. If the number
	// of tuples found exceeds this value, it will not be cached.
	maxResultSize int

	// sf is used to prevent draining the same iterator
	// across multiple requests.
	sf *singleflight.Group

	// closeOnce is used to synchronize .Close() and ensure
	// it's only done once.
	closeOnce sync.Once

	// wg is used purely for testing and is an internal detail.
	wg sync.WaitGroup
}

// Next will return the next available tuple from the underlying iterator and
// will attempt to add to buffer if not yet full. To set buffered tuples in cache,
// you must call .Stop().
func (c *cachedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	t, err := c.iter.Next(ctx)
	if err != nil {
		if !errors.Is(err, storage.ErrIteratorDone) &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			c.tuples = nil // don't store results that are incomplete
		}
		return nil, err
	}

	c.addToBuffer(t)

	return t, nil
}

// Stop terminates iteration over the underlying iterator.
//   - If there are incomplete results, they will not cached.
//   - If the iterator is already fully consumed, it will be cached in the foreground.
//   - If the iterator is not fully consumed, it will be drained in the background,
//     and attempt will be made to cache its results.
func (c *cachedIterator) Stop() {
	c.closeOnce.Do(func() {
		if c.tuples == nil {
			c.iter.Stop()
			return
		}
		// prevent goroutine if iterator was already consumed
		ctx := context.Background()
		if _, err := c.iter.Head(ctx); errors.Is(err, storage.ErrIteratorDone) {
			c.flush()
			c.iter.Stop()
			return
		}

		c.wg.Add(1)
		go func() {
			defer c.iter.Stop()
			defer c.wg.Done()
			// prevent draining on the same iterator across multiple requests
			_, _, _ = c.sf.Do(c.cacheKey, func() (interface{}, error) {
				for {
					// attempt to drain the iterator to have it ready for subsequent calls
					t, err := c.iter.Next(ctx)
					if err != nil {
						if errors.Is(err, storage.ErrIteratorDone) {
							c.flush()
						}
						break
					}
					// if the size is exceeded we don't add anymore and exit
					if !c.addToBuffer(t) {
						break
					}
				}
				return nil, nil
			})
		}()
	})
}

// Head see [storage.Iterator].Head.
func (c *cachedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return c.iter.Head(ctx)
}

// addToBuffer adds a tuple to the buffer if not yet full.
func (c *cachedIterator) addToBuffer(t *openfgav1.Tuple) bool {
	if c.tuples == nil {
		return false
	}
	c.tuples = append(c.tuples, t)
	if len(c.tuples) >= c.maxResultSize {
		tuplesCacheDiscardCounter.Inc()
		c.tuples = nil
	}
	return true
}

// flush will store copy of buffered tuples into cache.
func (c *cachedIterator) flush() {
	if c.tuples == nil {
		return
	}

	// Copy tuples buffer into new destination before storing into cache
	// otherwise, the cache will be storing pointers. This should also help
	// with garbage collection.
	tuples := make([]*openfgav1.Tuple, len(c.tuples))
	copy(tuples, c.tuples)

	c.cache.Set(c.cacheKey, tuples, c.ttl)

	tuplesCacheSizeHistogram.Observe(float64(len(tuples)))
}

func cacheKeyFor(s string) string {
	hasher := keys.NewCacheKeyHasher(xxhash.New())

	if err := hasher.WriteString(s); err != nil {
		return ""
	}

	return strconv.FormatUint(hasher.Key().ToUInt64(), 10)
}
