package graph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

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

type CachedDatastore struct {
	storage.OpenFGADatastore

	cache         storage.InMemoryCache[any]
	maxResultSize int
	ttl           time.Duration
}

// NewCachedDatastore returns a wrapper over a datastore...
func NewCachedDatastore(inner storage.OpenFGADatastore, cache storage.InMemoryCache[any], maxSize int, ttl time.Duration) *CachedDatastore {
	return &CachedDatastore{
		OpenFGADatastore: inner,
		cache:            cache,
		maxResultSize:    maxSize,
		ttl:              ttl,
	}
}

func (c *CachedDatastore) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "cache.ReadUsersetTuples", trace.WithAttributes(attribute.Bool("cached", false)))
	defer span.End()

	iter := func(ctx context.Context) (storage.TupleIterator, error) {
		return c.OpenFGADatastore.ReadUsersetTuples(ctx, store, filter, options)
	}

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return iter(ctx)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("%srut/%s/%s#%s", QueryCachePrefix, store, filter.Object, filter.Relation))

	var rb strings.Builder
	var wb strings.Builder

	for _, userset := range filter.AllowedUserTypeRestrictions {
		if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Relation); ok {
			rb.WriteString(fmt.Sprintf("/%s:%%#%s", userset.GetType(), userset.GetRelation()))
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

func (c *CachedDatastore) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadOptions) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "cache.Read", trace.WithAttributes(attribute.Bool("cached", false)))
	defer span.End()

	iter := func(ctx context.Context) (storage.TupleIterator, error) {
		return c.OpenFGADatastore.Read(ctx, store, tupleKey, options)
	}

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return iter(ctx)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("%sr%s/%s", QueryCachePrefix, store, tuple.TupleKeyToString(tupleKey)))
	return c.newCachedIterator(ctx, iter, b.String())
}

type iterFunc func(ctx context.Context) (storage.TupleIterator, error)

func (c *CachedDatastore) newCachedIterator(ctx context.Context, dsIterFunc iterFunc, key string) (storage.TupleIterator, error) {
	span := trace.SpanFromContext(ctx)
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
	}, nil
}

// Close closes the datastore and cleans up any residual resources.
func (c *CachedDatastore) Close() {
	c.cache.Stop()
	c.OpenFGADatastore.Close()
}

type cachedIterator struct {
	iter          storage.TupleIterator
	tuples        []*openfgav1.Tuple
	cacheKey      string
	cache         storage.InMemoryCache[any]
	ttl           time.Duration
	maxResultSize int
	closeOnce     sync.Once
	isClosed      bool
}

func (c *cachedIterator) addToBuffer(t *openfgav1.Tuple) bool {
	if c.tuples == nil {
		return false
	}
	c.tuples = append(c.tuples, t)
	if len(c.tuples) >= c.maxResultSize {
		c.tuples = nil
	}
	return true
}

// Next see [Iterator.Next].
func (c *cachedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	t, err := c.iter.Next(ctx)
	if err != nil {
		if !errors.Is(err, storage.ErrIteratorDone) {
			c.tuples = nil // don't store results that are incomplete
		}
		return nil, err
	}

	c.addToBuffer(t)

	return t, nil
}

// Stop see [Iterator.Stop].
func (c *cachedIterator) Stop() {
	c.closeOnce.Do(func() {
		if c.tuples == nil {
			c.iter.Stop()
			c.isClosed = true
			return
		}
		// prevent goroutine if iterator was already consumed
		ctx := context.Background()
		if _, err := c.iter.Head(ctx); errors.Is(err, storage.ErrIteratorDone) {
			c.flush()
			c.iter.Stop()
			c.isClosed = true
			return
		}

		go func() {
			defer c.iter.Stop()
			defer func() {
				c.isClosed = true
			}()
			for {
				// attempt to drain the iterator to have it ready for subsequent calls
				t, err := c.iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						c.flush()
					}
					return
				}
				// if the size is exceeded we don't add anymore and exit
				if !c.addToBuffer(t) {
					return
				}
			}
		}()
	})
}

// Head see [Iterator.Head].
func (c *cachedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return c.iter.Head(ctx)
}

func (c *cachedIterator) flush() {
	if c.tuples == nil {
		return
	}
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
