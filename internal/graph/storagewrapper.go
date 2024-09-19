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
	_ storage.OpenFGADatastore = (*cachedDatastore)(nil)

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
	ctx, span := tracer.Start(ctx, "ReadStartingWithUser")
	defer span.End()

	iter := func(ctx context.Context) (storage.TupleIterator, error) {
		return c.OpenFGADatastore.ReadUsersetTuples(ctx, store, filter, options)
	}

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return iter(ctx)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("%s/%s#%s", store, filter.Object, filter.Relation))

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

	if rb.Len() > 0 {
		b.WriteString(rb.String())
	}

	if wb.Len() > 0 {
		b.WriteString(wb.String())
	}

	return c.newCachedIterator(ctx, iter, b.String())
}

func (c *cachedDatastore) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadOptions) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "Read")
	defer span.End()

	iter := func(ctx context.Context) (storage.TupleIterator, error) {
		return c.OpenFGADatastore.Read(ctx, store, tupleKey, options)
	}

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return iter(ctx)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("%s/%s", store, tuple.TupleKeyToString(tupleKey)))
	return c.newCachedIterator(ctx, iter, b.String())
}

type iterFunc func(ctx context.Context) (storage.TupleIterator, error)

func (c *cachedDatastore) newCachedIterator(ctx context.Context, dsIterFunc iterFunc, key string) (storage.TupleIterator, error) {
	span := trace.SpanFromContext(ctx)

	cacheKey := cacheKeyFor(key)
	if cacheKey == "" {
		return dsIterFunc(ctx)
	}

	cachedResp := c.cache.Get(cacheKey)
	cachedIter := &cachedIterator{
		iter:         nil,
		tuples:       make([]*openfgav1.Tuple, c.maxCacheSize),
		cacheKey:     cacheKey,
		cache:        c.cache,
		maxCacheSize: c.maxCacheSize,
		isCached:     cachedResp != nil && !cachedResp.Expired && cachedResp.Value != nil,
		ttl:          c.ttl,
	}

	if cachedIter.isCached {
		cachedIter.iter = storage.NewStaticTupleIterator(cachedResp.Value.([]*openfgav1.Tuple))
	} else {
		dsIter, err := dsIterFunc(ctx)
		if err != nil {
			return nil, err
		}

		cachedIter.iter = dsIter
	}

	span.SetAttributes(attribute.Bool("is_iter_cached", cachedIter.isCached))

	return cachedIter, nil
}

// Close closes the datastore and cleans up any residual resources.
func (c *cachedDatastore) Close() {
	c.cache.Stop()
	c.OpenFGADatastore.Close()
}

type cachedIterator struct {
	pos          int
	iter         storage.TupleIterator
	tuples       []*openfgav1.Tuple
	cacheKey     string
	cache        storage.InMemoryCache[any]
	isCached     bool
	ttl          time.Duration
	maxCacheSize int64
	closeOnce    sync.Once
}

// Next see [Iterator.Next].
func (c *cachedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	tuple, err := c.iter.Next(ctx)
	if err != nil {
		return nil, err
	}

	if c.pos < int(c.maxCacheSize) {
		c.tuples[c.pos] = tuple
		c.pos++
	}

	return tuple, nil
}

// Stop see [Iterator.Stop].
func (c *cachedIterator) Stop() {
	// TODO: Determine if we need/should drain the iterator and cache in the background.
	// There are concerns with synchronization and leaking goroutines
	c.closeOnce.Do(func() {
		defer c.iter.Stop()

		tuplesCacheTotalCounter.Inc()
		if c.isCached {
			tuplesCacheHitCounter.Inc()
		}

		if c.pos >= int(c.maxCacheSize) {
			return
		}

		for {
			_, err := c.Next(context.Background())
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					c.flush()
				}
				break
			}
		}
	})
}

// Head see [Iterator.Head].
func (c *cachedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return c.iter.Head(ctx)
}

func (c *cachedIterator) flush() {
	if !c.isCached && c.pos > 0 && c.pos < int(c.maxCacheSize) {
		tuples := c.tuples[:c.pos]
		c.cache.Set(c.cacheKey, tuples, c.ttl)
		tuplesCacheSizeHistogram.Observe(float64(len(tuples)))
	}

	c.tuples = make([]*openfgav1.Tuple, c.maxCacheSize)
	c.pos = 0
}

func cacheKeyFor(s string) string {
	hasher := keys.NewCacheKeyHasher(xxhash.New())

	if err := hasher.WriteString(s); err != nil {
		return ""
	}

	return strconv.FormatUint(hasher.Key().ToUInt64(), 10)
}
