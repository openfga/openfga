package storagewrappers

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const (
	maxCachedElements = 1000
	// InitialBufferCapacity is the default initial capacity for tuple buffers.
	// Most queries return fewer than 100 tuples, so this avoids over-allocation
	// while still providing reasonable capacity to minimize slice growth.
	initialBufferCapacity = 100
)

// ─────────────────────────────────────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────────────────────────────────────

var (
	v2IterCacheTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "v2_iterator_cache_total",
		Help:      "Total v2 iterator cache operations.",
	}, []string{"operation"})

	v2IterCacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "v2_iterator_cache_hits",
		Help:      "Total v2 iterator cache hits.",
	}, []string{"operation"})

	v2IterCacheAbandoned = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "v2_iterator_cache_abandoned",
		Help:      "Total v2 iterator cache entries abandoned (exceeded max size).",
	}, []string{"operation"})

	v2IterCacheSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: build.ProjectName,
		Name:      "v2_iterator_cache_entry_size",
		Help:      "Number of tuples in cached iterator entries.",
		Buckets:   []float64{1, 10, 50, 100, 250, 500, 1000},
	}, []string{"operation"})
)

// ─────────────────────────────────────────────────────────────────────────────
// MinimalCacheEntry - Optimized storage for cached tuples
// ─────────────────────────────────────────────────────────────────────────────

// MinimalCacheEntry stores the minimum data needed to reconstruct a tuple
// for condition evaluation. Fields derivable from the cache key are omitted.
//
// Memory layout (~45 bytes typical vs ~100 bytes for TupleRecord):
//   - ObjectID: 16 byte header + variable data
//   - User: 16 byte header + variable data (may be userset like "group:eng#member")
//   - ConditionName: 16 byte header (often points to shared/interned string)
//   - ConditionContext: 8 byte pointer (often nil)
type MinimalCacheEntry struct {
	ObjectID         string           // The object ID (type is in cache key)
	User             string           // Full user string including userset if applicable
	ConditionName    string           // Condition name (empty if none)
	ConditionContext *structpb.Struct // Condition context (nil if none)
}

// V2IteratorCacheEntry is the cache entry stored in Theine.
type V2IteratorCacheEntry struct {
	Entries      []MinimalCacheEntry
	LastModified time.Time
	Ordered      bool
}

// CacheEntityType implements storage.CacheItem for metrics.
func (e *V2IteratorCacheEntry) CacheEntityType() string {
	return "v2_iterator"
}

// ─────────────────────────────────────────────────────────────────────────────
// CachingIterator - Mutex-based caching iterator for cache miss
// ─────────────────────────────────────────────────────────────────────────────

// CachingIterator wraps a storage iterator to cache results.
// Uses V1-style mutex for fast pointer collection, transforms to MinimalCacheEntry at flush.
//
// Design: Optimized for cache miss path performance while maintaining V2's
// memory-efficient MinimalCacheEntry storage format for cache hits.
//
// Thread Safety:
//   - Mutex protects tuples slice during collection
//   - Closing flag accessed within mutex prevents collection after Stop()
//   - Transform to MinimalCacheEntry happens at flush (single goroutine)
type CachingIterator struct {
	inner storage.TupleIterator

	// Mutex protects tuples slice
	mu sync.Mutex

	// Tuples collected during iteration (pointer append - fast)
	tuples []*openfgav1.Tuple

	// Flag to signal closing (always accessed within mutex)
	closing bool

	// Cache config
	cache    storage.InMemoryCache[any]
	cacheKey keys.Key
	maxSize  int
	ttl      time.Duration

	// createdAt records when the DB query was initiated.
	// Used as LastModified when flushing to cache, so invalidation entries
	// written after the query are correctly detected as newer.
	createdAt time.Time

	// Background drain coordination
	sf           *singleflight.Group
	wg           *sync.WaitGroup
	drainTimeout time.Duration // Timeout for background drain operations

	// Reconstruction params (used during transform)
	objectType string
	relation   string
	operation  string

	// sortKey extracts the field to sort by at flush time.
	// Read/ReadUsersetTuples sort by User; ReadStartingWithUser sorts by ObjectID.
	sortKey func(MinimalCacheEntry) string
}

// Ensure CachingIterator implements TupleIterator.
var _ storage.TupleIterator = (*CachingIterator)(nil)

// newCachingIterator creates a new caching iterator for cache miss scenarios.
func newCachingIterator(
	inner storage.TupleIterator,
	cache storage.InMemoryCache[any],
	cacheKey keys.Key,
	maxSize int,
	ttl time.Duration,
	drainTimeout time.Duration,
	sf *singleflight.Group,
	wg *sync.WaitGroup,
	objectType, relation, operation string,
	sortKey func(MinimalCacheEntry) string,
) *CachingIterator {
	// Cap initial capacity to avoid over-allocation for large maxSize values.
	// Most queries return few tuples, so initialBufferCapacity is usually sufficient.
	initCap := min(maxSize/2, initialBufferCapacity)

	// Register with WaitGroup at construction time to prevent Add-after-Wait panic.
	// This ensures Add() always happens before any Wait() can be called during shutdown.
	if wg != nil {
		wg.Add(1)
	}

	return &CachingIterator{
		inner:        inner,
		tuples:       make([]*openfgav1.Tuple, 0, initCap),
		cache:        cache,
		cacheKey:     cacheKey,
		maxSize:      maxSize,
		ttl:          ttl,
		drainTimeout: drainTimeout,
		sf:           sf,
		wg:           wg,
		createdAt:    time.Now(),
		objectType:   objectType,
		relation:     relation,
		operation:    operation,
		sortKey:      sortKey,
	}
}

// Next returns the next tuple from the underlying iterator.
// Collects tuple pointers for later transformation (V1 pattern - fast).
func (c *CachingIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing {
		return nil, storage.ErrIteratorDone
	}

	t, err := c.inner.Next(ctx)
	if err != nil {
		if !storage.IterIsDoneOrCancelled(err) {
			c.tuples = nil // Don't cache incomplete results
		}
		return nil, err
	}

	// Fast path: just append pointer (like V1)
	if c.tuples != nil {
		c.tuples = append(c.tuples, t)
		if len(c.tuples) > c.maxSize {
			v2IterCacheAbandoned.WithLabelValues(c.operation).Inc()
			c.tuples = nil // Exceeded max size, abandon caching
		}
	}

	return t, nil
}

// Head returns the next tuple without advancing the iterator.
func (c *CachingIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing {
		return nil, storage.ErrIteratorDone
	}

	return c.inner.Head(ctx)
}

func (c *CachingIterator) IsOrdered() bool { return c.inner.IsOrdered() }

// Stop terminates iteration and triggers caching.
// If not fully consumed, drains in background with singleflight deduplication.
// Follows V1's pattern: always spawns goroutine to avoid blocking Stop() on I/O.
func (c *CachingIterator) Stop() {
	c.mu.Lock()

	if c.closing {
		c.mu.Unlock()
		return
	}
	c.closing = true

	if c.tuples == nil {
		c.mu.Unlock()
		c.inner.Stop()
		// Must call Done since we Added in constructor
		if c.wg != nil {
			c.wg.Done()
		}
		return
	}

	// Spawn background goroutine to handle draining/flushing (like V1).
	// This avoids holding mutex during potential I/O operations.
	// Note: wg.Add(1) is now called in constructor to prevent Add-after-Wait panic.
	c.mu.Unlock()
	go c.drainInBackground()
}

// flush transforms collected tuples to MinimalCacheEntry and stores in cache.
// Must be called with mutex held or after closing is set.
func (c *CachingIterator) flush() {
	if len(c.tuples) == 0 {
		return
	}

	// Transform to MinimalCacheEntry (memory-efficient storage)
	entries := make([]MinimalCacheEntry, len(c.tuples))
	for i, t := range c.tuples {
		tk := t.GetKey()
		entries[i] = MinimalCacheEntry{
			ObjectID: extractObjectID(tk.GetObject()),
			User:     tk.GetUser(),
		}
		if cond := tk.GetCondition(); cond != nil {
			entries[i].ConditionName = cond.GetName()
			entries[i].ConditionContext = cond.GetContext()
		}
	}

	// Sort at flush time so cache hits are pre-sorted for v2Check merge algorithms.
	// Read/ReadUsersetTuples sort by User; ReadStartingWithUser sorts by ObjectID.
	// Skip if the inner iterator is already ordered — no redundant work needed.
	if c.sortKey != nil && !c.inner.IsOrdered() {
		sort.Slice(entries, func(i, j int) bool {
			return c.sortKey(entries[i]) < c.sortKey(entries[j])
		})
	}

	v2IterCacheSize.WithLabelValues(c.operation).Observe(float64(len(entries)))

	c.cache.Set(c.cacheKey, &V2IteratorCacheEntry{
		Entries:      entries,
		LastModified: c.createdAt,
		Ordered:      c.sortKey != nil || c.inner.IsOrdered(),
	}, c.ttl)

	c.tuples = nil // Release for GC
}

// drainInBackground continues fetching tuples after Stop().
// Uses a background context with timeout to ensure drains complete even if the
// original request context is cancelled, but don't block indefinitely.
//
// Optimizations (following V1's pattern):
//  1. Check if cache is already populated by another goroutine
//  2. Check if iterator is exhausted - flush directly without singleflight overhead
//  3. Use singleflight only when actual draining is needed
func (c *CachingIterator) drainInBackground() {
	if c.wg != nil {
		defer c.wg.Done()
	}
	defer c.inner.Stop()

	// Optimization 1: Check if cache is already populated by another goroutine.
	// This avoids redundant work when multiple iterators for the same key finish concurrently.
	if entry := c.cache.Get(c.cacheKey); entry != nil {
		if _, ok := entry.(*V2IteratorCacheEntry); ok {
			c.mu.Lock()
			c.tuples = nil // Another goroutine already cached
			c.mu.Unlock()
			return
		}
	}

	// Use background context with timeout - drain should complete even if
	// request context is cancelled, but should not block indefinitely.
	drainCtx, cancel := context.WithTimeout(context.Background(), c.drainTimeout)
	defer cancel()

	// Optimization 2: Check if iterator is already exhausted.
	// If so, flush directly without singleflight overhead.
	// This is the common case when caller fully consumed the iterator.
	if _, err := c.inner.Head(drainCtx); errors.Is(err, storage.ErrIteratorDone) {
		c.mu.Lock()
		c.flush()
		c.mu.Unlock()
		return
	}

	// Optimization 3: Use singleflight only for actual draining.
	// This prevents multiple goroutines from draining the same iterator key concurrently.
	_, _, _ = c.sf.Do(c.cacheKey.String(), func() (interface{}, error) {
		for {
			// Check for timeout before each iteration
			if drainCtx.Err() != nil {
				v2IterCacheAbandoned.WithLabelValues(c.operation).Inc()
				c.mu.Lock()
				c.tuples = nil // Don't cache incomplete results
				c.mu.Unlock()
				return nil, nil
			}

			t, err := c.inner.Next(drainCtx)
			if err != nil {
				c.mu.Lock()
				if errors.Is(err, storage.ErrIteratorDone) {
					c.flush() // write buffered tuples to cache
					c.mu.Unlock()
					return nil, nil
				}
				// On timeout or other errors, don't cache
				c.tuples = nil
				c.mu.Unlock()
				v2IterCacheAbandoned.WithLabelValues(c.operation).Inc()
				return nil, nil
			}

			c.mu.Lock()
			if c.tuples == nil {
				c.mu.Unlock()
				return nil, nil // Abandoned
			}
			c.tuples = append(c.tuples, t)
			if len(c.tuples) > c.maxSize {
				v2IterCacheAbandoned.WithLabelValues(c.operation).Inc()
				c.tuples = nil
				c.mu.Unlock()
				return nil, nil
			}
			c.mu.Unlock()
		}
	})
}

// extractObjectID extracts the ID portion from "type:id" format.
func extractObjectID(object string) string {
	if idx := strings.IndexByte(object, ':'); idx >= 0 {
		return object[idx+1:]
	}
	return object
}

// ─────────────────────────────────────────────────────────────────────────────
// LockFreeCachedIterator - Zero-lock iterator for cache hits
// ─────────────────────────────────────────────────────────────────────────────

// LockFreeCachedIterator provides lock-free iteration over cached entries.
// Uses atomic index for thread-safe access without mutex overhead.
//
// Performance: ~5ns per Next() vs ~25ns for mutex-based StaticIterator.
type LockFreeCachedIterator struct {
	entries    []MinimalCacheEntry
	index      atomic.Int64
	stopped    atomic.Bool
	objectType string
	relation   string
	ordered    bool
}

// Ensure LockFreeCachedIterator implements TupleIterator.
var _ storage.TupleIterator = (*LockFreeCachedIterator)(nil)

// NewLockFreeCachedIterator creates a lock-free iterator over cached entries.
func NewLockFreeCachedIterator(entries []MinimalCacheEntry, objectType, relation string, ordered bool) *LockFreeCachedIterator {
	return &LockFreeCachedIterator{
		entries:    entries,
		objectType: objectType,
		relation:   relation,
		ordered:    ordered,
	}
}

// Next returns the next tuple, reconstructing from cached minimal data.
//
// Lock-free: Uses atomic increment for index.
// Reconstruction cost: ~30-50ns (vs 120-175ns for current TupleRecord).
func (c *LockFreeCachedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if c.stopped.Load() {
		return nil, storage.ErrIteratorDone
	}

	// Atomic increment and check bounds
	idx := c.index.Add(1) - 1
	if idx >= int64(len(c.entries)) {
		return nil, storage.ErrIteratorDone
	}

	return c.reconstruct(&c.entries[idx]), nil
}

// Head returns the next tuple without advancing.
func (c *LockFreeCachedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if c.stopped.Load() {
		return nil, storage.ErrIteratorDone
	}

	idx := c.index.Load()
	if idx >= int64(len(c.entries)) {
		return nil, storage.ErrIteratorDone
	}

	return c.reconstruct(&c.entries[idx]), nil
}

// Stop marks the iterator as stopped.
func (c *LockFreeCachedIterator) Stop() {
	c.stopped.Store(true)
}

func (c *LockFreeCachedIterator) IsOrdered() bool { return c.ordered }

// reconstruct builds a full Tuple from minimal cached data.
func (c *LockFreeCachedIterator) reconstruct(e *MinimalCacheEntry) *openfgav1.Tuple {
	tk := &openfgav1.TupleKey{
		Object:   c.objectType + ":" + e.ObjectID,
		Relation: c.relation,
		User:     e.User,
	}

	if e.ConditionName != "" {
		tk.Condition = &openfgav1.RelationshipCondition{
			Name:    e.ConditionName,
			Context: e.ConditionContext,
		}
	}

	return &openfgav1.Tuple{Key: tk}
}
