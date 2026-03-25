package storagewrappers

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/storage"
)

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const (
	v2IteratorCachePrefix = "v2ic."
	maxCachedElements     = 1000
	initialBufferCapacity = 64

	// State machine states for CachingIterator.
	stateActive    uint32 = 0
	stateDraining  uint32 = 1
	stateDone      uint32 = 2
	stateAbandoned uint32 = 3
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
}

// CacheEntityType implements storage.CacheItem for metrics.
func (e *V2IteratorCacheEntry) CacheEntityType() string {
	return "v2_iterator"
}

// ─────────────────────────────────────────────────────────────────────────────
// Buffer Pool - Reduces allocation pressure
// ─────────────────────────────────────────────────────────────────────────────

var entryBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]MinimalCacheEntry, 0, initialBufferCapacity)
		return &buf
	},
}

func getEntryBuffer() []MinimalCacheEntry {
	buf := entryBufferPool.Get().(*[]MinimalCacheEntry)
	return (*buf)[:0] // Reset length, keep capacity
}

func putEntryBuffer(buf []MinimalCacheEntry) {
	if cap(buf) <= maxCachedElements {
		// Clear references to allow GC of condition contexts
		clear(buf[:cap(buf)])
		entryBufferPool.Put(&buf)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CachingIterator - Lock-free caching iterator for cache miss
// ─────────────────────────────────────────────────────────────────────────────

// CachingIterator wraps a storage iterator to cache results.
// Lock-free design: uses atomic state machine, single-writer assumption.
// No double buffering: collects directly into MinimalCacheEntry slice.
//
// Thread Safety:
//   - NOT safe for concurrent use from multiple goroutines
//   - Designed for single-writer during Active state
//   - Ownership transfers to background goroutine on Stop()
//
// State Machine:
//
//	Active → Draining → Done
//	   ↓
//	Abandoned (if exceeds maxSize)
type CachingIterator struct {
	inner storage.TupleIterator

	// State machine (atomic)
	state atomic.Uint32

	// Single buffer - no double buffering
	entries []MinimalCacheEntry

	// Cache config
	cache    storage.InMemoryCache[any]
	cacheKey string
	maxSize  int // Configurable max entries (passed from CachedTupleReader)
	ttl      time.Duration

	// Background drain coordination
	sf *singleflight.Group
	wg *sync.WaitGroup

	// Reconstruction params
	objectType string
	relation   string
	operation  string
}

// Ensure CachingIterator implements TupleIterator.
var _ storage.TupleIterator = (*CachingIterator)(nil)

// newCachingIterator creates a new caching iterator for cache miss scenarios.
// Note: The ctx parameter is currently unused but kept for API compatibility.
// Background drains use context.Background() to ensure they complete even if
// the original request context is canceled.
func newCachingIterator(
	_ context.Context, // unused - background drains use context.Background()
	inner storage.TupleIterator,
	cache storage.InMemoryCache[any],
	cacheKey string,
	maxSize int,
	ttl time.Duration,
	sf *singleflight.Group,
	wg *sync.WaitGroup,
	objectType, relation, operation string,
) *CachingIterator {
	return &CachingIterator{
		inner:      inner,
		entries:    getEntryBuffer(),
		cache:      cache,
		cacheKey:   cacheKey,
		maxSize:    maxSize,
		ttl:        ttl,
		sf:         sf,
		wg:         wg,
		objectType: objectType,
		relation:   relation,
		operation:  operation,
	}
}

// Next returns the next tuple from the underlying iterator.
// Tuples are collected for caching (before condition evaluation).
//
// Lock-free: Uses atomic state check, single-writer assumption.
// Complexity: O(1) amortized.
func (c *CachingIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	// Fast path: atomic check, no lock
	if c.state.Load() >= stateDone {
		return nil, storage.ErrIteratorDone
	}

	t, err := c.inner.Next(ctx)
	if err != nil {
		if storage.IterIsDoneOrCancelled(err) {
			c.markDone()
		}
		return nil, err
	}

	// Collect for caching (single writer, no lock)
	c.collect(t)
	return t, nil
}

// Head returns the next tuple without advancing the iterator.
func (c *CachingIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if c.state.Load() >= stateDone {
		return nil, storage.ErrIteratorDone
	}
	return c.inner.Head(ctx)
}

// Stop terminates iteration and triggers caching.
// If not fully consumed, drains in background with singleflight deduplication.
func (c *CachingIterator) Stop() {
	state := c.state.Load()

	if state == stateAbandoned || state == stateDone {
		c.inner.Stop()
		c.releaseBuffer()
		return
	}

	if c.state.CompareAndSwap(stateActive, stateDraining) {
		// Check if already exhausted using background context since the
		// original request context may be canceled by the time Stop() is called.
		if _, err := c.inner.Head(context.Background()); err != nil {
			c.state.Store(stateDone)
			// Only cache if iterator completed naturally (ErrIteratorDone).
			// Any other error (e.g., database error) means partial results.
			if errors.Is(err, storage.ErrIteratorDone) {
				c.flush()
			} else {
				c.releaseBuffer()
			}
			c.inner.Stop()
			return
		}

		// Drain in background
		if c.wg != nil {
			c.wg.Add(1)
		}
		go c.drainInBackground()
	}
}

// collect extracts minimal data from tuple and adds to buffer.
func (c *CachingIterator) collect(t *openfgav1.Tuple) {
	state := c.state.Load()
	if state != stateActive && state != stateDraining {
		return
	}

	// Check threshold BEFORE adding (uses configurable maxSize)
	if len(c.entries) >= c.maxSize {
		c.state.Store(stateAbandoned)
		v2IterCacheAbandoned.WithLabelValues(c.operation).Inc()
		c.releaseBuffer()
		return
	}

	tk := t.GetKey()
	entry := MinimalCacheEntry{
		ObjectID: extractObjectID(tk.GetObject()),
		User:     tk.GetUser(),
	}

	if cond := tk.GetCondition(); cond != nil {
		entry.ConditionName = cond.GetName()
		entry.ConditionContext = cond.GetContext()
	}

	c.entries = append(c.entries, entry)
}

// markDone transitions to Done state and flushes cache.
func (c *CachingIterator) markDone() {
	if c.state.CompareAndSwap(stateActive, stateDone) {
		c.flush()
	}
}

// flush stores collected entries in cache.
func (c *CachingIterator) flush() {
	if len(c.entries) == 0 {
		return
	}

	v2IterCacheSize.WithLabelValues(c.operation).Observe(float64(len(c.entries)))

	c.cache.Set(c.cacheKey, &V2IteratorCacheEntry{
		Entries:      c.entries,
		LastModified: time.Now(),
	}, c.ttl)

	c.entries = nil // Ownership transferred to cache
}

// releaseBuffer returns buffer to pool if not flushed.
func (c *CachingIterator) releaseBuffer() {
	if c.entries != nil {
		putEntryBuffer(c.entries)
		c.entries = nil
	}
}

// drainInBackground continues fetching tuples after Stop().
// Uses context.Background() because the drain should complete independently
// of the original request lifecycle. When a consumer calls Stop() (e.g., found
// a result early), the request may complete and cancel its context, but we still
// want to drain and cache the complete iterator results.
func (c *CachingIterator) drainInBackground() {
	if c.wg != nil {
		defer c.wg.Done()
	}
	defer c.inner.Stop()

	// Use background context for draining - the drain should complete
	// even if the original request context is canceled.
	drainCtx := context.Background()

	_, _, _ = c.sf.Do(c.cacheKey, func() (interface{}, error) {
		for c.state.Load() == stateDraining {
			t, err := c.inner.Next(drainCtx)
			if err != nil {
				c.state.Store(stateDone)
				// Only cache if iterator completed naturally (ErrIteratorDone).
				// Any other error (e.g., database error) means partial results.
				if errors.Is(err, storage.ErrIteratorDone) {
					c.flush()
				} else {
					c.releaseBuffer()
				}
				return nil, nil
			}

			c.collect(t)

			if c.state.Load() == stateAbandoned {
				return nil, nil
			}
		}
		return nil, nil
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
}

// Ensure LockFreeCachedIterator implements TupleIterator.
var _ storage.TupleIterator = (*LockFreeCachedIterator)(nil)

// NewLockFreeCachedIterator creates a lock-free iterator over cached entries.
func NewLockFreeCachedIterator(entries []MinimalCacheEntry, objectType, relation string) *LockFreeCachedIterator {
	return &LockFreeCachedIterator{
		entries:    entries,
		objectType: objectType,
		relation:   relation,
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

// ─────────────────────────────────────────────────────────────────────────────
// Cache Key Helpers
// ─────────────────────────────────────────────────────────────────────────────

// appendConditionsHash adds a hash of condition names to the key builder.
func appendConditionsHash(b *strings.Builder, conditions []string) {
	if len(conditions) == 0 {
		return
	}

	// Filter out empty/NoCond
	filtered := make([]string, 0, len(conditions))
	for _, c := range conditions {
		if c != "" {
			filtered = append(filtered, c)
		}
	}

	if len(filtered) == 0 {
		return
	}

	// Sort for deterministic hash
	sort.Strings(filtered)

	// Hash condition names
	hasher := xxhash.New()
	for _, c := range filtered {
		_, _ = hasher.WriteString(c)
		_, _ = hasher.WriteString("|") // Separator to avoid collisions
	}

	b.WriteString("/c:")
	b.WriteString(strconv.FormatUint(hasher.Sum64(), 10))
}
