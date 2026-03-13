package check

import (
	"context"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

const (
	// CheckIteratorCachePrefix differentiates from storage layer cache.
	checkIteratorCachePrefix = "cic/"
)

// CheckIteratorCacheEntry stores cached values for check iterator.
type CheckIteratorCacheEntry struct {
	Values []string
}

// CacheEntityType implements storage.CacheItem for metrics.
func (c *CheckIteratorCacheEntry) CacheEntityType() string {
	return "check_iterator"
}

// MapFunc extracts cacheable string from tuple key.
type MapFunc func(*openfgav1.TupleKey) string

// MapUser extracts user field.
func MapUser(tk *openfgav1.TupleKey) string {
	return tk.GetUser()
}

// MapUserObjectID extracts object ID from user field (TTU path).
func MapUserObjectID(tk *openfgav1.TupleKey) string {
	_, id := tuple.SplitObject(tk.GetUser())
	return id
}

// MapObjectID extracts object ID from object field (RSWU path).
func MapObjectID(tk *openfgav1.TupleKey) string {
	_, id := tuple.SplitObject(tk.GetObject())
	return id
}

// BuildUsersetTuplesCacheKey builds key for ReadUsersetTuples.
func BuildUsersetTuplesCacheKey(store, object, relation string, conditions []string) string {
	var b strings.Builder
	b.WriteString(checkIteratorCachePrefix)
	b.WriteString("rut/")
	b.WriteString(store)
	b.WriteByte('/')
	b.WriteString(object)
	b.WriteByte('#')
	b.WriteString(relation)
	appendConditions(&b, conditions)
	return b.String()
}

// BuildReadCacheKey builds key for Read (TTU path).
func BuildReadCacheKey(store, object, relation, userPrefix string, conditions []string) string {
	var b strings.Builder
	b.WriteString(checkIteratorCachePrefix)
	b.WriteString("r/")
	b.WriteString(store)
	b.WriteByte('/')
	b.WriteString(object)
	b.WriteByte('#')
	b.WriteString(relation)
	b.WriteByte('/')
	b.WriteString(userPrefix)
	appendConditions(&b, conditions)
	return b.String()
}

// BuildRSWUCacheKey builds key for ReadStartingWithUser.
func BuildRSWUCacheKey(store, objectType, relation, user string, conditions []string) string {
	var b strings.Builder
	b.WriteString(checkIteratorCachePrefix)
	b.WriteString("rswu/")
	b.WriteString(store)
	b.WriteByte('/')
	b.WriteString(objectType)
	b.WriteByte('#')
	b.WriteString(relation)
	b.WriteByte('/')
	b.WriteString(user)
	appendConditions(&b, conditions)
	return b.String()
}

func appendConditions(b *strings.Builder, conditions []string) {
	n := len(conditions)
	if n == 0 {
		return
	}
	b.WriteByte('/')
	if n == 1 {
		b.WriteString(conditions[0])
		return
	}
	// Sort for deterministic keys
	sorted := make([]string, n)
	copy(sorted, conditions)
	sort.Strings(sorted)
	b.WriteString(sorted[0])
	for i := 1; i < n; i++ {
		b.WriteByte(',')
		b.WriteString(sorted[i])
	}
}

// Iterator states.
const (
	stateActive int32 = iota
	stateAbandoned
	stateDone
)

// CachingIterator wraps TupleIterator with caching capability.
// Optimized for zero-lock hot path and minimal allocations.
type CachingIterator struct {
	iter     storage.TupleIterator
	cache    storage.InMemoryCache[any]
	cacheKey string
	ttl      time.Duration
	maxSize  int
	mapFunc  MapFunc
	drainCtx context.Context
	sf       *singleflight.Group
	state    atomic.Int32
	buffer   []string
}

// IteratorCacheConfig holds caching configuration.
type IteratorCacheConfig struct {
	Cache    storage.InMemoryCache[any]
	CacheKey string
	TTL      time.Duration
	MaxSize  int
	MapFunc  MapFunc
	DrainCtx context.Context
	SF       *singleflight.Group
}

// WrapIterator wraps iterator with caching if enabled.
// Returns static iterator on cache hit, original on cache disabled.
func WrapIterator(iter storage.TupleIterator, cfg IteratorCacheConfig) storage.TupleIterator {
	if cfg.Cache == nil {
		return iter
	}

	// Fast path: cache hit
	if v := cfg.Cache.Get(cfg.CacheKey); v != nil {
		if entry, ok := v.(*CheckIteratorCacheEntry); ok {
			iter.Stop()
			return &staticIter{values: entry.Values}
		}
	}

	return &CachingIterator{
		iter:     iter,
		cache:    cfg.Cache,
		cacheKey: cfg.CacheKey,
		ttl:      cfg.TTL,
		maxSize:  cfg.MaxSize,
		mapFunc:  cfg.MapFunc,
		drainCtx: cfg.DrainCtx,
		sf:       cfg.SF,
	}
}

// Next returns next tuple. Hot path - no locks, minimal branches.
func (c *CachingIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	t, err := c.iter.Next(ctx)
	if err != nil {
		if storage.IterIsDoneOrCancelled(err) {
			c.cacheSync()
		}
		return nil, err
	}

	// Only collect if active
	if c.state.Load() == stateActive {
		c.collect(t.GetKey())
	}

	return t, nil
}

// collect appends value to buffer.
func (c *CachingIterator) collect(tk *openfgav1.TupleKey) {
	if c.buffer == nil {
		c.buffer = make([]string, 0, 16)
	}

	c.buffer = append(c.buffer, c.mapFunc(tk))

	if len(c.buffer) >= c.maxSize {
		c.buffer = nil
		c.state.Store(stateAbandoned)
	}
}

// cacheSync stores buffer synchronously (iterator exhausted).
func (c *CachingIterator) cacheSync() {
	if !c.state.CompareAndSwap(stateActive, stateDone) {
		return
	}
	if len(c.buffer) == 0 {
		return
	}
	c.cache.Set(c.cacheKey, &CheckIteratorCacheEntry{Values: c.buffer}, c.ttl)
	c.buffer = nil
}

// Head returns head without advancing.
func (c *CachingIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return c.iter.Head(ctx)
}

// Stop terminates iteration and ensures caching.
func (c *CachingIterator) Stop() {
	if c.state.Load() != stateActive {
		c.iter.Stop()
		return
	}

	// No drain context - sync drain
	if c.drainCtx == nil || c.sf == nil {
		c.drainAndStop()
		return
	}

	// Context cancelled - just stop
	if c.drainCtx.Err() != nil {
		c.state.Store(stateDone)
		c.buffer = nil
		c.iter.Stop()
		return
	}

	// Already cached - skip drain
	if c.cache.Get(c.cacheKey) != nil {
		c.state.Store(stateDone)
		c.buffer = nil
		c.iter.Stop()
		return
	}

	c.drainAsync()
}

// drainAndStop drains synchronously then stops.
func (c *CachingIterator) drainAndStop() {
	if !c.state.CompareAndSwap(stateActive, stateDone) {
		c.iter.Stop()
		return
	}

	defer c.iter.Stop()

	if c.cache.Get(c.cacheKey) != nil {
		c.buffer = nil
		return
	}

	c.drainLoop(context.Background())
}

// drainAsync drains in background with singleflight.
func (c *CachingIterator) drainAsync() {
	if !c.state.CompareAndSwap(stateActive, stateDone) {
		c.iter.Stop()
		return
	}

	// Transfer ownership
	iter := c.iter
	buffer := c.buffer
	cache := c.cache
	cacheKey := c.cacheKey
	ttl := c.ttl
	maxSize := c.maxSize
	mapFunc := c.mapFunc
	drainCtx := c.drainCtx
	sf := c.sf

	c.iter = nil
	c.buffer = nil

	go sf.Do(cacheKey, func() (interface{}, error) {
		defer iter.Stop()

		if cache.Get(cacheKey) != nil {
			return nil, nil
		}

		for {
			t, err := iter.Next(drainCtx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) && len(buffer) > 0 {
					cache.Set(cacheKey, &CheckIteratorCacheEntry{Values: buffer}, ttl)
				}
				return nil, nil
			}

			if buffer == nil {
				buffer = make([]string, 0, 16)
			}
			buffer = append(buffer, mapFunc(t.GetKey()))

			if len(buffer) >= maxSize {
				return nil, nil
			}
		}
	})
}

// drainLoop drains iterator into buffer.
func (c *CachingIterator) drainLoop(ctx context.Context) {
	for {
		t, err := c.iter.Next(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) && len(c.buffer) > 0 {
				c.cache.Set(c.cacheKey, &CheckIteratorCacheEntry{Values: c.buffer}, c.ttl)
			}
			c.buffer = nil
			return
		}

		if c.buffer == nil {
			c.buffer = make([]string, 0, 16)
		}
		c.buffer = append(c.buffer, c.mapFunc(t.GetKey()))

		if len(c.buffer) >= c.maxSize {
			c.buffer = nil
			return
		}
	}
}

// staticIter serves cached values.
type staticIter struct {
	values []string
	idx    int
}

func (s *staticIter) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	if s.idx >= len(s.values) {
		return nil, storage.ErrIteratorDone
	}
	v := s.values[s.idx]
	s.idx++
	return &openfgav1.Tuple{Key: &openfgav1.TupleKey{User: v}}, nil
}

func (s *staticIter) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if s.idx >= len(s.values) {
		return nil, storage.ErrIteratorDone
	}
	return &openfgav1.Tuple{Key: &openfgav1.TupleKey{User: s.values[s.idx]}}, nil
}

func (s *staticIter) Stop() {}
