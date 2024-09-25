//go:generate mockgen -source cache.go -destination ../../internal/mocks/mock_cache.go -package mocks cache

package storage

import (
	"sync"
	"time"

	"github.com/karlseguin/ccache/v3"
)

const defaultMaxCacheSize = 10000

// InMemoryCache is a general purpose cache to store things in memory.
type InMemoryCache[T any] interface {

	// Get If the key exists, returns the value. If the key didn't exist, returns nil.
	Get(key string) *CachedResult[T]
	Set(key string, value T, ttl time.Duration)

	// Stop cleans resources.
	Stop()
}

type CachedResult[T any] struct {
	Value   T
	Expired bool
}

// Specific implementation

type InMemoryLRUCache[T any] struct {
	ccache      *ccache.Cache[T]
	maxElements int64
	closeOnce   sync.Once
}

type InMemoryLRUCacheOpt[T any] func(i *InMemoryLRUCache[T])

func WithMaxCacheSize[T any](maxElements int64) InMemoryLRUCacheOpt[T] {
	return func(i *InMemoryLRUCache[T]) {
		i.maxElements = maxElements
	}
}

var _ InMemoryCache[any] = (*InMemoryLRUCache[any])(nil)

func NewInMemoryLRUCache[T any](opts ...InMemoryLRUCacheOpt[T]) *InMemoryLRUCache[T] {
	t := &InMemoryLRUCache[T]{
		maxElements: defaultMaxCacheSize,
	}

	for _, opt := range opts {
		opt(t)
	}

	t.ccache = ccache.New(ccache.Configure[T]().MaxSize(t.maxElements))
	return t
}

func (i InMemoryLRUCache[T]) Get(key string) *CachedResult[T] {
	item := i.ccache.Get(key)
	if item != nil {
		return &CachedResult[T]{Value: item.Value(), Expired: item.Expired()}
	}
	return nil
}

func (i InMemoryLRUCache[T]) Set(key string, value T, ttl time.Duration) {
	i.ccache.Set(key, value, ttl)
}

func (i InMemoryLRUCache[T]) Stop() {
	i.closeOnce.Do(func() {
		i.ccache.Stop()
	})
}
