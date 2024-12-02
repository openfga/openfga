//go:generate mockgen -source cache.go -destination ../../internal/mocks/mock_cache.go -package mocks cache

package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/Yiling-J/theine-go"
)

const (
	SubproblemCachePrefix      = "sp."
	iteratorCachePrefix        = "ic."
	changelogCachePrefix       = "cc."
	invalidIteratorCachePrefix = "iq."
	defaultMaxCacheSize        = 10000
)

// InMemoryCache is a general purpose cache to store things in memory.
type InMemoryCache[T any] interface {
	// Get If the key exists, returns the value. If the key didn't exist, returns nil.
	Get(key string) T
	Set(key string, value T, ttl time.Duration)

	Delete(prefix string)

	// Stop cleans resources.
	Stop()
}

// Specific implementation

type InMemoryLRUCache[T any] struct {
	client      *theine.Cache[string, T]
	maxElements int64
	stopOnce    *sync.Once
}

type InMemoryLRUCacheOpt[T any] func(i *InMemoryLRUCache[T])

func WithMaxCacheSize[T any](maxElements int64) InMemoryLRUCacheOpt[T] {
	return func(i *InMemoryLRUCache[T]) {
		i.maxElements = maxElements
	}
}

var _ InMemoryCache[any] = (*InMemoryLRUCache[any])(nil)

func NewInMemoryLRUCache[T any](opts ...InMemoryLRUCacheOpt[T]) (*InMemoryLRUCache[T], error) {
	t := &InMemoryLRUCache[T]{
		maxElements: defaultMaxCacheSize,
		stopOnce:    &sync.Once{},
	}

	for _, opt := range opts {
		opt(t)
	}

	var err error
	t.client, err = theine.NewBuilder[string, T](t.maxElements).Build()
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (i InMemoryLRUCache[T]) Get(key string) T {
	var zero T
	item, ok := i.client.Get(key)
	if !ok {
		return zero
	}

	return item
}

func (i InMemoryLRUCache[T]) Set(key string, value T, ttl time.Duration) {
	// negative ttl are noop
	i.client.SetWithTTL(key, value, 1, ttl)
}

func (i InMemoryLRUCache[T]) Delete(key string) {
	i.client.Delete(key)
}

func (i InMemoryLRUCache[T]) Stop() {
	i.stopOnce.Do(func() {
		i.client.Close()
	})
}

type ChangelogCacheEntry struct {
	LastModified time.Time
}

func GetChangelogCacheKey(storeID string) string {
	return fmt.Sprintf("%s%s", changelogCachePrefix, storeID)
}

type InvalidEntityCacheEntry struct {
	LastModified time.Time
}

func GetInvalidIteratorCacheKey(storeID string) string {
	return fmt.Sprintf("%s%s", invalidIteratorCachePrefix, storeID)
}

func GetInvalidIteratorByObjectRelationCacheKeys(storeID, object, relation string) []string {
	return []string{fmt.Sprintf("%s%s-or/%s#%s", invalidIteratorCachePrefix, storeID, object, relation)}
}

func GetInvalidIteratorByUserObjectTypeCacheKeys(storeID string, users []string, objectType string) []string {
	res := make([]string, 0, len(users))
	for _, user := range users {
		res = append(res, fmt.Sprintf("%s%s-otr/%s|%s", invalidIteratorCachePrefix, storeID, user, objectType))
	}
	return res
}

type TupleIteratorCacheEntry struct {
	Tuples       []*TupleRecord
	LastModified time.Time
}

func GetReadUsersetTuplesCacheKeyPrefix(store, object, relation string) string {
	return fmt.Sprintf("%srut/%s/%s#%s", iteratorCachePrefix, store, object, relation)
}

func GetReadStartingWithUserCacheKeyPrefix(store, objectType, relation string) string {
	return fmt.Sprintf("%srtwu/%s/%s#%s", iteratorCachePrefix, store, objectType, relation)
}

func GetReadCacheKey(store, tuple string) string {
	return fmt.Sprintf("%sr/%s/%s", iteratorCachePrefix, store, tuple)
}
