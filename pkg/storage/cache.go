//go:generate mockgen -source cache.go -destination ../../internal/mocks/mock_cache.go -package mocks cache

package storage

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/karlseguin/ccache/v3"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
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
	ccache      *ccache.Cache[T]
	maxElements int64
	closeOnce   *sync.Once
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
		closeOnce:   &sync.Once{},
	}

	for _, opt := range opts {
		opt(t)
	}

	t.ccache = ccache.New(ccache.Configure[T]().MaxSize(t.maxElements))
	return t
}

func (i InMemoryLRUCache[T]) Get(key string) T {
	var zero T
	item := i.ccache.Get(key)
	if item == nil {
		return zero
	}

	if value, expired := item.Value(), item.Expired(); !reflect.ValueOf(value).IsZero() && !expired {
		return value
	}

	return zero
}

func (i InMemoryLRUCache[T]) Set(key string, value T, ttl time.Duration) {
	i.ccache.Set(key, value, ttl)
}

func (i InMemoryLRUCache[T]) Delete(key string) {
	i.ccache.Delete(key)
}

func (i InMemoryLRUCache[T]) Stop() {
	i.closeOnce.Do(func() {
		i.ccache.Stop()
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
	Tuples       []*openfgav1.Tuple
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
