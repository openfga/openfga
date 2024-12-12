//go:generate mockgen -source cache.go -destination ../../internal/mocks/mock_cache.go -package mocks cache

package storage

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yiling-J/theine-go"
	"github.com/cespare/xxhash/v2"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/keys"
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

// CheckCacheKeyParams is all the necessary pieces to create a unique-per-check cache key.
type CheckCacheKeyParams struct {
	StoreID              string
	AuthorizationModelID string
	TupleKey             *openfgav1.TupleKey
	ContextualTuples     []*openfgav1.TupleKey
	Context              *structpb.Struct
}

// GetCheckCacheKey converts the elements of a Check into a canonical cache key that can be
// used for Check resolution cache key lookups in a stable way.
//
// For one store and model ID, the same tuple provided with the same contextual tuples and context
// should produce the same cache key. Contextual tuple order and context parameter order is ignored,
// only the contents are compared.
func GetCheckCacheKey(params *CheckCacheKeyParams) (string, error) {
	hasher := keys.NewCacheKeyHasher(xxhash.New())

	key := strings.Builder{}
	key.WriteString(SubproblemCachePrefix)
	key.WriteString(params.StoreID)
	key.WriteString("/")
	key.WriteString(params.AuthorizationModelID)
	key.WriteString("/")
	key.WriteString(params.TupleKey.GetObject())
	key.WriteString("#")
	key.WriteString(params.TupleKey.GetRelation())
	key.WriteString("@")
	key.WriteString(params.TupleKey.GetUser())

	if err := hasher.WriteString(key.String()); err != nil {
		return "", err
	}

	// here, and for context below, avoid hashing if we don't need to
	if len(params.ContextualTuples) > 0 {
		if err := keys.NewTupleKeysHasher(params.ContextualTuples...).Append(hasher); err != nil {
			return "", err
		}
	}

	if params.Context != nil {
		err := keys.NewContextHasher(params.Context).Append(hasher)
		if err != nil {
			return "", err
		}
	}

	return strconv.FormatUint(hasher.Key().ToUInt64(), 10), nil
}
