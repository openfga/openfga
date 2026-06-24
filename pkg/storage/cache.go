//go:generate mockgen -source cache.go -destination ../../internal/mocks/mock_cache.go -package mocks cache

package storage

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/Yiling-J/theine-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/tuple"
)

var (
	cacheItemCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: build.ProjectName,
		Name:      "cache_item_count",
		Help:      "The current number of items stored in the cache",
	}, []string{"entity"})

	cacheItemRemovedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "cache_item_removed_count",
		Help:      "The total number of items removed (evicted/expired/deleted) from the cache",
	}, []string{"entity", "reason"})
)

// Cache key namespace prefixes. Each prefix scopes a key to a particular
// cache domain, preventing collisions between unrelated entry types.
const (
	PrefixSubproblemCache      = "SP"
	PrefixIteratorCache        = "IC"
	PrefixChangelogCache       = "CC"
	PrefixInvalidIteratorCache = "IQ"
)

const (
	defaultMaxCacheSize = 10000
	oneYear             = time.Hour * 24 * 365

	removedLabel     = "removed"
	evictedLabel     = "evicted"
	expiredLabel     = "expired"
	unspecifiedLabel = "unspecified"
)

type CacheItem interface {
	CacheEntityType() string
}

// InMemoryCache is a general purpose cache to store things in memory.
type InMemoryCache[T any] interface {
	// Get If the key exists, returns the value. If the key didn't exist, returns nil.
	Get(key keys.Key) T
	Set(key keys.Key, value T, ttl time.Duration)

	Delete(key keys.Key)

	// Stop cleans resources.
	Stop()
}

type NoopCache struct{}

func (n *NoopCache) Get(_ keys.Key) any {
	return nil
}

func (n *NoopCache) Set(_ keys.Key, _ any, _ time.Duration) {}

func (n *NoopCache) Delete(_ keys.Key) {}

func (n *NoopCache) Stop() {}

func NewNoopCache() *NoopCache {
	return &NoopCache{}
}

// Specific implementation

type InMemoryLRUCache[T any] struct {
	client      *theine.Cache[keys.Key, T]
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
		maxElements: int64(defaultMaxCacheSize),
		stopOnce:    &sync.Once{},
	}

	for _, opt := range opts {
		opt(t)
	}

	cacheBuilder := theine.NewBuilder[keys.Key, T](t.maxElements)
	cacheBuilder.RemovalListener(func(key keys.Key, value T, reason theine.RemoveReason) {
		var (
			reasonLabel string
			entityLabel string
		)
		switch reason {
		case theine.EVICTED:
			reasonLabel = evictedLabel
		case theine.EXPIRED:
			reasonLabel = expiredLabel
		case theine.REMOVED:
			reasonLabel = removedLabel
		default:
			reasonLabel = unspecifiedLabel
		}

		if item, ok := any(value).(CacheItem); ok {
			entityLabel = item.CacheEntityType()
		} else {
			entityLabel = unspecifiedLabel
		}

		cacheItemRemovedCount.WithLabelValues(entityLabel, reasonLabel).Inc()
	})

	var err error
	t.client, err = cacheBuilder.Build()
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (i InMemoryLRUCache[T]) Get(key keys.Key) T {
	var zero T
	item, ok := i.client.Get(key)
	if !ok {
		return zero
	}

	return item
}

// Set will store the value during the ttl.
// Note that ttl is truncated to one year to avoid misinterpreted as negative value.
// Negative ttl are noop.
func (i InMemoryLRUCache[T]) Set(key keys.Key, value T, ttl time.Duration) {
	if ttl >= oneYear {
		ttl = oneYear
	}

	if ttl < 0 {
		return
	}

	// Ignore the boolean return here as we always pass cost=1 and items are always admitted
	i.client.SetWithTTL(key, value, 1, ttl)

	// Note: EstimatedSize is eventually consistent due to a shared lock in theine's maintenance routine.
	// It shouldn't matter in practice, but it may lag behind a few entries.
	cacheSizeFloat := float64(i.client.EstimatedSize())
	if item, ok := any(value).(CacheItem); ok {
		cacheItemCount.WithLabelValues(item.CacheEntityType()).Set(cacheSizeFloat)
	} else {
		cacheItemCount.WithLabelValues(unspecifiedLabel).Set(cacheSizeFloat)
	}
}

func (i InMemoryLRUCache[T]) Delete(key keys.Key) {
	i.client.Delete(key)
}

func (i InMemoryLRUCache[T]) Stop() {
	i.stopOnce.Do(func() {
		i.client.Close()
	})
}

var (
	_ CacheItem = (*ChangelogCacheEntry)(nil)
	_ CacheItem = (*InvalidEntityCacheEntry)(nil)
	_ CacheItem = (*TupleIteratorCacheEntry)(nil)
)

type ChangelogCacheEntry struct {
	LastModified time.Time // Last time the store was modified
	LastChecked  time.Time // Last time the changelog was checked
}

func (c *ChangelogCacheEntry) CacheEntityType() string {
	return "changelog"
}

// ChangelogCacheKey returns the key under which a store's changelog metadata is cached.
func ChangelogCacheKey(storeID string) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(PrefixChangelogCache)
	builder.EncodeString(storeID)
	return builder.Key()
}

type InvalidEntityCacheEntry struct {
	LastModified time.Time
}

func (i *InvalidEntityCacheEntry) CacheEntityType() string {
	return "invalid_entity"
}

// InvalidIteratorCacheKey returns the store-wide iterator invalidation key.
func InvalidIteratorCacheKey(storeID string) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(PrefixInvalidIteratorCache)
	builder.EncodeString(storeID)
	return builder.Key()
}

// InvalidIteratorByObjectRelationCacheKey returns the invalidation key scoped to a specific object and relation.
func InvalidIteratorByObjectRelationCacheKey(storeID, object, relation string) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(PrefixInvalidIteratorCache)
	builder.EncodeString("OR")
	builder.EncodeString(storeID)
	builder.EncodeString(object)
	builder.EncodeString(relation)
	return builder.Key()
}

// InvalidIteratorByUserObjectTypeCacheKey returns the invalidation key scoped to a specific user and object type.
func InvalidIteratorByUserObjectTypeCacheKey(storeID string, user string, objectType string) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(PrefixInvalidIteratorCache)
	builder.EncodeString("UOT")
	builder.EncodeString(storeID)
	builder.EncodeString(user)
	builder.EncodeString(objectType)
	return builder.Key()
}

type TupleIteratorCacheEntry struct {
	Tuples       []*TupleRecord
	LastModified time.Time
}

func (t *TupleIteratorCacheEntry) CacheEntityType() string {
	return "tuple_iterator"
}

// JitteredTTL returns a TTL with random jitter added. The jitter is a random duration
// in the range [0, baseTTL * jitterPercentage / 100]. Values above 100 are treated as
// 100. If jitterPercentage is 0 the base TTL is returned unchanged.
func JitteredTTL(baseTTL time.Duration, jitterPercentage uint32) time.Duration {
	if baseTTL <= 0 || jitterPercentage == 0 {
		return baseTTL
	}

	if jitterPercentage > 100 {
		jitterPercentage = 100
	}

	quotient := baseTTL / 100
	remainder := baseTTL % 100
	maxJitter := quotient*time.Duration(jitterPercentage) + remainder*time.Duration(jitterPercentage)/100

	var jitter time.Duration
	if maxJitter == time.Duration(math.MaxInt64) {
		jitter = time.Duration(rand.Int63())
	} else {
		jitter = time.Duration(rand.Int63n(int64(maxJitter) + 1))
	}

	if jitter > time.Duration(math.MaxInt64)-baseTTL {
		return time.Duration(math.MaxInt64)
	}

	return baseTTL + jitter
}

// CheckCacheKey builds the canonical per-Check subproblem cache key.
// The trailing invariant uint64 already encodes storeID (see InvariantCacheKey)
// so cache entries for distinct stores can never collide here even if the same
// model ID were ever observed across stores.
//
// Tuple component order and context key order do not affect the invariant
// hash — equivalent requests produce identical keys.
func CheckCacheKey(storeID, object, relation, user string, invariant uint64) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(PrefixSubproblemCache)
	builder.EncodeString(storeID)
	builder.EncodeString(object)
	builder.EncodeString(relation)
	builder.EncodeString(user)
	builder.EncodeUint64(invariant)
	return builder.Key()
}

// InvariantCacheKey hashes the per-request parts of a Check that don't change
// in sub-problems: storeID, modelID, context, and contextual tuples. StoreID
// is part of the hash so the invariant carries store identity transitively to
// every key that embeds it (CheckCacheKey, WriteEdgeCacheKey). This is defense
// in depth: ULIDs are globally unique today, but cache correctness should not
// depend on that property holding across DB restores, environment imports, or
// future ID-generation changes.
func InvariantCacheKey(storeID, modelID string, ctx *structpb.Struct, contextualTuples ...*openfgav1.TupleKey) uint64 {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(storeID)
	builder.EncodeString(modelID)

	sortedTuples := make(tuple.TupleKeys, len(contextualTuples))
	copy(sortedTuples, contextualTuples)
	sort.Sort(sortedTuples)

	ts := make([]keys.Serializable, len(sortedTuples))
	for i, t := range sortedTuples {
		ts[i] = (*keys.Tuple)(t)
	}
	builder.EncodeArray(ts)

	value := (*keys.PbValue)(structpb.NewStructValue(ctx))
	builder.Serialize(value)

	digest := keys.GetDigest()
	defer digest.Close()
	digest.Write(builder.Bytes())
	return digest.Sum64()
}
