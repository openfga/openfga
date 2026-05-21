//go:generate mockgen -source cache.go -destination ../../internal/mocks/mock_cache.go -package mocks cache

package storage

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/Yiling-J/theine-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/pkg/storage/cache"
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

const (
	SubproblemCachePrefix      = "sp."
	IteratorCachePrefix        = "ic."
	ChangelogCachePrefix       = "cc."
	InvalidIteratorCachePrefix = "iq."
	defaultMaxCacheSize        = 10000
	oneYear                    = time.Hour * 24 * 365

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
	Get(key string) T
	Set(key string, value T, ttl time.Duration)

	Delete(key string)

	// Stop cleans resources.
	Stop()
}

type NoopCache struct{}

func (n *NoopCache) Get(_ string) any {
	return nil
}

func (n *NoopCache) Set(_ string, _ any, _ time.Duration) {}

func (n *NoopCache) Delete(_ string) {}

func (n *NoopCache) Stop() {}

func NewNoopCache() *NoopCache {
	return &NoopCache{}
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

	cacheBuilder := theine.NewBuilder[string, T](t.maxElements)
	cacheBuilder.RemovalListener(func(key string, value T, reason theine.RemoveReason) {
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

func (i InMemoryLRUCache[T]) Get(key string) T {
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
func (i InMemoryLRUCache[T]) Set(key string, value T, ttl time.Duration) {
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

func (i InMemoryLRUCache[T]) Delete(key string) {
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

func GetChangelogCacheKey(storeID string) string {
	return ChangelogCachePrefix + storeID
}

type InvalidEntityCacheEntry struct {
	LastModified time.Time
}

func (i *InvalidEntityCacheEntry) CacheEntityType() string {
	return "invalid_entity"
}

func GetInvalidIteratorCacheKey(storeID string) string {
	return InvalidIteratorCachePrefix + storeID
}

func GetInvalidIteratorByObjectRelationCacheKey(storeID, object, relation string) string {
	var builder cache.KeyBuilder

	builder.WriteString(InvalidIteratorCachePrefix)
	builder.Break()
	builder.WriteString("or")
	builder.Break()
	builder.WriteString(storeID)
	builder.Break()
	builder.WriteString(object)
	builder.Break()
	builder.WriteString(relation)

	return builder.String()
}

func GetInvalidIteratorByUserObjectTypeCacheKeys(storeID string, users []string, objectType string) []string {
	result := make([]string, len(users))

	for i, user := range users {
		var builder cache.KeyBuilder

		builder.WriteString(InvalidIteratorCachePrefix)
		builder.Break()
		builder.WriteString("otr")
		builder.Break()
		builder.WriteString(storeID)
		builder.Break()
		builder.WriteString(user)
		builder.Break()
		builder.WriteString(objectType)

		result[i] = builder.String()
	}
	return result
}

type TupleIteratorCacheEntry struct {
	Tuples       []*TupleRecord
	LastModified time.Time
}

func (t *TupleIteratorCacheEntry) CacheEntityType() string {
	return "tuple_iterator"
}

func ApplyReadUsersetTuplesCacheKeyPrefix(builder cache.KeyWriter, store, object, relation string) {
	builder.WriteString(IteratorCachePrefix)
	builder.Break()
	builder.WriteString("rut")
	builder.Break()
	builder.WriteString(store)
	builder.Break()
	builder.WriteString(object)
	builder.Break()
	builder.WriteString(relation)
}

func ApplyReadStartingWithUserCacheKeyPrefix(builder cache.KeyWriter, store, objectType, relation string) {
	builder.WriteString(IteratorCachePrefix)
	builder.Break()
	builder.WriteString("rtwu")
	builder.Break()
	builder.WriteString(store)
	builder.Break()
	builder.WriteString(objectType)
	builder.Break()
	builder.WriteString(relation)
}

func ApplyReadCacheKey(builder cache.KeyWriter, store, tuple string) {
	builder.WriteString(IteratorCachePrefix)
	builder.Break()
	builder.WriteString("r")
	builder.Break()
	builder.WriteString(store)
	builder.Break()
	builder.WriteString(tuple)
}

// ErrUnexpectedStructValue is an error used to indicate that
// an unexpected structpb.Value kind was encountered.
var ErrUnexpectedStructValue = errors.New("unexpected structpb value encountered")

// validateNoForbiddenChars returns an error if s contains characters OpenFGA doesn't allow.
// These characters should be rejected at validation boundaries before reaching
// cache key generation. If this errors, it indicates a validation bug upstream.
func validateNoForbiddenChars(s string) error {
	if utils.ContainsForbiddenChars(s) {
		return fmt.Errorf("invariant violation: forbidden character in cache key input")
	}
	return nil
}

// writeValue writes value v to the writer w. An error
// is returned only when the underlying writer returns
// an error or an unexpected value kind is encountered.
func writeValue(w cache.KeyWriter, v *structpb.Value) {
	type stack struct {
		Value *structpb.Value
		Next  *stack
	}

	// collect nested values for recursive processing
	next := &stack{Value: v}

	// use a scalar for loop to avoid stack overflow possibility
	for next != nil {
		current := next
		next = next.Next

		switch val := current.Value.GetKind().(type) {
		case *structpb.Value_BoolValue:
			var b uint8
			if val.BoolValue {
				b = 1
			}
			w.WriteByte(b)
		case *structpb.Value_NullValue:
			w.WriteByte(0x00)
		case *structpb.Value_StringValue:
			w.WriteString(val.StringValue)
		case *structpb.Value_NumberValue:
			// get the 8 bytes that reprent the float64 value directly
			// the value will retain the endianess of the architecture
			bits := math.Float64bits(val.NumberValue)
			w.WriteUint64(bits)
		case *structpb.Value_ListValue:
			values := val.ListValue.GetValues()
			for _, vv := range values {
				next = &stack{
					Value: vv,
					Next:  next,
				}
			}
		case *structpb.Value_StructValue:
			writeStruct(w, val.StructValue)
		}
	}
}

// writeStruct writes Struct value s to writer w. When s is nil, a
// nil error is returned. An error is returned only when the underlying
// writer returns an error. The struct fields are written in the sorted
// order of their names. A comma separates fields.
func writeStruct(w cache.KeyWriter, s *structpb.Struct) {
	if s == nil {
		return
	}

	fields := s.GetFields()
	keys := slices.Collect(maps.Keys(fields))
	sort.Strings(keys)

	for _, key := range keys {
		w.WriteString(key)
		writeValue(w, fields[key])
	}
}

// writeTuples writes the set of tuples to writer w in ascending sorted order.
// The intention of this function is to write the tuples as a unique string.
// Tuples are separated by commas, and when present, conditions are included
// in the tuple string representation. Returns an error only when
// the underlying writer returns an error.
func writeTuples(w cache.KeyWriter, tuples ...*openfgav1.TupleKey) {
	sortedTuples := make(tuple.TupleKeys, len(tuples))

	// copy tuples slice to avoid mutating the original slice during sorting.
	copy(sortedTuples, tuples)

	// sort tuples for a deterministic write
	sort.Sort(sortedTuples)

	for _, tupleKey := range sortedTuples {
		objectRelation := tupleKey.GetObject() + "#" + tupleKey.GetRelation()
		w.WriteString(objectRelation)

		cond := tupleKey.GetCondition()
		if cond != nil {
			w.Break()
			condName := cond.GetName()
			w.WriteString(condName)
			w.Break()
			writeStruct(w, cond.GetContext())
		}

		w.Break()

		user := tupleKey.GetUser()
		w.WriteString(user)
	}
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

// CheckCacheKeyParams is all the necessary pieces to create a unique-per-check cache key.
type CheckCacheKeyParams struct {
	StoreID              string
	AuthorizationModelID string
	TupleKey             *openfgav1.TupleKey
	ContextualTuples     []*openfgav1.TupleKey
	Context              *structpb.Struct
}

// BuildCheckCacheKey converts the elements of a Check into a canonical cache key that can be
// used for Check resolution cache key lookups in a stable way, and writes it to the provided writer.
//
// For one store and model ID, the same tuple provided with the same contextual tuples and context
// should produce the same cache key. Contextual tuple order and context parameter order is ignored,
// only the contents are compared.
func BuildCheckCacheKey(params *CheckCacheKeyParams) string {
	t := tuple.From(params.TupleKey)

	tupleStr := t.String()

	var builder cache.KeyBuilder

	builder.WriteString(tupleStr)
	builder.WriteUint64(BuildInvariantCacheKeyHash(
		params.AuthorizationModelID,
		params.Context,
		params.ContextualTuples...,
	))

	hash := builder.Sum64()

	builder.Reset()
	builder.WriteUint64(hash)
	return builder.String()
}

func BuildInvariantCacheKeyHash(modelID string, ctx *structpb.Struct, contextualTuples ...*openfgav1.TupleKey) uint64 {
	var builder cache.KeyBuilder
	builder.WriteString(modelID)

	// here, and for context below, avoid encoding if we don't need to
	if len(contextualTuples) > 0 {
		var tupleBuilder cache.KeyBuilder
		writeTuples(&tupleBuilder, contextualTuples...)
		builder.WriteUint64(tupleBuilder.Sum64())
	}

	if ctx != nil {
		var structBuilder cache.KeyBuilder
		writeStruct(&structBuilder, ctx)
		builder.WriteUint64(structBuilder.Sum64())
	}

	return builder.Sum64()
}

// BuildUserTypeRestrictionsHash creates a deterministic xxhash digest from user type restrictions.
// Each restriction is formatted as "type", "type:*", or "type#relation", then sorted and
// hashed with null-byte separators. Returns the hash as a []byte.
func BuildUserTypeRestrictionsHash(refs []*openfgav1.RelationReference) uint64 {
	if len(refs) == 0 {
		return 0
	}

	parts := make([]string, 0, len(refs))
	for _, ref := range refs {
		var part string
		switch r := ref.GetRelationOrWildcard().(type) {
		case *openfgav1.RelationReference_Relation:
			part = ref.GetType() + "#" + r.Relation
		case *openfgav1.RelationReference_Wildcard:
			part = ref.GetType() + ":*"
		default:
			part = ref.GetType()
		}
		parts = append(parts, part)
	}

	sort.Strings(parts)

	var builder cache.KeyBuilder
	for _, part := range parts {
		builder.WriteString(part)
	}
	return builder.Sum64()
}
