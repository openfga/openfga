//go:generate mockgen -source cache.go -destination ../../internal/mocks/mock_cache.go -package mocks cache

package storage

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/Yiling-J/theine-go"
	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/utils"
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
	iteratorCachePrefix        = "ic."
	changelogCachePrefix       = "cc."
	invalidIteratorCachePrefix = "iq."
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
	return changelogCachePrefix + storeID
}

type InvalidEntityCacheEntry struct {
	LastModified time.Time
}

func (i *InvalidEntityCacheEntry) CacheEntityType() string {
	return "invalid_entity"
}

func GetInvalidIteratorCacheKey(storeID string) string {
	return invalidIteratorCachePrefix + storeID
}

func GetInvalidIteratorByObjectRelationCacheKey(storeID, object, relation string) string {
	var builder CacheKeyBuilder
	builder.Grow(5) // grown by the number of elements to be written to the builder

	builder.WriteString(invalidIteratorCachePrefix)
	builder.WriteString("or")
	builder.WriteString(storeID)
	builder.WriteString(object)
	builder.WriteString(relation)

	return builder.Build()
}

func GetInvalidIteratorByUserObjectTypeCacheKeys(storeID string, users []string, objectType string) []string {
	result := make([]string, len(users))

	for i, user := range users {
		var builder CacheKeyBuilder
		builder.Grow(5) // grown by the number of elements to be written to the builder

		builder.WriteString(invalidIteratorCachePrefix)
		builder.WriteString("otr")
		builder.WriteString(storeID)
		builder.WriteString(user)
		builder.WriteString(objectType)

		result[i] = builder.Build()
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

func GetReadUsersetTuplesCacheKeyPrefix(builder *CacheKeyBuilder, store, object, relation string) {
	builder.Grow(5) // grown by the number of elements to be written to the builder

	builder.WriteString(iteratorCachePrefix)
	builder.WriteString("rut")
	builder.WriteString(store)
	builder.WriteString(object)
	builder.WriteString(relation)
}

func GetReadStartingWithUserCacheKeyPrefix(builder *CacheKeyBuilder, store, objectType, relation string) {
	builder.Grow(5) // grown by the number of elements to be written to the builder

	builder.WriteString(iteratorCachePrefix)
	builder.WriteString("rtwu")
	builder.WriteString(store)
	builder.WriteString(objectType)
	builder.WriteString(relation)
}

func GetReadCacheKey(builder *CacheKeyBuilder, store, tuple string) {
	builder.Grow(4) // grown by the number of elements to be written to the builder

	builder.WriteString(iteratorCachePrefix)
	builder.WriteString("r")
	builder.WriteString(store)
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
func writeValue(w io.StringWriter, v *structpb.Value) (err error) {
	switch val := v.GetKind().(type) {
	case *structpb.Value_BoolValue:
		_, err = w.WriteString(strconv.FormatBool(val.BoolValue))
	case *structpb.Value_NullValue:
		_, err = w.WriteString("null")
	case *structpb.Value_StringValue:
		if err = validateNoForbiddenChars(val.StringValue); err != nil {
			return
		}

		numBytes := len(val.StringValue)
		_, err = w.WriteString(strconv.Itoa(numBytes) + ":")
		if err != nil {
			return
		}

		_, err = w.WriteString(val.StringValue)
		if err != nil {
			return
		}
	case *structpb.Value_NumberValue:
		_, err = w.WriteString(strconv.FormatFloat(val.NumberValue, 'f', -1, 64)) // -1 precision ensures we represent the 64-bit value with the maximum precision needed to represent it, see strconv#FormatFloat for more info.
	case *structpb.Value_ListValue:
		values := val.ListValue.GetValues()

		for n, vv := range values {
			if err = writeValue(w, vv); err != nil {
				return
			}

			if n < len(values)-1 {
				if _, err = w.WriteString(","); err != nil {
					return
				}
			}
		}
	case *structpb.Value_StructValue:
		err = writeStruct(w, val.StructValue)
	default:
		err = ErrUnexpectedStructValue
	}
	return
}

// keys accepts a map m and returns a slice of its keys.
// When this project is updated to Go version 1.23 or greater,
// `maps.Keys` should be preferred.
func keys[T comparable, U any](m map[T]U) []T {
	n := make([]T, len(m))
	var i int
	for k := range m {
		n[i] = k
		i++
	}
	return n
}

// writeStruct writes Struct value s to writer w. When s is nil, a
// nil error is returned. An error is returned only when the underlying
// writer returns an error. The struct fields are written in the sorted
// order of their names. A comma separates fields.
func writeStruct(w io.StringWriter, s *structpb.Struct) (err error) {
	if s == nil {
		return
	}

	fields := s.GetFields()
	keys := keys(fields)
	sort.Strings(keys)

	// encode the length of the keys to ensure the correct number of keys are reflected
	// e.g. "a": "x,'b:'y"
	if _, err = w.WriteString(strconv.Itoa(len(keys))); err != nil {
		return
	}

	for _, key := range keys {
		if err = validateNoForbiddenChars(key); err != nil {
			return
		}
		if _, err = w.WriteString("'"); err != nil {
			return
		}
		if _, err = w.WriteString(key); err != nil {
			return
		}
		// 'key:'value separator and quote
		if _, err = w.WriteString(":'"); err != nil {
			return
		}

		if err = writeValue(w, fields[key]); err != nil {
			return
		}

		if _, err = w.WriteString(","); err != nil {
			return
		}
	}
	return
}

// writeTuples writes the set of tuples to writer w in ascending sorted order.
// The intention of this function is to write the tuples as a unique string.
// Tuples are separated by commas, and when present, conditions are included
// in the tuple string representation. Returns an error only when
// the underlying writer returns an error.
func writeTuples(w io.StringWriter, tuples ...*openfgav1.TupleKey) (err error) {
	sortedTuples := make(tuple.TupleKeys, len(tuples))

	// copy tuples slice to avoid mutating the original slice during sorting.
	copy(sortedTuples, tuples)

	// sort tuples for a deterministic write
	sort.Sort(sortedTuples)

	// prefix to avoid overlap with previous strings written
	_, err = w.WriteString("/")
	if err != nil {
		return
	}

	for n, tupleKey := range sortedTuples {
		objectRelation := tupleKey.GetObject() + "#" + tupleKey.GetRelation()
		if err = validateNoForbiddenChars(objectRelation); err != nil {
			return
		}
		_, err = w.WriteString(objectRelation)
		if err != nil {
			return
		}

		cond := tupleKey.GetCondition()
		if cond != nil {
			// " with " is separated by spaces as those are invalid in relation names
			// and we need to ensure this cache key is unique
			// resultant cache key format is "object:object_id#relation with {condition} {context}@user:user_id"
			condName := cond.GetName()
			if err = validateNoForbiddenChars(condName); err != nil {
				return
			}
			_, err = w.WriteString(" with " + condName)
			if err != nil {
				return
			}

			// if the condition also has context, we need an additional separator
			// which cannot be present in condition names
			if cond.GetContext() != nil {
				_, err = w.WriteString(" ")
				if err != nil {
					return
				}
			}

			// now write context to hash. Is a noop if context is nil.
			if err = writeStruct(w, cond.GetContext()); err != nil {
				return
			}
		}

		user := tupleKey.GetUser()
		if err = validateNoForbiddenChars(user); err != nil {
			return
		}
		if _, err = w.WriteString("@" + user); err != nil {
			return
		}

		if n < len(tuples)-1 {
			if _, err = w.WriteString(","); err != nil {
				return
			}
		}
	}
	return
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

// WriteCheckCacheKey converts the elements of a Check into a canonical cache key that can be
// used for Check resolution cache key lookups in a stable way, and writes it to the provided writer.
//
// For one store and model ID, the same tuple provided with the same contextual tuples and context
// should produce the same cache key. Contextual tuple order and context parameter order is ignored,
// only the contents are compared.
func WriteCheckCacheKey(w io.StringWriter, params *CheckCacheKeyParams) error {
	t := tuple.From(params.TupleKey)

	tupleStr := t.String()
	if err := validateNoForbiddenChars(tupleStr); err != nil {
		return err
	}
	_, err := w.WriteString(tupleStr)
	if err != nil {
		return err
	}

	err = WriteInvariantCheckCacheKey(w, params)
	if err != nil {
		return err
	}

	return nil
}

func WriteInvariantCheckCacheKey(w io.StringWriter, params *CheckCacheKeyParams) error {
	_, err := w.WriteString(params.AuthorizationModelID)
	if err != nil {
		return err
	}

	// here, and for context below, avoid hashing if we don't need to
	if len(params.ContextualTuples) > 0 {
		if err = writeTuples(w, params.ContextualTuples...); err != nil {
			return err
		}
	}

	if params.Context != nil {
		if err = writeStruct(w, params.Context); err != nil {
			return err
		}
	}

	return nil
}

const hextable string = "0123456789ABCDEF"

type CacheKeyBuilder struct {
	buf [][]byte
}

func (b *CacheKeyBuilder) Grow(n int) {
	if cap(b.buf) < n {
		buf := make([][]byte, len(b.buf), n)
		copy(buf, b.buf)
		b.buf = buf
	}
}

func (b *CacheKeyBuilder) Write(val []byte) (int, error) {
	b.buf = append(b.buf, val)
	return len(val), nil
}

func (b *CacheKeyBuilder) WriteString(val string) (int, error) {
	return b.Write(unsafe.Slice(unsafe.StringData(val), len(val)))
}

// Build hex-encodes each written value and separates them with '|'.
// The output alphabet is [0-9A-F|], which makes it injection-proof: no crafted
// input can produce a '|', so segments can never collide across boundaries.
// Tradeoff: keys are 2x larger than raw concatenation, increasing cache memory usage.
func (b *CacheKeyBuilder) Build() string {
	var count int
	for _, buf := range b.buf {
		count += len(buf)*2 + 1
	}
	hex := make([]byte, count)
	var j int
	for _, buf := range b.buf {
		for i := range len(buf) {
			hex[j] = hextable[buf[i]>>4]
			j++
			hex[j] = hextable[buf[i]&0x0F]
			j++
		}
		hex[j] = '|'
		j++
	}
	// unsafe.String: safe because hex is a local buffer never mutated after this point.
	return unsafe.String(unsafe.SliceData(hex), len(hex))
}

// BuildUserTypeRestrictionsHash creates a deterministic xxhash digest from user type restrictions.
// Each restriction is formatted as "type", "type:*", or "type#relation", then sorted and
// hashed with null-byte separators. Returns the hash as a []byte.
func BuildUserTypeRestrictionsHash(refs []*openfgav1.RelationReference) []byte {
	if len(refs) == 0 {
		return []byte{}
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

	var hasher xxhash.Digest
	for _, part := range parts {
		hasher.WriteString(part)
		hasher.Write([]byte{0})
	}
	return hasher.Sum([]byte{})
}
