//go:generate mockgen -source cache.go -destination ../../internal/mocks/mock_cache.go -package mocks cache

package storage

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Yiling-J/theine-go"
	"github.com/cespare/xxhash/v2"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

const (
	SubproblemCachePrefix      = "sp."
	iteratorCachePrefix        = "ic."
	changelogCachePrefix       = "cc."
	invalidIteratorCachePrefix = "iq."
	defaultMaxCacheSize        = 10000
	oneYear                    = time.Hour * 24 * 365
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

// Set will store the value during the ttl.
// Note that ttl is truncated to one year to avoid misinterpreted as negative value.
// Negative ttl are noop.
func (i InMemoryLRUCache[T]) Set(key string, value T, ttl time.Duration) {
	if ttl >= oneYear {
		ttl = oneYear
	}
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
	return changelogCachePrefix + storeID
}

type InvalidEntityCacheEntry struct {
	LastModified time.Time
}

func GetInvalidIteratorCacheKey(storeID string) string {
	return invalidIteratorCachePrefix + storeID
}

func GetInvalidIteratorByObjectRelationCacheKeys(storeID, object, relation string) []string {
	return []string{invalidIteratorCachePrefix + storeID + "-or/" + object + "#" + relation}
}

func GetInvalidIteratorByUserObjectTypeCacheKeys(storeID string, users []string, objectType string) []string {
	res := make([]string, len(users))
	var i int
	for _, user := range users {
		res[i] = invalidIteratorCachePrefix + storeID + "-otr/" + user + "|" + objectType
		i++
	}
	return res
}

type TupleIteratorCacheEntry struct {
	Tuples       []*TupleRecord
	LastModified time.Time
}

func GetReadUsersetTuplesCacheKeyPrefix(store, object, relation string) string {
	return iteratorCachePrefix + "rut/" + store + "/" + object + "#" + relation
}

func GetReadStartingWithUserCacheKeyPrefix(store, objectType, relation string) string {
	return iteratorCachePrefix + "rtwu/" + store + "/" + objectType + "#" + relation
}

func GetReadCacheKey(store, tuple string) string {
	return iteratorCachePrefix + "r/" + store + "/" + tuple
}

// ErrUnexpectedStructValue is an error used to indicate that
// an unexpected structpb.Value kind was encountered.
var ErrUnexpectedStructValue = errors.New("unexpected structpb value encountered")

// writeValue writes value v to the writer w. An error
// is returned only when the underlying writer returns
// an error or an unexpected value kind is encountered.
func writeValue(w io.StringWriter, v *structpb.Value) (err error) {
	switch val := v.GetKind().(type) {
	case *structpb.Value_BoolValue:
		_, err = w.WriteString(fmt.Sprintf("%v", val.BoolValue))
	case *structpb.Value_NullValue:
		_, err = w.WriteString("null")
	case *structpb.Value_StringValue:
		_, err = w.WriteString(val.StringValue)
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

	for _, key := range keys {
		if _, err = w.WriteString(fmt.Sprintf("'%s:'", key)); err != nil {
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

	// sort tulpes for a deterministic write
	sort.Sort(sortedTuples)

	// prefix to avoid overlap with previous strings written
	_, err = w.WriteString("/")
	if err != nil {
		return
	}

	for n, tupleKey := range sortedTuples {
		_, err = w.WriteString(tupleKey.GetObject() + "#" + tupleKey.GetRelation())
		if err != nil {
			return
		}

		cond := tupleKey.GetCondition()
		if cond != nil {
			// " with " is separated by spaces as those are invalid in relation names
			// and we need to ensure this cache key is unique
			// resultant cache key format is "object:object_id#relation with {condition} {context}@user:user_id"
			_, err = w.WriteString(" with " + cond.GetName())
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

		if _, err = w.WriteString("@" + tupleKey.GetUser()); err != nil {
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
	hasher := xxhash.New()

	_, err := hasher.WriteString(
		SubproblemCachePrefix +
			params.StoreID +
			"/" +
			params.AuthorizationModelID +
			"/" +
			params.TupleKey.GetObject() +
			"#" +
			params.TupleKey.GetRelation() +
			"@" +
			params.TupleKey.GetUser(),
	)
	if err != nil {
		return "", err
	}

	// here, and for context below, avoid hashing if we don't need to
	if len(params.ContextualTuples) > 0 {
		if err = writeTuples(hasher, params.ContextualTuples...); err != nil {
			return "", err
		}
	}

	if params.Context != nil {
		if err = writeStruct(hasher, params.Context); err != nil {
			return "", err
		}
	}
	return strconv.FormatUint(hasher.Sum64(), 10), nil
}
