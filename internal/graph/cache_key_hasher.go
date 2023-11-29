package graph

import (
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type hasher interface {
	WriteString(value string) error
}

// CacheKeyHasher implements a key hash using Hash64 for computing cache keys in a stable way.
type CacheKeyHasher struct {
	hasher *xxhash.Digest
}

// NewHasher returns a hasher for string values.
func NewHasher(xhash *xxhash.Digest) *CacheKeyHasher {
	return &CacheKeyHasher{hasher: xhash}
}

// WriteString writes the provided string to the hash.
func (c *CacheKeyHasher) WriteString(value string) error {
	_, err := c.hasher.WriteString(value)
	if err != nil {
		return err
	}

	return nil
}

func (c *CacheKeyHasher) Key() uint64 {
	return c.hasher.Sum64()
}

// NewTupleKeysHasher returns a hasher for an array of *openfgav1.TupleKey.
// It sorts the tuples first to guarantee that two arrays that are identical except for the ordering
// return the same hash.
func NewTupleKeysHasher(tupleKeys ...*openfgav1.TupleKey) *TupleKeysHasher {
	return &TupleKeysHasher{tupleKeys}
}

type TupleKeysHasher struct {
	tupleKeys []*openfgav1.TupleKey
}

func (t *TupleKeysHasher) Append(h hasher) error {
	sortedTupleKeys := append([]*openfgav1.TupleKey(nil), t.tupleKeys...) // Copy input to avoid mutating it

	sort.SliceStable(sortedTupleKeys, func(i, j int) bool {
		if sortedTupleKeys[i].Object > sortedTupleKeys[j].Object {
			return false
		}

		if sortedTupleKeys[i].Relation > sortedTupleKeys[j].Relation {
			return false
		}

		if sortedTupleKeys[i].User > sortedTupleKeys[j].User {
			return false
		}

		return true
	})

	// prefix to avoid overlap with previous strings written
	if err := h.WriteString("/"); err != nil {
		return err
	}

	for _, tupleKey := range sortedTupleKeys {
		// tuple with a separator at the end
		key := fmt.Sprintf("%s#%s@%s,", tupleKey.GetObject(), tupleKey.GetRelation(), tupleKey.GetUser())

		if err := h.WriteString(key); err != nil {
			return err
		}
	}

	return nil
}
