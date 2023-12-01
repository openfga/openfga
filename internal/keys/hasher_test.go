package keys

import (
	"testing"

	"github.com/cespare/xxhash/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
)

func TestTupleKeysHasherSortsFirst(t *testing.T) {
	var testCases = map[string]struct {
		tuplesReversed []*openfgav1.TupleKey
		tuplesOriginal []*openfgav1.TupleKey
	}{
		`unordered_users`: {
			tuplesReversed: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
				tuple.NewTupleKey("document:A", "relationA", "user:B"),
				tuple.NewTupleKey("document:A", "relationA", "user:C"),
			},
			tuplesOriginal: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:C"),
				tuple.NewTupleKey("document:A", "relationA", "user:B"),
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
			},
		},
		`unordered_relations`: {
			tuplesReversed: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
				tuple.NewTupleKey("document:A", "relationB", "user:A"),
				tuple.NewTupleKey("document:A", "relationC", "user:A"),
			},
			tuplesOriginal: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationC", "user:A"),
				tuple.NewTupleKey("document:A", "relationB", "user:A"),
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
			},
		},
		`unordered_objects`: {
			tuplesReversed: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
				tuple.NewTupleKey("document:B", "relationA", "user:A"),
				tuple.NewTupleKey("document:C", "relationA", "user:A"),
			},
			tuplesOriginal: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:C", "relationA", "user:A"),
				tuple.NewTupleKey("document:B", "relationA", "user:A"),
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			hasher1 := NewCacheKeyHasher(xxhash.New())
			tuplesHasher := NewTupleKeysHasher(test.tuplesOriginal...)
			require.NoError(t, tuplesHasher.Append(hasher1))

			hasher2 := NewCacheKeyHasher(xxhash.New())
			tuplesInvertedHasher := NewTupleKeysHasher(test.tuplesReversed...)
			require.NoError(t, tuplesInvertedHasher.Append(hasher2))

			require.Equal(t, hasher1.Key(), hasher2.Key())
		})
	}
}
