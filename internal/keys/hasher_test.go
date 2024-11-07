package keys

import (
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
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
		`unordered_relations_users_and_objects`: {
			tuplesReversed: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
				tuple.NewTupleKey("document:A", "relationA", "user:B"),
				tuple.NewTupleKey("document:A", "relationB", "user:A"),
				tuple.NewTupleKey("document:A", "relationB", "user:B"),
				tuple.NewTupleKey("document:B", "relationA", "user:A"),
				tuple.NewTupleKey("document:B", "relationA", "user:B"),
				tuple.NewTupleKey("document:B", "relationB", "user:A"),
				tuple.NewTupleKey("document:B", "relationB", "user:B"),
			},
			tuplesOriginal: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:B", "relationB", "user:B"),
				tuple.NewTupleKey("document:B", "relationB", "user:A"),
				tuple.NewTupleKey("document:B", "relationA", "user:B"),
				tuple.NewTupleKey("document:B", "relationA", "user:A"),
				tuple.NewTupleKey("document:A", "relationB", "user:B"),
				tuple.NewTupleKey("document:A", "relationB", "user:A"),
				tuple.NewTupleKey("document:A", "relationA", "user:B"),
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

			require.Equal(t, hasher1.Key().ToUInt64(), hasher2.Key().ToUInt64())
		})
	}
}

func TestContextHasher(t *testing.T) {
	var testCases = []struct {
		name     string
		context1 map[string]any
		context2 map[string]any
		equal    bool
	}{
		{
			context1: map[string]any{
				"x": []any{"1", "2"},
			},
			context2: map[string]any{
				"x": []any{"2", "1"},
			},
			equal: false,
		},
		{
			context1: map[string]any{
				"x": []any{1},
			},
			context2: map[string]any{
				"x": []any{"1"},
			},
			equal: true,
		},
		{
			context1: map[string]any{
				"x": []any{1},
			},
			context2: map[string]any{
				"x": []any{"1.1"},
			},
			equal: false,
		},
		{
			context1: map[string]any{
				"x": []any{1.0},
			},
			context2: map[string]any{
				"x": []any{1},
			},
			equal: true,
		},
		{
			context1: map[string]any{
				"x": []any{float64(3) / 2},
			},
			context2: map[string]any{
				"x": []any{1},
			},
			equal: false,
		},
		{
			context1: map[string]any{
				"x": []any{3 / 2},
			},
			context2: map[string]any{
				"x": []any{1},
			},
			equal: true,
		},
		{
			context1: map[string]any{
				"x": []any{float64(1) / 1},
			},
			context2: map[string]any{
				"x": []any{1},
			},
			equal: true,
		},
		{
			context1: map[string]any{
				"x": []any{0.000011},
			},
			context2: map[string]any{
				"x": []any{0.0000112},
			},
			equal: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			hasher1 := NewCacheKeyHasher(xxhash.New())

			struct1 := testutils.MustNewStruct(t, test.context1)
			err := NewContextHasher(struct1).Append(hasher1)
			require.NoError(t, err)
			key1 := hasher1.Key().ToUInt64()

			hasher2 := NewCacheKeyHasher(xxhash.New())
			struct2 := testutils.MustNewStruct(t, test.context2)
			err = NewContextHasher(struct2).Append(hasher2)
			require.NoError(t, err)
			key2 := hasher2.Key().ToUInt64()

			if test.equal {
				require.Equal(t, key1, key2)
			} else {
				require.NotEqual(t, key1, key2)
			}
		})
	}
}
