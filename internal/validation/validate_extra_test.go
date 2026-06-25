package validation

import (
	"errors"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestValidatorFunc(t *testing.T) {
	v := ValidatorFunc(func(n int) (bool, error) {
		return n > 0, nil
	})
	ok, err := v(5)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = v(-1)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestMakeFallible(t *testing.T) {
	v := MakeFallible(func(s string) bool { return s != "" })

	ok, err := v("non-empty")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = v("")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCombineValidators(t *testing.T) {
	positive := MakeFallible(func(n int) bool { return n > 0 })
	even := MakeFallible(func(n int) bool { return n%2 == 0 })

	t.Run("all_pass", func(t *testing.T) {
		ok, err := CombineValidators(positive, even)(4)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("short_circuits_on_first_failure", func(t *testing.T) {
		ok, err := CombineValidators(positive, even)(3) // positive passes, even fails
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("propagates_error", func(t *testing.T) {
		boom := errors.New("boom")
		failing := ValidatorFunc(func(int) (bool, error) { return false, boom })
		ok, err := CombineValidators(positive, failing, even)(2)
		require.ErrorIs(t, err, boom)
		require.False(t, ok)
	})

	t.Run("skips_nil_validators", func(t *testing.T) {
		ok, err := CombineValidators[int](nil, positive, nil)(7)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("empty_returns_true", func(t *testing.T) {
		ok, err := CombineValidators[int]()(0)
		require.NoError(t, err)
		require.True(t, ok)
	})
}

func TestFilterInvalidTuples(t *testing.T) {
	model := parser.MustTransformDSLToProto(`
model
  schema 1.1
type user
type document
  relations
    define viewer: [user]
`)
	typesys, err := typesystem.New(model)
	require.NoError(t, err)

	filter := FilterInvalidTuples(typesys)

	t.Run("keeps_valid_tuple", func(t *testing.T) {
		require.True(t, filter(tuple.NewTupleKey("document:1", "viewer", "user:anne")))
	})

	t.Run("drops_unknown_relation", func(t *testing.T) {
		require.False(t, filter(tuple.NewTupleKey("document:1", "unknown", "user:anne")))
	})

	t.Run("drops_unknown_object_type", func(t *testing.T) {
		require.False(t, filter(tuple.NewTupleKey("folder:1", "viewer", "user:anne")))
	})

	t.Run("drops_malformed_object", func(t *testing.T) {
		require.False(t, filter(&openfgav1.TupleKey{Object: "malformed", Relation: "viewer", User: "user:anne"}))
	})
}
