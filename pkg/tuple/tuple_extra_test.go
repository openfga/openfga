package tuple

import (
	"errors"
	"sort"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestConvertRequestTupleKeys(t *testing.T) {
	want := &openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:anne"}

	require.Equal(t, want, ConvertCheckRequestTupleKeyToTupleKey(
		&openfgav1.CheckRequestTupleKey{Object: "document:1", Relation: "viewer", User: "user:anne"}))

	require.Equal(t, want, ConvertAssertionTupleKeyToTupleKey(
		&openfgav1.AssertionTupleKey{Object: "document:1", Relation: "viewer", User: "user:anne"}))

	require.Equal(t, want, ConvertReadRequestTupleKeyToTupleKey(
		&openfgav1.ReadRequestTupleKey{Object: "document:1", Relation: "viewer", User: "user:anne"}))
}

func TestTupleKeyWithoutConditionRoundTrip(t *testing.T) {
	tk := NewTupleKey("document:1", "viewer", "user:anne")

	without := TupleKeyToTupleKeyWithoutCondition(tk)
	require.Equal(t, "document:1", without.GetObject())
	require.Equal(t, "viewer", without.GetRelation())
	require.Equal(t, "user:anne", without.GetUser())

	back := TupleKeyWithoutConditionToTupleKey(without)
	require.Equal(t, tk.GetObject(), back.GetObject())
	require.Equal(t, tk.GetRelation(), back.GetRelation())
	require.Equal(t, tk.GetUser(), back.GetUser())

	list := TupleKeysWithoutConditionToTupleKeys(without, without)
	require.Len(t, list, 2)
}

func TestNewRequestTupleKeyConstructors(t *testing.T) {
	a := NewAssertionTupleKey("document:1", "viewer", "user:anne")
	require.Equal(t, "document:1", a.GetObject())

	c := NewCheckRequestTupleKey("document:1", "viewer", "user:anne")
	require.Equal(t, "viewer", c.GetRelation())

	e := NewExpandRequestTupleKey("document:1", "viewer")
	require.Equal(t, "document:1", e.GetObject())
	require.Equal(t, "viewer", e.GetRelation())
}

func TestGetRelation(t *testing.T) {
	require.Equal(t, "viewer", GetRelation("document:1#viewer"))
	require.Empty(t, GetRelation("document:1"))
}

func TestMustParseTupleString(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		tk := MustParseTupleString("document:1#viewer@user:jon")
		require.Equal(t, "document:1", tk.GetObject())
		require.Equal(t, "viewer", tk.GetRelation())
		require.Equal(t, "user:jon", tk.GetUser())
	})

	t.Run("invalid_panics", func(t *testing.T) {
		require.Panics(t, func() { MustParseTupleString("not-a-valid-tuple") })
	})
}

func TestMustParseTupleStrings(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		tks := MustParseTupleStrings("document:1#viewer@user:jon", "document:2#editor@user:anne")
		require.Len(t, tks, 2)
		require.Equal(t, "document:2", tks[1].GetObject())
	})

	t.Run("invalid_panics", func(t *testing.T) {
		require.Panics(t, func() { MustParseTupleStrings("good:1#viewer@user:a", "garbage") })
	})
}

func TestTupleKeys_Less_ByCondition(t *testing.T) {
	// Same object/relation/user, differing condition names -> compared by condition name.
	keys := TupleKeys{
		NewTupleKeyWithCondition("document:1", "viewer", "user:anne", "z_cond", nil),
		NewTupleKeyWithCondition("document:1", "viewer", "user:anne", "a_cond", nil),
	}
	require.False(t, keys.Less(0, 1))
	require.True(t, keys.Less(1, 0))

	// Fully equal tuples (no conditions) fall through to the final `return true`.
	equal := TupleKeys{
		NewTupleKey("document:1", "viewer", "user:anne"),
		NewTupleKey("document:1", "viewer", "user:anne"),
	}
	require.True(t, equal.Less(0, 1))
	require.True(t, equal.Less(1, 0))

	// Sorting integrates Less/Swap/Len.
	unsorted := TupleKeys{
		NewTupleKey("document:2", "viewer", "user:anne"),
		NewTupleKey("document:1", "viewer", "user:anne"),
	}
	sort.Sort(unsorted)
	require.Equal(t, "document:1", unsorted[0].GetObject())
}

func TestTupleErrors(t *testing.T) {
	t.Run("InvalidConditionalTupleError", func(t *testing.T) {
		err := &InvalidConditionalTupleError{
			Cause:    errors.New("bad condition"),
			TupleKey: NewTupleKey("document:1", "viewer", "user:anne"),
		}
		require.Contains(t, err.Error(), "Invalid tuple")
		require.Contains(t, err.Error(), "bad condition")
		require.ErrorIs(t, err, &InvalidConditionalTupleError{})
		require.NotErrorIs(t, err, errors.New("other"))
	})

	t.Run("InvalidTupleError", func(t *testing.T) {
		err := &InvalidTupleError{
			Cause:    errors.New("malformed"),
			TupleKey: &openfgav1.TupleKeyWithoutCondition{Object: "document:1", Relation: "viewer", User: "user:anne"},
		}
		require.Contains(t, err.Error(), "Invalid tuple")
		require.ErrorIs(t, err, &InvalidTupleError{})
	})

	t.Run("TypeNotFoundError", func(t *testing.T) {
		err := &TypeNotFoundError{TypeName: "group"}
		require.Equal(t, "type 'group' not found", err.Error())
		require.ErrorIs(t, err, &TypeNotFoundError{})
	})

	t.Run("RelationNotFoundError_without_tuple", func(t *testing.T) {
		err := &RelationNotFoundError{TypeName: "document", Relation: "viewer"}
		require.Equal(t, "relation 'document#viewer' not found", err.Error())
		require.ErrorIs(t, err, &RelationNotFoundError{})
	})

	t.Run("RelationNotFoundError_with_tuple", func(t *testing.T) {
		err := &RelationNotFoundError{
			TypeName: "document",
			Relation: "viewer",
			TupleKey: NewTupleKey("document:1", "viewer", "user:anne"),
		}
		require.Contains(t, err.Error(), "relation 'document#viewer' not found")
		require.Contains(t, err.Error(), "for tuple")
	})
}
