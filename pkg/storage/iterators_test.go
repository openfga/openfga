package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestStaticTupleKeyIterator(t *testing.T) {
	expected := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "bill"),
		tuple.NewTupleKey("document:doc2", "editor", "bob"),
	}

	iter := NewStaticTupleKeyIterator(expected)

	var actual []*openfgav1.TupleKey
	for {
		tk, err := iter.Next(context.Background())
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				break
			}
			require.Fail(t, "no error was expected")
		}

		actual = append(actual, tk)
	}

	require.Equal(t, expected, actual)
}

func TestCombinedIterator(t *testing.T) {
	expected := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "bill"),
		tuple.NewTupleKey("document:doc2", "editor", "bob"),
	}

	iter1 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{expected[0]})
	iter2 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{expected[1]})
	iter := NewCombinedIterator(iter1, iter2)

	var actual []*openfgav1.TupleKey
	for {
		tk, err := iter.Next(context.Background())
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				break
			}
			require.Fail(t, "no error was expected")
		}

		actual = append(actual, tk)
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	if diff := cmp.Diff(expected, actual, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-want +got):\n%s", diff)
	}
}

func ExampleNewFilteredTupleKeyIterator() {
	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
		tuple.NewTupleKey("document:doc1", "editor", "user:elbuo"),
	}

	iter := NewFilteredTupleKeyIterator(
		NewStaticTupleKeyIterator(tuples),
		func(tk *openfgav1.TupleKey) bool {
			return tk.GetRelation() == "editor"
		},
	)
	defer iter.Stop()

	var filtered []string
	for {
		tuple, err := iter.Next(context.Background())
		if err != nil {
			if err == ErrIteratorDone {
				break
			}

			// Handle the error in some way.
			panic(err)
		}

		filtered = append(filtered, fmt.Sprintf("%s#%s@%s", tuple.GetObject(), tuple.GetRelation(), tuple.GetUser()))
	}

	fmt.Println(filtered)
	// Output: [document:doc1#editor@user:elbuo]
}

func TestConditionsFilteredTupleKeyIterator(t *testing.T) {
	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		switch tupleKey.GetCondition().GetName() {
		case "condition1":
			return true, nil
		case "condition2":
			return false, nil
		default:
			return false, fmt.Errorf("unknown condition: %s", tupleKey.GetCondition().GetName())
		}
	}

	t.Run("no_error", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition1", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:elbuo", "condition2", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:maria", "condition1", nil),
		}
		iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
			}

			actual = append(actual, tk)
		}
		expected := []*openfgav1.TupleKey{tuples[0], tuples[2]}
		require.Equal(t, expected, actual)
	})

	t.Run("has_some_valid_but_middle_invalid", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition1", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:elbuo", "condition3", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:maria", "condition1", nil),
		}
		iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
			}

			actual = append(actual, tk)
		}
		expected := []*openfgav1.TupleKey{tuples[0], tuples[2]}
		require.Equal(t, expected, actual)
	})

	t.Run("has_some_valid_but_last_invalid", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition1", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:elbuo", "condition2", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:maria", "condition3", nil),
		}
		iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
			}

			actual = append(actual, tk)
		}
		expected := []*openfgav1.TupleKey{tuples[0]}
		require.Equal(t, expected, actual)
	})

	t.Run("empty_list", func(t *testing.T) {
		var tuples []*openfgav1.TupleKey
		iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
			}

			actual = append(actual, tk)
		}
		var expected []*openfgav1.TupleKey
		require.Equal(t, expected, actual)
	})

	t.Run("all_invalid", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition3", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:elbuo", "condition3", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:maria", "condition3", nil),
		}
		iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
		tk, err := iter.Next(context.Background())
		require.Error(t, err)
		require.Nil(t, tk)
	})
}
