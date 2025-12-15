package iterator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestFilter_Conditions(t *testing.T) {
	conditionFilter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
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
		iter := NewFilteredIterator(
			storage.NewStaticTupleKeyIterator(tuples),
			conditionFilter,
		)
		defer iter.Stop()

		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
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

		iter := NewFilteredIterator(
			storage.NewStaticTupleKeyIterator(tuples),
			conditionFilter,
		)
		defer iter.Stop()

		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
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
		iter := NewFilteredIterator(
			storage.NewStaticTupleKeyIterator(tuples),
			conditionFilter,
		)
		defer iter.Stop()

		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
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
		iter := NewFilteredIterator(
			storage.NewStaticTupleKeyIterator(tuples),
			conditionFilter,
		)
		defer iter.Stop()

		tk, err := iter.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		require.Nil(t, tk)
	})

	t.Run("all_invalid", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition3", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:elbuo", "condition4", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:maria", "condition5", nil),
		}
		iter := NewFilteredIterator(
			storage.NewStaticTupleKeyIterator(tuples),
			conditionFilter,
		)
		defer iter.Stop()

		tk, err := iter.Next(context.Background())
		require.Equal(t, "unknown condition: condition5", err.Error())
		require.Nil(t, tk)
	})

	t.Run("ctx_timeout", func(t *testing.T) {
		var tuples []*openfgav1.TupleKey
		iter := NewFilteredIterator(
			storage.NewStaticTupleKeyIterator(tuples),
			conditionFilter,
		)
		defer iter.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := iter.Next(ctx)
		require.Error(t, err)
		require.NotEqual(t, storage.ErrIteratorDone, err)
	})
}

func TestFilter_Uniqueness(t *testing.T) {
	seenTuples := &sync.Map{}
	//nolint:unparam
	uniqueFilter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		key := tuple.TupleKeyToString(tupleKey)
		if _, exists := seenTuples.LoadOrStore(key, struct{}{}); exists {
			return false, nil
		}
		return true, nil
	}

	t.Run("filters_duplicates", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
			tuple.NewTupleKey("document:doc1", "viewer", "user:jon"), // duplicate
			tuple.NewTupleKey("document:doc2", "editor", "user:elbuo"),
			tuple.NewTupleKey("document:doc1", "viewer", "user:jon"), // duplicate
		}
		iter := NewFilteredIterator(
			storage.NewStaticTupleKeyIterator(tuples),
			uniqueFilter,
		)
		defer iter.Stop()

		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
			}
			actual = append(actual, tk)
		}

		expected := []*openfgav1.TupleKey{tuples[0], tuples[2]}
		require.Equal(t, expected, actual)
	})

	t.Run("all_unique", func(t *testing.T) {
		seenTuples := &sync.Map{}
		uniqueFilter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
			key := tuple.TupleKeyToString(tupleKey)
			if _, exists := seenTuples.LoadOrStore(key, struct{}{}); exists {
				return false, nil
			}
			return true, nil
		}

		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
			tuple.NewTupleKey("document:doc2", "editor", "user:elbuo"),
			tuple.NewTupleKey("document:doc3", "viewer", "user:maria"),
		}
		iter := NewFilteredIterator(
			storage.NewStaticTupleKeyIterator(tuples),
			uniqueFilter,
		)
		defer iter.Stop()

		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
			}
			actual = append(actual, tk)
		}

		require.Equal(t, tuples, actual)
	})
}

func TestFilter_MultipleFilters(t *testing.T) {
	t.Run("both_filters_applied", func(t *testing.T) {
		conditionFilter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
			return tupleKey.GetCondition().GetName() == "condition1", nil
		}
		seenTuples := &sync.Map{}
		uniqueFilter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
			key := tuple.TupleKeyToString(tupleKey)
			if _, exists := seenTuples.LoadOrStore(key, struct{}{}); exists {
				return false, nil
			}
			return true, nil
		}
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition1", nil),
			tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition1", nil),   // duplicate
			tuple.NewTupleKeyWithCondition("document:doc2", "editor", "user:elbuo", "condition2", nil), // wrong condition
			tuple.NewTupleKeyWithCondition("document:doc3", "viewer", "user:maria", "condition1", nil),
		}
		iter := NewFilteredIterator(
			storage.NewStaticTupleKeyIterator(tuples),
			conditionFilter,
			uniqueFilter,
		)
		defer iter.Stop()

		var actual []*openfgav1.TupleKey
		for {
			tk, err := iter.Next(context.Background())
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
			}
			actual = append(actual, tk)
		}

		expected := []*openfgav1.TupleKey{tuples[0], tuples[3]}
		require.Equal(t, expected, actual)
	})
}
