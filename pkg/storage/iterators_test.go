package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestEmptyIterator(t *testing.T) {
	t.Run("next", func(t *testing.T) {
		iter := emptyTupleIterator{}
		defer iter.Stop()

		tk, err := iter.Next(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
		require.Nil(t, tk)
	})
	t.Run("head", func(t *testing.T) {
		iter := emptyTupleIterator{}
		defer iter.Stop()

		tk, err := iter.Head(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
		require.Nil(t, tk)
	})
}

func TestStaticTupleKeyIterator(t *testing.T) {
	t.Run("next", func(t *testing.T) {
		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc1", "viewer", "bill"),
			tuple.NewTupleKey("document:doc2", "editor", "bob"),
		}

		iter := NewStaticTupleKeyIterator(expected)
		defer iter.Stop()

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
	})
	t.Run("head_empty", func(t *testing.T) {
		var expected []*openfgav1.TupleKey
		iter := NewStaticTupleKeyIterator(expected)
		defer iter.Stop()

		tk, err := iter.Head(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
		require.Nil(t, tk)
	})
	t.Run("head_not_empty", func(t *testing.T) {
		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc1", "viewer", "bill"),
			tuple.NewTupleKey("document:doc2", "editor", "bob"),
		}
		iter := NewStaticTupleKeyIterator(expected)
		defer iter.Stop()

		tk, err := iter.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, expected[0], tk)

		// Ensure the iterator does not increment
		tk, err = iter.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, expected[0], tk)
	})
}

func TestStaticTupleIterator(t *testing.T) {
	t.Run("next", func(t *testing.T) {
		expected := []*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKey("document:doc1", "viewer", "bill"),
				Timestamp: timestamppb.New(time.Now()),
			},
			{
				Key:       tuple.NewTupleKey("document:doc2", "editor", "bob"),
				Timestamp: timestamppb.New(time.Now()),
			},
		}

		iter := NewStaticTupleIterator(expected)
		defer iter.Stop()

		var actual []*openfgav1.Tuple
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
	})
	t.Run("head_empty", func(t *testing.T) {
		var expected []*openfgav1.Tuple
		iter := NewStaticTupleIterator(expected)
		defer iter.Stop()

		tk, err := iter.Head(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
		require.Nil(t, tk)
	})
	t.Run("head_not_empty", func(t *testing.T) {
		expected := []*openfgav1.Tuple{
			{
				Key:       tuple.NewTupleKey("document:doc1", "viewer", "bill"),
				Timestamp: timestamppb.New(time.Now()),
			},
			{
				Key:       tuple.NewTupleKey("document:doc2", "editor", "bob"),
				Timestamp: timestamppb.New(time.Now()),
			},
		}
		iter := NewStaticTupleIterator(expected)
		defer iter.Stop()
		tk, err := iter.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, expected[0], tk)

		// Ensure the iterator does not increment.
		tk, err = iter.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, expected[0], tk)
	})
	t.Run("check_for_race", func(t *testing.T) {
		t.Skip("TODO: whether static tuple iterator is intended to be thread safe")
		const numberItem = 50
		tks := make([]*openfgav1.Tuple, numberItem)
		for i := 0; i < numberItem; i++ {
			tks[i] = &openfgav1.Tuple{
				Key:       tuple.NewTupleKey("document:doc"+strconv.Itoa(i), "viewer", "bill"),
				Timestamp: timestamppb.New(time.Now()),
			}
		}
		iter := NewStaticTupleIterator(tks)
		defer iter.Stop()

		var wg errgroup.Group
		for i := 0; i < numberItem; i++ {
			wg.Go(func() error {
				_, err := iter.Next(context.Background())
				return err
			})
		}

		err := wg.Wait()
		require.NoError(t, err)
	})
}

func TestCombinedIterator(t *testing.T) {
	t.Run("next", func(t *testing.T) {
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
	})
	t.Run("head_empty_slices", func(t *testing.T) {
		iter1 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{})
		iter2 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{})
		iter := NewCombinedIterator(iter1, iter2)
		tk, err := iter.Head(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
		require.Nil(t, tk)
	})
	t.Run("head_not_empty", func(t *testing.T) {
		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc1", "viewer", "bill"),
			tuple.NewTupleKey("document:doc2", "editor", "bob"),
			tuple.NewTupleKey("document:doc2", "editor", "charles"),
		}

		iter1 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{expected[0]})
		iter2 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{expected[1], expected[2]})
		iter := NewCombinedIterator(iter1, iter2)
		tk, err := iter.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, expected[0], tk)

		tk, err = iter.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, expected[0], tk)
	})
}

func TestFilteredTupleKeyIterator(t *testing.T) {
	t.Run("next", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
			tuple.NewTupleKey("document:doc1", "editor", "user:elbuo"),
			tuple.NewTupleKey("document:doc2", "viewer", "user:elbuo"),
			tuple.NewTupleKey("document:doc2", "editor", "user:charlie"),
		}
		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc1", "editor", "user:elbuo"),
			tuple.NewTupleKey("document:doc2", "editor", "user:charlie"),
		}
		iter := NewFilteredTupleKeyIterator(
			NewStaticTupleKeyIterator(tuples),
			func(tk *openfgav1.TupleKey) bool {
				return tk.GetRelation() == "editor"
			},
		)
		defer iter.Stop()
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
	})
	t.Run("head_empty_slices", func(t *testing.T) {
		var tuples []*openfgav1.TupleKey

		iter := NewFilteredTupleKeyIterator(
			NewStaticTupleKeyIterator(tuples),
			func(tk *openfgav1.TupleKey) bool {
				return tk.GetRelation() == "editor"
			},
		)
		defer iter.Stop()
		tk, err := iter.Head(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
		require.Nil(t, tk)
	})
	t.Run("head_non_slices", func(t *testing.T) {
		tuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
			tuple.NewTupleKey("document:doc1", "editor", "user:elbuo"),
			tuple.NewTupleKey("document:doc2", "viewer", "user:elbuo"),
			tuple.NewTupleKey("document:doc2", "editor", "user:charlie"),
		}

		iter := NewFilteredTupleKeyIterator(
			NewStaticTupleKeyIterator(tuples),
			func(tk *openfgav1.TupleKey) bool {
				return tk.GetRelation() == "editor"
			},
		)
		defer iter.Stop()
		tk, err := iter.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, tuples[1], tk)

		// ensure the underlying iterator is not updated
		tk, err = iter.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, tuples[1], tk)
	})
	t.Run("race_condition", func(t *testing.T) {
		t.Skip("TODO: underlying static tuple key iterator is not thread safe")
		const numMatchingIterator = 50
		tuples := make([]*openfgav1.TupleKey, numMatchingIterator*3)
		for i := 0; i < numMatchingIterator; i++ {
			tuples[3*i] = tuple.NewTupleKey("document:doc"+strconv.Itoa(i), "viewer", "user:jon")
			tuples[3*i+1] = tuple.NewTupleKey("document:doc"+strconv.Itoa(i), "editor", "user:elbuo")
			tuples[3*i+2] = tuple.NewTupleKey("document:doc"+strconv.Itoa(i), "viewer", "user:charlie")
		}
		iter := NewFilteredTupleKeyIterator(
			NewStaticTupleKeyIterator(tuples),
			func(tk *openfgav1.TupleKey) bool {
				return tk.GetRelation() == "editor"
			},
		)
		defer iter.Stop()
		var wg errgroup.Group

		for i := 0; i < numMatchingIterator; i++ {
			wg.Go(func() error {
				_, err := iter.Head(context.Background())
				return err
			})
		}

		for i := 0; i < numMatchingIterator-1; i++ {
			wg.Go(func() error {
				_, err := iter.Next(context.Background())
				return err
			})
		}

		err := wg.Wait()
		require.NoError(t, err)
	})
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
		tests := []struct {
			name  string
			mixed bool
		}{
			{
				name:  "next_only",
				mixed: false,
			},
			{
				name:  "mixed_head_next",
				mixed: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tuples := []*openfgav1.TupleKey{
					tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition1", nil),
					tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:elbuo", "condition2", nil),
					tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:maria", "condition1", nil),
				}
				iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
				t.Cleanup(iter.Stop)

				var actual []*openfgav1.TupleKey
				var actualHead []*openfgav1.TupleKey

				for {
					if tt.mixed {
						headTk, err := iter.Head(context.Background())
						if err != nil {
							if errors.Is(err, ErrIteratorDone) {
								break
							}
							require.Fail(t, "no error was expected")
						}
						actualHead = append(actualHead, headTk)

						_, err = iter.Head(context.Background())
						if err != nil {
							require.Fail(t, "no error was expected")
						}
					}
					tk, err := iter.Next(context.Background())
					if err != nil {
						if errors.Is(err, ErrIteratorDone) {
							break
						}
						require.Fail(t, "no error was expected")
					}

					actual = append(actual, tk)
					if tt.mixed {
						_, err := iter.Head(context.Background())
						if err != nil {
							if errors.Is(err, ErrIteratorDone) {
								break
							}
							require.Fail(t, "no error was expected")
						}
					}
				}
				expected := []*openfgav1.TupleKey{tuples[0], tuples[2]}
				require.Equal(t, expected, actual)
				if tt.mixed {
					require.Equal(t, expected, actualHead)
				}
			})
		}
	})

	t.Run("has_some_valid_but_middle_invalid", func(t *testing.T) {
		tests := []struct {
			name  string
			mixed bool
		}{
			{
				name:  "next_only",
				mixed: false,
			},

			{
				name:  "mixed_head_next",
				mixed: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tuples := []*openfgav1.TupleKey{
					tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition1", nil),
					tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:elbuo", "condition3", nil),
					tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:maria", "condition1", nil),
				}
				iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
				t.Cleanup(iter.Stop)
				var actual []*openfgav1.TupleKey
				var actualHead []*openfgav1.TupleKey

				for {
					if tt.mixed {
						headTk, err := iter.Head(context.Background())
						if err != nil {
							// Notice that we don't expect errors as some tuples are valid.
							if errors.Is(err, ErrIteratorDone) {
								break
							}
							require.Fail(t, "no error was expected")
						}
						actualHead = append(actualHead, headTk)
						_, err = iter.Head(context.Background())
						if err != nil {
							require.Fail(t, "no error was expected")
						}
					}
					tk, err := iter.Next(context.Background())
					if err != nil {
						// Notice that we don't expect errors as some tuples are valid.
						if errors.Is(err, ErrIteratorDone) {
							break
						}
						require.Fail(t, "no error was expected")
					}

					actual = append(actual, tk)
				}
				expected := []*openfgav1.TupleKey{tuples[0], tuples[2]}
				require.Equal(t, expected, actual)
				if tt.mixed {
					require.Equal(t, expected, actualHead)
				}
			})
		}
	})

	t.Run("has_some_valid_but_last_invalid", func(t *testing.T) {
		tests := []struct {
			name  string
			mixed bool
		}{
			{
				name:  "next_only",
				mixed: false,
			},
			{
				name:  "mixed_head_next",
				mixed: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tuples := []*openfgav1.TupleKey{
					tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition1", nil),
					tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:elbuo", "condition2", nil),
					tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:maria", "condition3", nil),
				}
				iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
				t.Cleanup(iter.Stop)
				var actual []*openfgav1.TupleKey
				var actualHead []*openfgav1.TupleKey

				for {
					if tt.mixed {
						headTk, err := iter.Head(context.Background())
						if err != nil {
							// Notice that we don't expect errors as some tuples are valid.
							if errors.Is(err, ErrIteratorDone) {
								break
							}
							require.Fail(t, "no error was expected")
						}
						actualHead = append(actualHead, headTk)
						_, err = iter.Head(context.Background())
						if err != nil {
							require.Fail(t, "no error was expected")
						}
					}
					tk, err := iter.Next(context.Background())
					if err != nil {
						// Notice that we don't expect errors as some tuples are valid.
						if errors.Is(err, ErrIteratorDone) {
							break
						}
						require.Fail(t, "no error was expected")
					}

					actual = append(actual, tk)
				}
				expected := []*openfgav1.TupleKey{tuples[0]}
				require.Equal(t, expected, actual)
				if tt.mixed {
					require.Equal(t, expected, actualHead)
				}
			})
		}
	})

	t.Run("empty_list", func(t *testing.T) {
		tests := []struct {
			name  string
			mixed bool
		}{
			{
				name:  "next_only",
				mixed: false,
			},
			{
				name:  "mixed_head_next",
				mixed: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var tuples []*openfgav1.TupleKey
				iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
				t.Cleanup(iter.Stop)
				var actual []*openfgav1.TupleKey
				var actualHead []*openfgav1.TupleKey

				for {
					if tt.mixed {
						headTk, err := iter.Head(context.Background())
						if err != nil {
							// Notice that we don't expect errors as some tuples are valid.
							if errors.Is(err, ErrIteratorDone) {
								break
							}
							require.Fail(t, "no error was expected")
						}
						actualHead = append(actualHead, headTk)
						_, err = iter.Head(context.Background())
						if err != nil {
							require.Fail(t, "no error was expected")
						}
					}
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
				if tt.mixed {
					require.Equal(t, expected, actualHead)
				}
			})
		}
	})

	t.Run("all_invalid", func(t *testing.T) {
		tests := []struct {
			name  string
			mixed bool
		}{
			{
				name:  "next_only",
				mixed: false,
			},
			{
				name:  "mixed_head_next",
				mixed: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tuples := []*openfgav1.TupleKey{
					tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:jon", "condition3", nil),
					tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:elbuo", "condition4", nil),
					tuple.NewTupleKeyWithCondition("document:doc1", "editor", "user:maria", "condition5", nil),
				}
				iter := NewConditionsFilteredTupleKeyIterator(NewStaticTupleKeyIterator(tuples), filter)
				t.Cleanup(iter.Stop)
				if tt.mixed {
					tk, err := iter.Head(context.Background())

					// only the last error is returned
					require.Equal(t, "unknown condition: condition5", err.Error())
					require.Nil(t, tk)
				}
				tk, err := iter.Next(context.Background())

				// only the last error is returned
				require.Equal(t, "unknown condition: condition5", err.Error())
				require.Nil(t, tk)
			})
		}
	})
}
