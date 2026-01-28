package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

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
	t.Run("next_and_head_are_thread_safe", func(t *testing.T) {
		tks := make([]*openfgav1.Tuple, 100)
		for i := 0; i < 100; i++ {
			tks[i] = &openfgav1.Tuple{
				Key:       tuple.NewTupleKey("document:doc"+strconv.Itoa(i), "viewer", "bill"),
				Timestamp: timestamppb.New(time.Now()),
			}
		}
		iter := NewStaticTupleIterator(tks)
		defer iter.Stop()

		var wg errgroup.Group
		for i := 0; i < 101; i++ {
			wg.Go(func() error {
				_, err := iter.Head(context.Background())
				return err
			})
		}

		err := wg.Wait()
		require.NoError(t, err)

		for i := 0; i < 101; i++ {
			wg.Go(func() error {
				_, err := iter.Next(context.Background())
				return err
			})
		}

		err = wg.Wait()
		require.ErrorIs(t, err, ErrIteratorDone)
	})
}

type mockStoppedIterator[T any] struct {
	stopped bool
}

func (c *mockStoppedIterator[T]) Next(ctx context.Context) (T, error) {
	var val T
	return val, ErrIteratorDone
}

func (c *mockStoppedIterator[T]) Stop() {
	c.stopped = true
}

func (c *mockStoppedIterator[T]) Head(ctx context.Context) (T, error) {
	var val T
	return val, nil
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

		_, err := iter.Head(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
	})
	t.Run("head_empty_slices", func(t *testing.T) {
		iter1 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{})
		iter2 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{})
		iter := NewCombinedIterator(iter1, iter2)
		defer iter.Stop()
		tk, err := iter.Head(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
		require.Nil(t, tk)
	})
	t.Run("head_empty_iter", func(t *testing.T) {
		iter := NewCombinedIterator[[]*openfgav1.Tuple]()
		defer iter.Stop()
		tk, err := iter.Head(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
		require.Nil(t, tk)
	})
	t.Run("nil_iterator", func(t *testing.T) {
		iter := NewCombinedIterator[[]*openfgav1.Tuple](nil)
		defer iter.Stop()
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
		defer iter.Stop()

		for i := 0; i < len(expected); i++ {
			tk, err := iter.Head(context.Background())
			require.NoError(t, err)
			require.Equal(t, expected[i], tk)

			tk, err = iter.Head(context.Background())
			require.NoError(t, err)
			require.Equal(t, expected[i], tk)

			tk, err = iter.Next(context.Background())
			require.NoError(t, err)
			require.Equal(t, expected[i], tk)
		}

		_, err := iter.Head(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)

		_, err = iter.Next(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
	})
	t.Run("ctx_error_head", func(t *testing.T) {
		tks := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc1", "viewer", "bill"),
			tuple.NewTupleKey("document:doc2", "editor", "bob"),
			tuple.NewTupleKey("document:doc2", "editor", "charles"),
		}

		iter1 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{tks[0]})
		iter2 := NewStaticTupleKeyIterator([]*openfgav1.TupleKey{tks[1], tks[2]})
		iter := NewCombinedIterator(iter1, iter2)
		defer iter.Stop()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := iter.Head(ctx)
		require.ErrorIs(t, err, context.Canceled)

		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("stop", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		iter1 := &mockStoppedIterator[openfgav1.TupleKey]{}
		iter2 := &mockStoppedIterator[openfgav1.TupleKey]{}

		iter := NewCombinedIterator(iter1, iter2)

		_, _ = iter.Next(context.Background())

		iter.Stop()

		require.True(t, iter1.stopped)
		require.True(t, iter2.stopped)
	})
}

type combinedIterTestCasesStruct = map[string]struct {
	iter1    []*openfgav1.Tuple
	iter2    []*openfgav1.Tuple
	expected []string
}

var combinedIterUserMapperTestCases = combinedIterTestCasesStruct{
	`removes_duplicates_within_iterator`: {
		iter1: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
		},
		iter2: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:b")},
			{Key: tuple.NewTupleKey("document:1", "2", "user:c")},
		},
		expected: []string{
			"user:a", "user:b", "user:c",
		},
	},
	`removes_duplicates_across_iterators_first_entry`: {
		iter1: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
		},
		iter2: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
		},
		expected: []string{"user:a"},
	},
	`removes_duplicates_across_iterators_last_entry`: {
		iter1: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:1", "2", "user:b")},
		},
		iter2: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:b")},
		},
		expected: []string{
			"user:a", "user:b",
		},
	},
	`non_overlapping_elements_returns_all`: {
		iter1: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:1", "2", "user:c")},
			{Key: tuple.NewTupleKey("document:1", "2", "user:e")},
		},
		iter2: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:b")},
			{Key: tuple.NewTupleKey("document:1", "2", "user:d")},
			{Key: tuple.NewTupleKey("document:1", "2", "user:f")},
		},
		expected: []string{
			"user:a", "user:b", "user:c", "user:d", "user:e", "user:f",
		},
	},
	`overlapping_elements`: {
		iter1: []*openfgav1.Tuple{},
		iter2: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:2", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:2", "2", "user:b")},
		},
		expected: []string{"user:a", "user:b"},
	},
	`overlapping_elements_last_items_across_iter`: {
		iter1: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:2", "2", "user:a")},
		},
		iter2: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:2", "2", "user:b")},
		},
		expected: []string{"user:a", "user:b"},
	},
	`many_overlapping_elements`: {
		iter1: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:2", "2", "user:c")},
			{Key: tuple.NewTupleKey("document:3", "2", "user:c")},
			{Key: tuple.NewTupleKey("document:4", "2", "user:c")},
		},
		iter2: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:2", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:2", "2", "user:b")},
			{Key: tuple.NewTupleKey("document:3", "2", "user:b")},
			{Key: tuple.NewTupleKey("document:4", "2", "user:b")},
			{Key: tuple.NewTupleKey("document:5", "2", "user:b")},
			{Key: tuple.NewTupleKey("document:6", "2", "user:b")},
		},
		expected: []string{"user:a", "user:b", "user:c"},
	},

	`all_empty_iterators`: {
		iter1:    []*openfgav1.Tuple{},
		iter2:    []*openfgav1.Tuple{},
		expected: []string{},
	},
	`one_empty_iterator`: {
		iter1: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
		},
		iter2:    []*openfgav1.Tuple{},
		expected: []string{"user:a"},
	},
}

var combinedIterObjectMapperTestCases = combinedIterTestCasesStruct{
	`iter_map_to_object`: {
		iter1: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:*")},
			{Key: tuple.NewTupleKey("document:2", "2", "user:*")},
		},
		iter2: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:2", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:4", "2", "user:a")},
		},
		expected: []string{
			"document:1", "document:2", "document:4",
		},
	},
	`same_object_different_rel`: {
		iter1: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:2", "2", "user:a")},
			{Key: tuple.NewTupleKey("document:2", "3", "user:a")},
			{Key: tuple.NewTupleKey("document:3", "2", "user:a")},
		},
		iter2: []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:*")},
			{Key: tuple.NewTupleKey("document:2", "2", "user:*")},
			{Key: tuple.NewTupleKey("document:4", "2", "user:*")},
		},
		expected: []string{
			"document:1", "document:2", "document:3", "document:4",
		},
	},
}

var combinedTestCases = map[string]struct {
	mapper    TupleMapperFunc
	testcases combinedIterTestCasesStruct
}{
	"userMapper": {
		mapper:    UserMapper(),
		testcases: combinedIterUserMapperTestCases,
	},
	"objectMapper": {
		mapper:    ObjectMapper(),
		testcases: combinedIterObjectMapperTestCases,
	},
}

func TestOrderedCombinedIterator(t *testing.T) {
	t.Run("Stop", func(t *testing.T) {
		iter1 := NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
		})
		iter := NewOrderedCombinedIterator(UserMapper(), iter1, nil)
		iter.Stop()
		require.Len(t, iter.pending, 1)
		_, err := iter.Next(context.Background())
		require.ErrorIs(t, err, ErrIteratorDone)
	})

	t.Run("Next", func(t *testing.T) {
		for name, combinedTest := range combinedTestCases {
			t.Run(name, func(t *testing.T) {
				for name, tc := range combinedTest.testcases {
					t.Run(name, func(t *testing.T) {
						mapper := combinedTest.mapper

						iter := NewOrderedCombinedIterator(mapper, NewStaticTupleIterator(tc.iter1), NewStaticTupleIterator(tc.iter2))
						t.Cleanup(func() {
							iter.Stop()
							require.Empty(t, iter.pending)
						})

						gotItems := make([]string, 0)
						for {
							got, err := iter.Next(context.Background())
							if err != nil {
								if errors.Is(err, ErrIteratorDone) {
									break
								}
								require.Fail(t, "no error was expected")
							}
							require.NotNil(t, got)
							gotItems = append(gotItems, mapper(got))
						}

						require.Equal(t, tc.expected, gotItems)
					})
				}
			})
		}
	})

	t.Run("Head", func(t *testing.T) {
		t.Run("return_err_if_unsorted_input_iterator", func(t *testing.T) {
			iter1 := NewStaticTupleIterator([]*openfgav1.Tuple{
				{Key: tuple.NewTupleKey("document:1", "2", "user:b")},
				{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
			})
			iter2 := NewStaticTupleIterator([]*openfgav1.Tuple{})

			iter := NewOrderedCombinedIterator(UserMapper(), iter1, iter2)
			t.Cleanup(iter.Stop)

			next, err := iter.Next(context.Background())
			require.NoError(t, err)
			require.Equal(t, "document:1#2@user:b", tuple.TupleKeyToString(next.GetKey()))

			next, err = iter.Next(context.Background())
			require.Nil(t, next)
			require.ErrorContains(t, err, "iterator 0 is not in ascending order")
		})
		t.Run("multiple_calls_to_head_should_return_same_value", func(t *testing.T) {
			iter1 := NewStaticTupleIterator([]*openfgav1.Tuple{
				{Key: tuple.NewTupleKey("document:1", "2", "user:a")},
				{Key: tuple.NewTupleKey("document:1", "2", "user:b")},
			})
			iter2 := NewStaticTupleIterator([]*openfgav1.Tuple{
				{Key: tuple.NewTupleKey("document:1", "2", "user:c")},
				{Key: tuple.NewTupleKey("document:1", "2", "user:d")},
			})
			expected := &openfgav1.Tuple{
				Key: tuple.NewTupleKey("document:1", "2", "user:a"),
			}

			iter := NewOrderedCombinedIterator(UserMapper(), iter1, iter2)
			t.Cleanup(iter.Stop)

			for i := 0; i < 5; i++ {
				got, err := iter.Head(context.Background())
				require.NoError(t, err)

				if diff := cmp.Diff(expected, got, protocmp.Transform()); diff != "" {
					t.Errorf("mismatch (-want +got):\n%s", diff)
				}
			}
		})
		t.Run("head_and_next_interleaved", func(t *testing.T) {
			for name, combinedTest := range combinedTestCases {
				t.Run(name, func(t *testing.T) {
					for name, tc := range combinedTest.testcases {
						t.Run(name, func(t *testing.T) {
							mapper := combinedTest.mapper

							iter := NewOrderedCombinedIterator(mapper, NewStaticTupleIterator(tc.iter1), NewStaticTupleIterator(tc.iter2))
							t.Cleanup(iter.Stop)

							var errorFromHead error
							gotItems := make([]string, 0)
							for {
								gotHead, err := iter.Head(context.Background())
								if err != nil {
									require.ErrorIs(t, err, ErrIteratorDone)
									errorFromHead = err
								}
								gotNext, err := iter.Next(context.Background())
								if err != nil {
									require.Equal(t, errorFromHead, err)
									break
								}
								require.NotNil(t, gotNext)
								if diff := cmp.Diff(gotHead, gotNext, protocmp.Transform()); diff != "" {
									t.Errorf("mismatch in result of Next compared to Head (-want +got):\n%s", diff)
								}
								gotItems = append(gotItems, mapper(gotNext))
							}

							require.Equal(t, tc.expected, gotItems)
						})
					}
				})
			}
		})
		t.Run("head_head_and_next_interleaved", func(t *testing.T) {
			for name, combinedTest := range combinedTestCases {
				t.Run(name, func(t *testing.T) {
					for name, tc := range combinedTest.testcases {
						t.Run(name, func(t *testing.T) {
							mapper := combinedTest.mapper

							iter := NewOrderedCombinedIterator(mapper, NewStaticTupleIterator(tc.iter1), NewStaticTupleIterator(tc.iter2))
							t.Cleanup(iter.Stop)

							var errorFromHead error
							gotItems := make([]string, 0)
							for {
								gotHead, err := iter.Head(context.Background())
								if err != nil {
									require.ErrorIs(t, err, ErrIteratorDone)
									errorFromHead = err
								}
								var newHead *openfgav1.Tuple
								newHead, err = iter.Head(context.Background())
								if err != nil {
									require.Equal(t, errorFromHead, err)
									require.Nil(t, newHead)
								} else {
									require.Equal(t, gotHead, newHead)
								}

								gotNext, err := iter.Next(context.Background())
								if err != nil {
									require.Equal(t, errorFromHead, err)
									break
								}
								require.NotNil(t, gotNext)
								if diff := cmp.Diff(gotHead, gotNext, protocmp.Transform()); diff != "" {
									t.Errorf("mismatch in result of Next compared to Head (-want +got):\n%s", diff)
								}
								gotItems = append(gotItems, mapper(gotNext))
							}

							require.Equal(t, tc.expected, gotItems)
						})
					}
				})
			}
		})
	})
}

func TestTupleKeyIteratorFromTupleIterator(t *testing.T) {
	tests := []struct {
		name  string
		mixed bool
	}{
		{
			name:  "nextOnly",
			mixed: false,
		},
		{
			name:  "mixed",
			mixed: true,
		},
	}
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			staticTupleIterator := NewStaticTupleIterator(expected)
			defer staticTupleIterator.Stop()
			iter := NewTupleKeyIteratorFromTupleIterator(staticTupleIterator)
			defer iter.Stop()

			if tt.mixed {
				tk, err := iter.Head(context.Background())
				require.NoError(t, err)
				require.Equal(t, expected[0].GetKey(), tk)
			}

			tk, err := iter.Next(context.Background())
			require.NoError(t, err)
			require.Equal(t, expected[0].GetKey(), tk)

			if tt.mixed {
				tk, err := iter.Head(context.Background())
				require.NoError(t, err)
				require.Equal(t, expected[1].GetKey(), tk)
			}

			tk, err = iter.Next(context.Background())
			require.NoError(t, err)
			require.Equal(t, expected[1].GetKey(), tk)

			if tt.mixed {
				_, err := iter.Head(context.Background())
				require.ErrorIs(t, err, ErrIteratorDone)
			}

			_, err = iter.Next(context.Background())
			require.ErrorIs(t, err, ErrIteratorDone)
		})
	}
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
	t.Run("head", func(t *testing.T) {
		t.Run("empty_slices", func(t *testing.T) {
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
		t.Run("non_empty_slices", func(t *testing.T) {
			tests := []struct {
				name                 string
				unmatchedFilterFirst bool // first item does not match filter
				unmatchedFilterLast  bool
			}{
				{
					name:                 "matchedFirstLast",
					unmatchedFilterFirst: false,
					unmatchedFilterLast:  false,
				},
				{
					name:                 "unmatchedFirst",
					unmatchedFilterFirst: true,
					unmatchedFilterLast:  false,
				},
				{
					name:                 "unmatchedLast",
					unmatchedFilterFirst: false,
					unmatchedFilterLast:  true,
				},
				{
					name:                 "unmatchedFirstLast",
					unmatchedFilterFirst: true,
					unmatchedFilterLast:  true,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					var tuples []*openfgav1.TupleKey
					if tt.unmatchedFilterFirst {
						tuples = append(tuples, tuple.NewTupleKey("document:doc1", "viewer", "user:jon"))
					}
					tuples = append(tuples, []*openfgav1.TupleKey{
						tuple.NewTupleKey("document:doc1", "editor", "user:elbuo"),
						tuple.NewTupleKey("document:doc2", "viewer", "user:elbuo"),
						tuple.NewTupleKey("document:doc2", "editor", "user:charlie")}...)
					if tt.unmatchedFilterLast {
						tuples = append(tuples, tuple.NewTupleKey("document:doc1", "viewer", "user:donald"))
					}
					expectedTuples := []*openfgav1.TupleKey{
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
					tk, err := iter.Head(context.Background())
					require.NoError(t, err)
					require.Equal(t, expectedTuples[0], tk)

					// ensure the underlying iterator is not updated
					tk, err = iter.Head(context.Background())
					require.NoError(t, err)
					require.Equal(t, expectedTuples[0], tk)

					tk, err = iter.Next(context.Background())
					require.NoError(t, err)
					require.Equal(t, expectedTuples[0], tk)

					tk, err = iter.Head(context.Background())
					require.NoError(t, err)
					require.Equal(t, expectedTuples[1], tk)

					tk, err = iter.Next(context.Background())
					require.NoError(t, err)
					require.Equal(t, expectedTuples[1], tk)

					// we should have consumed all valid items at this point
					_, err = iter.Head(context.Background())
					require.ErrorIs(t, err, ErrIteratorDone)

					_, err = iter.Next(context.Background())
					require.ErrorIs(t, err, ErrIteratorDone)
				})
			}
		})
	})

	t.Run("next_and_head_are_thread_safe", func(t *testing.T) {
		tuples := make([]*openfgav1.TupleKey, 100)
		for i := 0; i < 100; i++ {
			tuples[i] = tuple.NewTupleKey("document:doc"+strconv.Itoa(i), "viewer", "user:jon")
		}
		iter := NewFilteredTupleKeyIterator(
			NewStaticTupleKeyIterator(tuples),
			func(tk *openfgav1.TupleKey) bool {
				return true
			},
		)
		defer iter.Stop()

		var wg errgroup.Group
		for i := 0; i < 101; i++ {
			wg.Go(func() error {
				_, err := iter.Head(context.Background())
				return err
			})
		}

		err := wg.Wait()
		require.NoError(t, err)

		for i := 0; i < 101; i++ {
			wg.Go(func() error {
				_, err := iter.Next(context.Background())
				return err
			})
		}

		err = wg.Wait()
		require.ErrorIs(t, err, ErrIteratorDone)
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
	t.Run("ctx_timeout_head", func(t *testing.T) {
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

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				if tt.mixed {
					_, err := iter.Head(ctx)
					require.Error(t, err)
					require.NotEqual(t, ErrIteratorDone, err)
				}

				_, err := iter.Next(ctx)
				require.Error(t, err)
				require.NotEqual(t, ErrIteratorDone, err)
			})
		}
	})
}

func TestIterIsDoneOrCancelled(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{
			err:      ErrIteratorDone,
			expected: true,
		},
		{
			err:      context.Canceled,
			expected: true,
		},
		{
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			err:      fmt.Errorf("some error"),
			expected: false,
		},
		{
			err:      nil,
			expected: false,
		},
	}
	for _, tt := range tests {
		output := IterIsDoneOrCancelled(tt.err)
		require.Equal(t, tt.expected, output)
	}
}
