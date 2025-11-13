package iterator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestConcat(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name           string
		setupIters     func(*gomock.Controller) (storage.Iterator[string], storage.Iterator[string])
		expectedValues []string
		expectedError  error
	}{
		{
			name: "should_concatenate_two_iterators",
			setupIters: func(ctrl *gomock.Controller) (storage.Iterator[string], storage.Iterator[string]) {
				iter1 := mocks.NewMockIterator[string](ctrl)
				iter1.EXPECT().Next(gomock.Any()).Return("a", nil)
				iter1.EXPECT().Next(gomock.Any()).Return("b", nil)
				iter1.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter1.EXPECT().Stop().Times(1)

				iter2 := mocks.NewMockIterator[string](ctrl)
				iter2.EXPECT().Next(gomock.Any()).Return("c", nil)
				iter2.EXPECT().Next(gomock.Any()).Return("d", nil)
				iter2.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter2.EXPECT().Stop().Times(1)

				return iter1, iter2
			},
			expectedValues: []string{"a", "b", "c", "d"},
			expectedError:  storage.ErrIteratorDone,
		},
		{
			name: "should_handle_empty_first_iterator",
			setupIters: func(ctrl *gomock.Controller) (storage.Iterator[string], storage.Iterator[string]) {
				iter1 := mocks.NewMockIterator[string](ctrl)
				iter1.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter1.EXPECT().Stop().Times(1)

				iter2 := mocks.NewMockIterator[string](ctrl)
				iter2.EXPECT().Next(gomock.Any()).Return("a", nil)
				iter2.EXPECT().Next(gomock.Any()).Return("b", nil)
				iter2.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter2.EXPECT().Stop().Times(1)

				return iter1, iter2
			},
			expectedValues: []string{"a", "b"},
			expectedError:  storage.ErrIteratorDone,
		},
		{
			name: "should_handle_empty_second_iterator",
			setupIters: func(ctrl *gomock.Controller) (storage.Iterator[string], storage.Iterator[string]) {
				iter1 := mocks.NewMockIterator[string](ctrl)
				iter1.EXPECT().Next(gomock.Any()).Return("a", nil)
				iter1.EXPECT().Next(gomock.Any()).Return("b", nil)
				iter1.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter1.EXPECT().Stop().Times(1)

				iter2 := mocks.NewMockIterator[string](ctrl)
				iter2.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter2.EXPECT().Stop().Times(1)

				return iter1, iter2
			},
			expectedValues: []string{"a", "b"},
			expectedError:  storage.ErrIteratorDone,
		},
		{
			name: "should_handle_both_empty_iterators",
			setupIters: func(ctrl *gomock.Controller) (storage.Iterator[string], storage.Iterator[string]) {
				iter1 := mocks.NewMockIterator[string](ctrl)
				iter1.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter1.EXPECT().Stop().Times(1)

				iter2 := mocks.NewMockIterator[string](ctrl)
				iter2.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter2.EXPECT().Stop().Times(1)

				return iter1, iter2
			},
			expectedValues: nil,
			expectedError:  storage.ErrIteratorDone,
		},
		{
			name: "should_handle_error_in_first_iterator",
			setupIters: func(ctrl *gomock.Controller) (storage.Iterator[string], storage.Iterator[string]) {
				iter1 := mocks.NewMockIterator[string](ctrl)
				iter1.EXPECT().Next(gomock.Any()).Return("a", nil)
				expectedErr := errors.New("first iterator error")
				iter1.EXPECT().Next(gomock.Any()).Return("", expectedErr)
				iter1.EXPECT().Stop().Times(1)

				iter2 := mocks.NewMockIterator[string](ctrl)
				iter2.EXPECT().Stop().Times(1)

				return iter1, iter2
			},
			expectedValues: []string{"a"},
			expectedError:  errors.New("first iterator error"),
		},
		{
			name: "should_handle_error_in_second_iterator",
			setupIters: func(ctrl *gomock.Controller) (storage.Iterator[string], storage.Iterator[string]) {
				iter1 := mocks.NewMockIterator[string](ctrl)
				iter1.EXPECT().Next(gomock.Any()).Return("a", nil)
				iter1.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter1.EXPECT().Stop().Times(1)

				iter2 := mocks.NewMockIterator[string](ctrl)
				iter2.EXPECT().Next(gomock.Any()).Return("b", nil)
				expectedErr := errors.New("second iterator error")
				iter2.EXPECT().Next(gomock.Any()).Return("", expectedErr)
				iter2.EXPECT().Stop().Times(1)

				return iter1, iter2
			},
			expectedValues: []string{"a", "b"},
			expectedError:  errors.New("second iterator error"),
		},
		{
			name: "should_return_done_on_subsequent_calls_after_completion",
			setupIters: func(ctrl *gomock.Controller) (storage.Iterator[string], storage.Iterator[string]) {
				iter1 := mocks.NewMockIterator[string](ctrl)
				iter1.EXPECT().Next(gomock.Any()).Return("a", nil)
				iter1.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter1.EXPECT().Stop().Times(1)

				iter2 := mocks.NewMockIterator[string](ctrl)
				iter2.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter2.EXPECT().Stop().Times(1)

				return iter1, iter2
			},
			expectedValues: []string{"a"},
			expectedError:  storage.ErrIteratorDone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			iter1, iter2 := tt.setupIters(ctrl)
			concatIter := Concat(iter1, iter2)
			defer concatIter.Stop()

			ctx := context.Background()
			var values []string

			for {
				val, err := concatIter.Next(ctx)
				if err != nil {
					if tt.expectedError != nil {
						if storage.IterIsDoneOrCancelled(err) {
							require.ErrorIs(t, err, storage.ErrIteratorDone)
						} else {
							require.Error(t, err)
							require.Equal(t, tt.expectedError.Error(), err.Error())
						}
					}
					break
				}
				values = append(values, val)
			}

			require.Equal(t, tt.expectedValues, values)
		})
	}
}

func TestConcatStop(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("should_stop_both_iterators", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Stop().Times(1)

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Stop().Times(1)

		concatIter := Concat[string](iter1, iter2)
		concatIter.Stop()
	})

	t.Run("should_stop_iterators_after_partial_consumption", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Next(gomock.Any()).Return("a", nil)
		iter1.EXPECT().Stop().Times(1)

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Stop().Times(1)

		concatIter := Concat[string](iter1, iter2)
		ctx := context.Background()

		_, err := concatIter.Next(ctx)
		require.NoError(t, err)

		concatIter.Stop()
	})
}

func TestConcatWithContextCancellation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("should_handle_context_cancellation_in_first_iterator", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Next(gomock.Any()).Return("a", nil)
		iter1.EXPECT().Next(gomock.Any()).DoAndReturn(func(ctx context.Context) (string, error) {
			return "", ctx.Err()
		})
		iter1.EXPECT().Stop().Times(1)

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Stop().Times(1)

		concatIter := Concat[string](iter1, iter2)
		defer concatIter.Stop()

		ctx, cancel := context.WithCancel(context.Background())

		val, err := concatIter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "a", val)

		cancel()

		_, err = concatIter.Next(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("should_handle_context_cancellation_in_second_iterator", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Next(gomock.Any()).Return("a", nil)
		iter1.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
		iter1.EXPECT().Stop().Times(1)

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Next(gomock.Any()).DoAndReturn(func(ctx context.Context) (string, error) {
			return "", ctx.Err()
		})
		iter2.EXPECT().Stop().Times(1)

		concatIter := Concat[string](iter1, iter2)
		defer concatIter.Stop()

		ctx, cancel := context.WithCancel(context.Background())

		val, err := concatIter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "a", val)

		cancel()

		_, err = concatIter.Next(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})
}
