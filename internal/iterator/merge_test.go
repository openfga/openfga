package iterator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestMergedIterator(t *testing.T) {
	compareFn := func(a, b string) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}

	t.Run("both_iterators_empty", func(t *testing.T) {
		iter1 := storage.NewStaticIterator[string]([]string{})
		iter2 := storage.NewStaticIterator[string]([]string{})

		merged := Merge(iter1, iter2, compareFn)

		_, err := merged.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("first_iterator_empty", func(t *testing.T) {
		iter1 := storage.NewStaticIterator[string]([]string{})
		iter2 := storage.NewStaticIterator[string]([]string{"obj:1", "obj:2"})

		merged := Merge(iter1, iter2, compareFn)

		val1, err := merged.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "obj:1", val1)

		val2, err := merged.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "obj:2", val2)

		_, err = merged.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("second_iterator_empty", func(t *testing.T) {
		iter1 := storage.NewStaticIterator[string]([]string{"obj:1", "obj:2"})
		iter2 := storage.NewStaticIterator[string]([]string{})

		merged := Merge(iter1, iter2, compareFn)

		val1, err := merged.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "obj:1", val1)

		val2, err := merged.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "obj:2", val2)

		_, err = merged.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("merge_sorted_iterators", func(t *testing.T) {
		iter1 := storage.NewStaticIterator[string]([]string{"obj:1", "obj:3", "obj:5"})
		iter2 := storage.NewStaticIterator[string]([]string{"obj:2", "obj:4", "obj:6"})

		merged := Merge(iter1, iter2, compareFn)

		expected := []string{"obj:1", "obj:2", "obj:3", "obj:4", "obj:5", "obj:6"}
		for _, exp := range expected {
			val, err := merged.Next(context.Background())
			require.NoError(t, err)
			require.Equal(t, exp, val)
		}

		_, err := merged.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("skip_duplicates", func(t *testing.T) {
		iter1 := storage.NewStaticIterator[string]([]string{"obj:1", "obj:2", "obj:3", "obj:5"})
		iter2 := storage.NewStaticIterator[string]([]string{"obj:2", "obj:3", "obj:4"})

		merged := Merge(iter1, iter2, compareFn)

		expected := []string{"obj:1", "obj:2", "obj:3", "obj:4", "obj:5"}
		for _, exp := range expected {
			val, err := merged.Next(context.Background())
			require.NoError(t, err)
			require.Equal(t, exp, val)
		}

		_, err := merged.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("all_duplicates", func(t *testing.T) {
		iter1 := storage.NewStaticIterator[string]([]string{"obj:1", "obj:2", "obj:3"})
		iter2 := storage.NewStaticIterator[string]([]string{"obj:1", "obj:2", "obj:3"})

		merged := Merge(iter1, iter2, compareFn)

		expected := []string{"obj:1", "obj:2", "obj:3"}
		for _, exp := range expected {
			val, err := merged.Next(context.Background())
			require.NoError(t, err)
			require.Equal(t, exp, val)
		}

		_, err := merged.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("initialization_error_iter1", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Next(gomock.Any()).Return("", errors.New("init error"))

		iter2 := storage.NewStaticIterator[string]([]string{"obj:1"})

		merged := Merge(iter1, iter2, compareFn)

		_, err := merged.Next(context.Background())
		require.Error(t, err)
		require.Equal(t, "init error", err.Error())
	})

	t.Run("initialization_error_iter2", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := storage.NewStaticIterator[string]([]string{"obj:1"})

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Next(gomock.Any()).Return("", errors.New("init error"))

		merged := Merge(iter1, iter2, compareFn)

		_, err := merged.Next(context.Background())
		require.Error(t, err)
		require.Equal(t, "init error", err.Error())
	})

	t.Run("error_during_iteration_iter1", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Next(gomock.Any()).Return("obj:1", nil)
		iter1.EXPECT().Next(gomock.Any()).Return("", errors.New("iteration error"))

		iter2 := storage.NewStaticIterator[string]([]string{"obj:2"})

		merged := Merge(iter1, iter2, compareFn)

		_, err := merged.Next(context.Background())
		require.Error(t, err)
		require.Equal(t, "iteration error", err.Error())
	})

	t.Run("error_during_iteration_iter2", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := storage.NewStaticIterator[string]([]string{"obj:2"})

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Next(gomock.Any()).Return("obj:1", nil)
		iter2.EXPECT().Next(gomock.Any()).Return("", errors.New("iteration error"))

		merged := Merge(iter1, iter2, compareFn)

		_, err := merged.Next(context.Background())
		require.Error(t, err)
		require.Equal(t, "iteration error", err.Error())
	})

	t.Run("context_canceled_during_initialization", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		iter1 := storage.NewStaticIterator[string]([]string{"obj:1"})
		iter2 := storage.NewStaticIterator[string]([]string{"obj:2"})

		merged := Merge(iter1, iter2, compareFn)

		_, err := merged.Next(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("stop_calls_both_iterators", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Stop()

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Stop()

		merged := Merge(iter1, iter2, compareFn)
		merged.Stop()
	})
}
