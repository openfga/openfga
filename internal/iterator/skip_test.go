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

func TestSkipTo(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name         string
		values       []string
		target       string
		expectedHead string
		expectError  bool
		expectDone   bool
	}{
		{
			name:         "should_skip_to_exact_match",
			values:       []string{"a", "b", "c", "d", "e"},
			target:       "c",
			expectedHead: "c",
		},
		{
			name:         "should_skip_to_next_greater_value",
			values:       []string{"a", "b", "d", "e"},
			target:       "c",
			expectedHead: "d",
		},
		{
			name:         "should_not_skip_when_target_is_before_head",
			values:       []string{"c", "d", "e"},
			target:       "a",
			expectedHead: "c",
		},
		{
			name:         "should_handle_target_equal_to_head",
			values:       []string{"a", "b", "c"},
			target:       "a",
			expectedHead: "a",
		},
		{
			name:       "should_handle_target_beyond_all_values",
			values:     []string{"a", "b", "c"},
			target:     "z",
			expectDone: true,
		},
		{
			name:         "should_handle_single_value_iterator",
			values:       []string{"m"},
			target:       "k",
			expectedHead: "m",
		},
		{
			name:       "should_handle_empty_iterator",
			values:     []string{},
			target:     "a",
			expectDone: true,
		},
		{
			name:         "should_handle_target_at_last_value",
			values:       []string{"a", "b", "c"},
			target:       "c",
			expectedHead: "c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			iter := storage.NewStaticIterator(tt.values)
			defer iter.Stop()

			err := SkipTo(ctx, iter, tt.target)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.expectDone {
				_, err := iter.Head(ctx)
				require.ErrorIs(t, err, storage.ErrIteratorDone)
			} else {
				head, err := iter.Head(ctx)
				require.NoError(t, err)
				require.Equal(t, tt.expectedHead, head)
			}
		})
	}
}

func TestSkipToWithError(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("should_return_error_from_head", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := context.Background()
		expectedErr := errors.New("head error")

		iter := mocks.NewMockIterator[string](ctrl)
		iter.EXPECT().Head(ctx).Return("", expectedErr)

		err := SkipTo(ctx, iter, "target")
		require.Error(t, err)
		require.Equal(t, expectedErr.Error(), err.Error())
	})

	t.Run("should_return_error_from_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := context.Background()
		expectedErr := errors.New("next error")

		iter := mocks.NewMockIterator[string](ctrl)
		iter.EXPECT().Head(ctx).Return("a", nil).Times(1)
		iter.EXPECT().Next(ctx).Return("a", nil).Times(1)
		iter.EXPECT().Head(ctx).Return("", expectedErr).Times(1)

		err := SkipTo(ctx, iter, "z")
		require.Error(t, err)
		require.Equal(t, expectedErr.Error(), err.Error())
	})
}

func TestSkipToContextCancellation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("should_handle_context_cancelled_from_head", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		iter := storage.NewStaticIterator([]string{"a", "b", "c"})
		defer iter.Stop()

		err := SkipTo(ctx, iter, "z")
		require.NoError(t, err)
	})

	t.Run("should_handle_context_cancelled_from_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, cancel := context.WithCancel(context.Background())

		iter := mocks.NewMockIterator[string](ctrl)
		iter.EXPECT().Head(ctx).Return("a", nil).Times(1)
		iter.EXPECT().Next(ctx).DoAndReturn(func(context.Context) (string, error) {
			cancel()
			return "a", nil
		}).Times(1)
		iter.EXPECT().Head(gomock.Any()).Return("", context.Canceled).Times(1)

		err := SkipTo(ctx, iter, "z")
		require.NoError(t, err)
	})
}

func TestSkipToWithLexicographicOrdering(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name         string
		values       []string
		target       string
		expectedHead string
	}{
		{
			name:         "should_handle_numeric_strings",
			values:       []string{"1", "10", "2", "20", "3"},
			target:       "2",
			expectedHead: "2",
		},
		{
			name:         "should_handle_prefixes",
			values:       []string{"app", "apple", "application"},
			target:       "appl",
			expectedHead: "apple",
		},
		{
			name:         "should_handle_case_sensitive_comparison",
			values:       []string{"A", "B", "a", "b"},
			target:       "Z",
			expectedHead: "a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			iter := storage.NewStaticIterator(tt.values)
			defer iter.Stop()

			err := SkipTo(ctx, iter, tt.target)
			require.NoError(t, err)

			head, err := iter.Head(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedHead, head)
		})
	}
}
