package iterator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
)

// erroringHeadIterator yields values and then returns err from Head/Next, without advancing.
type erroringHeadIterator struct {
	storage.Iterator[string]
	err error
}

func (e *erroringHeadIterator) Head(ctx context.Context) (string, error) {
	if h, err := e.Iterator.Head(ctx); err == nil {
		return h, nil
	}
	return "", e.err
}

func (e *erroringHeadIterator) Next(ctx context.Context) (string, error) {
	if n, err := e.Iterator.Next(ctx); err == nil {
		return n, nil
	}
	return "", e.err
}

func drain(t *testing.T, iter storage.Iterator[string]) []string {
	t.Helper()
	var got []string
	for {
		v, err := iter.Next(context.Background())
		if errors.Is(err, storage.ErrIteratorDone) {
			return got
		}
		require.NoError(t, err)
		got = append(got, v)
	}
}

func TestDedupe(t *testing.T) {
	t.Run("skips_consecutive_duplicates_via_next", func(t *testing.T) {
		iter := Dedupe(storage.NewStaticIterator([]string{"a", "a", "b", "c", "c", "c", "d"}))
		t.Cleanup(iter.Stop)
		require.Equal(t, []string{"a", "b", "c", "d"}, drain(t, iter))
	})

	t.Run("head_is_consistent_with_next_after_duplicates", func(t *testing.T) {
		ctx := context.Background()
		iter := Dedupe(storage.NewStaticIterator([]string{"a", "a", "b", "b"}))
		t.Cleanup(iter.Stop)

		h, err := iter.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, "a", h)

		n, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "a", n)

		// the duplicate "a" must not surface through Head
		h, err = iter.Head(ctx)
		require.NoError(t, err)
		require.Equal(t, "b", h)

		n, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "b", n)

		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("no_duplicates_passes_everything_through", func(t *testing.T) {
		iter := Dedupe(storage.NewStaticIterator([]string{"a", "b", "c"}))
		t.Cleanup(iter.Stop)
		require.Equal(t, []string{"a", "b", "c"}, drain(t, iter))
	})

	t.Run("empty", func(t *testing.T) {
		iter := Dedupe(storage.NewStaticIterator([]string{}))
		t.Cleanup(iter.Stop)
		_, err := iter.Head(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		require.Empty(t, drain(t, iter))
	})

	t.Run("stop_makes_next_return_done", func(t *testing.T) {
		iter := Dedupe(storage.NewStaticIterator([]string{"a", "b"}))
		iter.Stop()
		_, err := iter.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("is_ordered_forwards_from_inner", func(t *testing.T) {
		iter := Dedupe(storage.NewStaticIterator([]string{"a"}))
		t.Cleanup(iter.Stop)
		require.True(t, iter.IsOrdered())
	})

	t.Run("inner_error_while_skipping_duplicates_is_returned_not_retried", func(t *testing.T) {
		ctx := context.Background()
		boom := errors.New("boom")
		iter := Dedupe(&erroringHeadIterator{Iterator: storage.NewStaticIterator([]string{"a", "a"}), err: boom})
		t.Cleanup(iter.Stop)

		n, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "a", n)

		// the second "a" is skipped; then the inner iterator errors. The error must surface, not loop.
		_, err = iter.Head(ctx)
		require.ErrorIs(t, err, boom)
	})
}
