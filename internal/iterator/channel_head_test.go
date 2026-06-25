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

func TestChannelIterator_Head(t *testing.T) {
	t.Run("returns_head_of_current_iterator", func(t *testing.T) {
		t.Cleanup(func() { goleak.VerifyNone(t) })
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter := mocks.NewMockIterator[string](ctrl)
		iter.EXPECT().Head(gomock.Any()).Return("a", nil).AnyTimes()
		iter.EXPECT().Stop().Times(1)

		ch := make(chan *Msg, 1)
		ch <- &Msg{Iter: iter}
		close(ch)

		it := FromChannel(ch)
		// Head is non-advancing: repeated calls return the same value.
		v, err := it.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "a", v)

		v, err = it.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "a", v)

		it.Stop()
	})

	t.Run("skips_exhausted_iterator_and_advances_to_next", func(t *testing.T) {
		t.Cleanup(func() { goleak.VerifyNone(t) })
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).Return("", storage.ErrIteratorDone)
		iter1.EXPECT().Stop().Times(1)

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Head(gomock.Any()).Return("b", nil)
		iter2.EXPECT().Stop().Times(1)

		ch := make(chan *Msg, 2)
		ch <- &Msg{Iter: iter1}
		ch <- &Msg{Iter: iter2}
		close(ch)

		it := FromChannel(ch)
		v, err := it.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "b", v)

		it.Stop()
	})

	t.Run("skips_nil_iterator_message", func(t *testing.T) {
		t.Cleanup(func() { goleak.VerifyNone(t) })
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		iter := mocks.NewMockIterator[string](ctrl)
		iter.EXPECT().Head(gomock.Any()).Return("x", nil)
		iter.EXPECT().Stop().Times(1)

		ch := make(chan *Msg, 2)
		ch <- &Msg{Iter: nil}
		ch <- &Msg{Iter: iter}
		close(ch)

		it := FromChannel(ch)
		v, err := it.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "x", v)

		it.Stop()
	})

	t.Run("propagates_channel_error_message", func(t *testing.T) {
		t.Cleanup(func() { goleak.VerifyNone(t) })
		wantErr := errors.New("channel boom")

		ch := make(chan *Msg, 1)
		ch <- &Msg{Err: wantErr}
		close(ch)

		it := FromChannel(ch)
		_, err := it.Head(context.Background())
		require.ErrorIs(t, err, wantErr)

		it.Stop()
	})

	t.Run("propagates_non_done_iterator_error", func(t *testing.T) {
		t.Cleanup(func() { goleak.VerifyNone(t) })
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		wantErr := errors.New("iter boom")
		iter := mocks.NewMockIterator[string](ctrl)
		iter.EXPECT().Head(gomock.Any()).Return("", wantErr)
		iter.EXPECT().Stop().Times(1)

		ch := make(chan *Msg, 1)
		ch <- &Msg{Iter: iter}
		close(ch)

		it := FromChannel(ch)
		_, err := it.Head(context.Background())
		require.ErrorIs(t, err, wantErr)

		it.Stop()
	})

	t.Run("returns_done_on_empty_closed_channel", func(t *testing.T) {
		t.Cleanup(func() { goleak.VerifyNone(t) })
		ch := make(chan *Msg)
		close(ch)

		it := FromChannel(ch)
		_, err := it.Head(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)

		it.Stop()
	})

	t.Run("returns_context_error_when_canceled", func(t *testing.T) {
		t.Cleanup(func() { goleak.VerifyNone(t) })
		ch := make(chan *Msg) // never written to
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		it := FromChannel(ch)
		_, err := it.Head(ctx)
		require.ErrorIs(t, err, context.Canceled)

		close(ch)
		it.Stop()
	})

	t.Run("returns_done_after_stop", func(t *testing.T) {
		t.Cleanup(func() { goleak.VerifyNone(t) })
		ch := make(chan *Msg)
		close(ch)

		it := FromChannel(ch)
		it.Stop()

		_, err := it.Head(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})
}
