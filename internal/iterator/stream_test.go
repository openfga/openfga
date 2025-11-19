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

func TestIteratorStream(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("fetchSource", func(t *testing.T) {
		t.Run("no_nil_buffer_should_not_fetch", func(t *testing.T) {
			stream := &Stream{
				buffer: storage.NewStaticIterator[string]([]string{"obj:0", "obj:1", "obj:2", "obj:3"})}
			err := stream.fetchSource(context.Background())
			// notice there is no error even when there is no source
			require.NoError(t, err)
			require.False(t, stream.sourceIsClosed)
		})
		t.Run("done_should_not_fetch", func(t *testing.T) {
			stream := &Stream{sourceIsClosed: true}
			err := stream.fetchSource(context.Background())
			// notice there is no error
			require.NoError(t, err)
		})
		t.Run("handle_ctx_cancel", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			stream := NewStream(0, nil)
			err := stream.fetchSource(ctx)
			require.ErrorIs(t, err, context.Canceled)
		})
		t.Run("fetch_from_source", func(t *testing.T) {
			c := make(chan *Msg, 1)
			stream := NewStream(0, c)
			c <- &Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:1"})}
			err := stream.fetchSource(context.Background())
			require.NoError(t, err)
			require.False(t, stream.sourceIsClosed)
			require.NotNil(t, stream.buffer)
		})
		t.Run("fetch_from_source_has_error", func(t *testing.T) {
			c := make(chan *Msg, 1)
			stream := NewStream(0, c)
			c <- &Msg{Err: errors.New("fetch err")}
			err := stream.fetchSource(context.Background())
			require.Error(t, err)
			require.Nil(t, stream.buffer)
		})
		t.Run("source_cannot_be_fetched", func(t *testing.T) {
			c := make(chan *Msg, 1)
			stream := NewStream(0, c)
			close(c)
			err := stream.fetchSource(context.Background())
			require.NoError(t, err)
			require.True(t, stream.sourceIsClosed)
			require.Nil(t, stream.buffer)
		})
	})

	t.Run("isDone", func(t *testing.T) {
		t.Run("non_empty_buffer_not_done", func(t *testing.T) {
			dut := Stream{
				sourceIsClosed: false,
				buffer:         storage.NewStaticIterator[string]([]string{}),
			}
			require.False(t, dut.isDone())
		})
		t.Run("source_is_closed", func(t *testing.T) {
			dut := Stream{
				sourceIsClosed: true,
				buffer:         nil,
			}
			require.True(t, dut.isDone())
		})
		t.Run("buffer_empty_only", func(t *testing.T) {
			dut := Stream{
				buffer:         nil,
				sourceIsClosed: false,
			}
			require.False(t, dut.isDone())
		})
		t.Run("non_empty_buffer_but_done", func(t *testing.T) {
			dut := Stream{
				sourceIsClosed: true,
				buffer:         storage.NewStaticIterator[string]([]string{}),
			}
			require.False(t, dut.isDone())
		})
	})

	t.Run("Idx", func(t *testing.T) {
		dut := NewStream(5, nil)
		require.Equal(t, 5, dut.Idx())
	})

	t.Run("Head", func(t *testing.T) {
		t.Run("nil_buffer", func(t *testing.T) {
			dut := NewStream(0, nil)
			item, err := dut.Head(context.Background())
			require.ErrorIs(t, err, storage.ErrIteratorDone)
			require.Empty(t, item)
		})
		t.Run("empty_buffer", func(t *testing.T) {
			dut := &Stream{
				buffer: storage.NewStaticIterator[string]([]string{}),
			}
			item, err := dut.Head(context.Background())
			require.ErrorIs(t, err, storage.ErrIteratorDone)
			require.Empty(t, item)
			require.Empty(t, dut.buffer)
		})
		t.Run("non_empty_buffer", func(t *testing.T) {
			item := "obj:1"
			dut := &Stream{
				buffer: storage.NewStaticIterator[string]([]string{item}),
			}
			res, err := dut.Head(context.Background())
			require.NoError(t, err)
			require.NotNil(t, item)
			require.Equal(t, item, res)
			require.NotEmpty(t, dut.buffer)
		})
	})

	t.Run("Next", func(t *testing.T) {
		t.Run("nil_buffer", func(t *testing.T) {
			dut := NewStream(0, nil)
			item, err := dut.Next(context.Background())
			require.ErrorIs(t, err, storage.ErrIteratorDone)
			require.Empty(t, item)
		})
		t.Run("empty_buffer", func(t *testing.T) {
			dut := &Stream{
				buffer: storage.NewStaticIterator[string]([]string{}),
			}
			item, err := dut.Next(context.Background())
			require.ErrorIs(t, err, storage.ErrIteratorDone)
			require.Empty(t, item)
			require.Empty(t, dut.buffer)
		})
		t.Run("non_empty_buffer", func(t *testing.T) {
			item := "obj:1"
			dut := &Stream{
				buffer: storage.NewStaticIterator[string]([]string{item}),
			}
			item, err := dut.Next(context.Background())
			require.NoError(t, err)
			require.NotNil(t, item)
			require.Equal(t, "obj:1", item)
			require.NotEmpty(t, dut.buffer)
			_, err = dut.Head(context.Background())
			require.ErrorIs(t, err, storage.ErrIteratorDone)
		})
	})

	t.Run("Drain", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			dut := Stream{
				buffer: storage.NewStaticIterator[string]([]string{}),
			}
			items, err := dut.Drain(context.Background())
			require.NoError(t, err)
			require.Empty(t, items)
		})
		t.Run("nonEmpty", func(t *testing.T) {
			dut := Stream{
				buffer: storage.NewStaticIterator[string]([]string{"obj:1", "obj:2"}),
			}
			items, err := dut.Drain(context.Background())
			require.NoError(t, err)
			require.Len(t, items, 2)
			require.Equal(t, "obj:1", items[0])
			require.Equal(t, "obj:2", items[1])
			require.Nil(t, dut.buffer)
		})
		t.Run("error", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			iter1 := mocks.NewMockIterator[string](ctrl)
			iter1.EXPECT().Next(gomock.Any()).Times(1).Return("", errors.New("boom"))
			dut := Stream{buffer: iter1}
			items, err := dut.Drain(context.Background())
			require.Error(t, err)
			require.Nil(t, items)
		})
	})
	t.Run("SkipToTargetObject", func(t *testing.T) {
		t.Run("bad_target", func(t *testing.T) {
			dut := Stream{
				buffer: storage.NewStaticIterator[string]([]string{}),
			}
			err := dut.SkipToTargetObject(context.Background(), "bad_target")
			require.Error(t, err)
		})
		t.Run("empty", func(t *testing.T) {
			dut := Stream{
				buffer: storage.NewStaticIterator[string]([]string{}),
			}
			err := dut.SkipToTargetObject(context.Background(), "obj:2")
			require.NoError(t, err)
			require.Nil(t, dut.buffer)
		})
		t.Run("head_error", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			iter1 := mocks.NewMockIterator[string](ctrl)
			iter1.EXPECT().Head(gomock.Any()).Times(1).Return("", errors.New("boom"))
			dut := Stream{buffer: iter1}
			require.Error(t, dut.SkipToTargetObject(context.Background(), "obj:2"))
		})
		t.Run("skip_head_error", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			iter1 := mocks.NewMockIterator[string](ctrl)
			iter1.EXPECT().Head(gomock.Any()).Times(1).Return("obj:0", nil)
			iter1.EXPECT().Next(gomock.Any()).Times(1).Return("obj:0", nil)
			iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))
			dut := Stream{buffer: iter1}
			require.Error(t, dut.SkipToTargetObject(context.Background(), "obj:2"))
		})
		t.Run("allItemsAreSmaller", func(t *testing.T) {
			ctx := context.Background()

			dut := Stream{
				buffer: storage.NewStaticIterator[string]([]string{"obj:0", "obj:1", "obj:2", "obj:3"}),
			}
			err := dut.SkipToTargetObject(ctx, "obj:4")
			require.NoError(t, err)
			require.Nil(t, dut.buffer)
		})

		t.Run("skipToMatchingItem", func(t *testing.T) {
			ctx := context.Background()

			dut := Stream{
				buffer: storage.NewStaticIterator[string]([]string{"obj:0", "obj:1", "obj:2", "obj:3"}),
			}
			err := dut.SkipToTargetObject(ctx, "obj:2")
			require.NoError(t, err)
			item1, err := dut.buffer.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "obj:2", item1)
			item2, err := dut.buffer.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "obj:3", item2)
			_, err = dut.buffer.Next(ctx)
			require.ErrorIs(t, err, storage.ErrIteratorDone)
		})

		t.Run("skipToNextItem", func(t *testing.T) {
			ctx := context.Background()

			dut := Stream{
				buffer: storage.NewStaticIterator[string]([]string{"obj:0", "obj:1", "obj:3", "obj:4"}),
			}
			err := dut.SkipToTargetObject(ctx, "obj:2")
			require.NoError(t, err)
			item1, err := dut.buffer.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "obj:3", item1)
			item2, err := dut.buffer.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "obj:4", item2)
			_, err = dut.buffer.Next(ctx)
			require.ErrorIs(t, err, storage.ErrIteratorDone)
		})
	})
	t.Run("Stop", func(t *testing.T) {
		sourceIter := storage.NewStaticIterator[string]([]string{"obj:10", "obj:11"})

		c := make(chan *Msg, 1)
		c <- &Msg{Iter: sourceIter}
		close(c)

		buf := storage.NewStaticIterator[string]([]string{"obj:0", "obj:1", "obj:3"})

		dut := &Stream{
			buffer: buf,
			source: c,
		}
		dut.Stop()

		_, bufErr := buf.Next(context.Background())
		require.ErrorIs(t, bufErr, storage.ErrIteratorDone)
	})
}

func TestNextItemInSliceStreams(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("error_on_empty", func(t *testing.T) {
		stream1 := &Stream{buffer: storage.NewStaticIterator[string]([]string{})}
		stream2 := &Stream{buffer: storage.NewStaticIterator[string]([]string{"obj:0"})}
		item, err := NextItemInSliceStreams(context.Background(), []*Stream{stream1, stream2}, []int{0, 1})
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		require.Empty(t, item)
	})
	t.Run("skip_item", func(t *testing.T) {
		stream1 := &Stream{buffer: storage.NewStaticIterator[string]([]string{"obj:3"})}
		stream2 := &Stream{buffer: storage.NewStaticIterator[string]([]string{"obj:0", "obj:1"})}
		stream3 := &Stream{buffer: storage.NewStaticIterator[string]([]string{"obj:0", "obj:2"})}
		item, err := NextItemInSliceStreams(context.Background(), []*Stream{stream1, stream2, stream3}, []int{1, 2})
		require.NoError(t, err)
		require.Equal(t, "obj:0", item)
		// validate that only stream2 and stream3 are iterated to next item
		stream1Item, err := stream1.buffer.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "obj:3", stream1Item)
		stream2Item, err := stream2.buffer.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "obj:1", stream2Item)
		stream3Item, err := stream3.buffer.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "obj:2", stream3Item)
	})
}

func TestIteratorStreams(t *testing.T) {
	t.Run("CleanDone", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		t.Run("should_exit_on_context_cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			stream := NewStreams([]*Stream{{sourceIsClosed: false}})
			_, err := stream.CleanDone(ctx)
			require.Equal(t, context.Canceled, err)
		})
		t.Run("should_error_out_with_producer_errors", func(t *testing.T) {
			ctx := context.Background()
			c := make(chan *Msg, 1)
			expectedErr := errors.New("boom")
			c <- &Msg{Err: expectedErr}
			stream := NewStreams([]*Stream{{sourceIsClosed: true}, {sourceIsClosed: false, source: c}})
			_, err := stream.CleanDone(ctx)
			require.Equal(t, expectedErr, err)
		})
		t.Run("should_filter_out_drained_producers", func(t *testing.T) {
			ctx := context.Background()
			streams := make([]*Stream, 0)
			expectedLen := 0
			for i := 0; i < 5; i++ {
				c := make(chan *Msg, 1)
				producer := NewStream(0, c)
				if i%2 == 0 {
					close(c)
				} else {
					expectedLen++
					c <- &Msg{Iter: storage.NewStaticIterator[string]([]string{})}
				}
				streams = append(streams, producer)
			}
			dut := NewStreams(streams)
			require.Equal(t, 5, dut.GetActiveStreamsCount())
			res, err := dut.CleanDone(ctx)
			require.NoError(t, err)
			require.Len(t, res, expectedLen)
		})
		t.Run("should_return_empty_when_fully_drained", func(t *testing.T) {
			ctx := context.Background()
			streams := make([]*Stream, 0)
			for i := 0; i < 5; i++ {
				producer := &Stream{buffer: nil, sourceIsClosed: true}
				streams = append(streams, producer)
			}
			stream := NewStreams(streams)
			res, err := stream.CleanDone(ctx)
			require.NoError(t, err)
			require.Empty(t, res)
		})
	})
	t.Run("Stop", func(t *testing.T) {
		streams := make([]*Stream, 3)
		for i := 0; i < 3; i++ {
			buf := storage.NewStaticIterator[string]([]string{"obj:0", "obj:1", "obj:3"})
			c := make(chan *Msg, 1)
			close(c)
			streams[i] = &Stream{
				buffer: buf,
				source: c,
			}
		}
		dut := NewStreams(streams)
		dut.Stop()
		for _, stream := range streams {
			_, err := stream.Head(context.Background())
			require.ErrorIs(t, err, storage.ErrIteratorDone)
		}
	})
}
