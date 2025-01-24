package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestIteratorStream(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("drain", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			dut := iteratorStream{
				buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{}),
			}
			items, err := dut.drain(context.Background())
			require.NoError(t, err)
			require.Empty(t, items)
		})
		t.Run("nonEmpty", func(t *testing.T) {
			dut := iteratorStream{
				buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
					{
						Object: "1",
					},
					{
						Object: "2",
					},
				}),
			}
			items, err := dut.drain(context.Background())
			require.NoError(t, err)
			require.Len(t, items, 2)
			require.Equal(t, "1", items[0].GetObject())
			require.Equal(t, "2", items[1].GetObject())
			require.Nil(t, dut.buffer)
		})
		t.Run("error", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
			iter1.EXPECT().Next(gomock.Any()).Times(1).Return(nil, errors.New("boom"))
			dut := iteratorStream{buffer: iter1}
			items, err := dut.drain(context.Background())
			require.Error(t, err)
			require.Nil(t, items)
		})
	})
	t.Run("skipToTarget", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			dut := iteratorStream{
				buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{}),
			}
			err := dut.skipToTarget(context.Background(), "2")
			require.NoError(t, err)
			require.Nil(t, dut.buffer)
		})
		t.Run("head_error", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
			iter1.EXPECT().Head(gomock.Any()).Times(1).Return(nil, errors.New("boom"))
			dut := iteratorStream{buffer: iter1}
			require.Error(t, dut.skipToTarget(context.Background(), "2"))
		})
		t.Run("skip_head_error", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
			iter1.EXPECT().Head(gomock.Any()).Times(1).Return(&openfgav1.TupleKey{
				Object: "0",
			}, nil)
			iter1.EXPECT().Next(gomock.Any()).Times(1).Return(&openfgav1.TupleKey{
				Object: "0",
			}, nil)
			iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return(nil, errors.New("boom"))
			dut := iteratorStream{buffer: iter1}
			require.Error(t, dut.skipToTarget(context.Background(), "2"))
		})
		t.Run("allItemsAreSmaller", func(t *testing.T) {
			ctx := context.Background()

			dut := iteratorStream{
				buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
					{
						Object: "0",
					},
					{
						Object: "1",
					},
					{
						Object: "2",
					},
					{
						Object: "3",
					},
				}),
			}
			err := dut.skipToTarget(ctx, "4")
			require.NoError(t, err)
			require.Nil(t, dut.buffer)
		})

		t.Run("skipToMatchingItem", func(t *testing.T) {
			ctx := context.Background()

			dut := iteratorStream{
				buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
					{
						Object: "0",
					},
					{
						Object: "1",
					},
					{
						Object: "2",
					},
					{
						Object: "3",
					},
				}),
			}
			err := dut.skipToTarget(ctx, "2")
			require.NoError(t, err)
			item1, err := dut.buffer.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "2", item1.GetObject())
			item2, err := dut.buffer.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "3", item2.GetObject())
			_, err = dut.buffer.Next(ctx)
			require.ErrorIs(t, err, storage.ErrIteratorDone)
		})

		t.Run("skipToNextItem", func(t *testing.T) {
			ctx := context.Background()

			dut := iteratorStream{
				buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
					{
						Object: "0",
					},
					{
						Object: "1",
					},
					{
						Object: "3",
					},
					{
						Object: "4",
					},
				}),
			}
			err := dut.skipToTarget(ctx, "2")
			require.NoError(t, err)
			item1, err := dut.buffer.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "3", item1.GetObject())
			item2, err := dut.buffer.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "4", item2.GetObject())
			_, err = dut.buffer.Next(ctx)
			require.ErrorIs(t, err, storage.ErrIteratorDone)
		})
	})
}

func TestNextItemInSliceStreams(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("error_on_empty", func(t *testing.T) {
		stream1 := &iteratorStream{buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{})}
		stream2 := &iteratorStream{buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
			{
				Object: "0",
			},
		})}
		item, err := nextItemInSliceStreams(context.Background(), []*iteratorStream{stream1, stream2}, []int{0, 1})
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		require.Nil(t, item)
	})
	t.Run("skip_item", func(t *testing.T) {
		stream1 := &iteratorStream{buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
			{
				Object: "3",
			},
		})}
		stream2 := &iteratorStream{buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
			{
				Object: "0",
			},
			{
				Object: "1",
			},
		})}
		stream3 := &iteratorStream{buffer: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
			{
				Object: "0",
			},
			{
				Object: "2",
			},
		})}
		item, err := nextItemInSliceStreams(context.Background(), []*iteratorStream{stream1, stream2, stream3}, []int{1, 2})
		require.NoError(t, err)
		require.Equal(t, "0", item.GetObject())
		// validate that only stream2 and stream3 are iterated to next item
		stream1Item, err := stream1.buffer.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "3", stream1Item.GetObject())
		stream2Item, err := stream2.buffer.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "1", stream2Item.GetObject())
		stream3Item, err := stream3.buffer.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "2", stream3Item.GetObject())
	})
}

func TestIteratorStreams(t *testing.T) {
	t.Run("getActiveStreams", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		t.Run("should_exit_on_context_cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			stream := iteratorStreams{streams: []*iteratorStream{{done: false}}}
			_, err := stream.getActiveStreams(ctx)
			require.Equal(t, context.Canceled, err)
		})
		t.Run("should_error_out_with_producer_errors", func(t *testing.T) {
			ctx := context.Background()
			c := make(chan *iteratorMsg, 1)
			expectedErr := errors.New("boom")
			c <- &iteratorMsg{err: expectedErr}
			stream := iteratorStreams{streams: []*iteratorStream{{done: true}, {done: false, source: c}}}
			_, err := stream.getActiveStreams(ctx)
			require.Equal(t, expectedErr, err)
		})
		t.Run("should_filter_out_drained_producers", func(t *testing.T) {
			ctx := context.Background()
			streams := make([]*iteratorStream, 0)
			expectedLen := 0
			for i := 0; i < 5; i++ {
				c := make(chan *iteratorMsg, 1)
				producer := &iteratorStream{source: c}
				if i%2 == 0 {
					close(c)
				} else {
					expectedLen++
					c <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(nil)}
				}
				streams = append(streams, producer)
			}
			stream := iteratorStreams{streams: streams}
			res, err := stream.getActiveStreams(ctx)
			require.NoError(t, err)
			require.Len(t, res, expectedLen)
		})
		t.Run("should_return_empty_when_fully_drained", func(t *testing.T) {
			ctx := context.Background()
			streams := make([]*iteratorStream, 0)
			for i := 0; i < 5; i++ {
				producer := &iteratorStream{buffer: nil, done: true}
				streams = append(streams, producer)
			}
			stream := iteratorStreams{streams: streams}
			res, err := stream.getActiveStreams(ctx)
			require.NoError(t, err)
			require.Empty(t, res)
		})
	})
}
