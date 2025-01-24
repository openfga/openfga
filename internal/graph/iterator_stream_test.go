package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/pkg/storage"
)

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
