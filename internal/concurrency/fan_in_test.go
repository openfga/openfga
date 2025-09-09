package concurrency

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func makeIterChan(ctrl *gomock.Controller, id string) chan *iterator.Msg {
	iterChan := make(chan *iterator.Msg, 1)
	iter := mocks.NewMockIterator[string](ctrl)
	iter.EXPECT().Next(gomock.Any()).MaxTimes(1).Return(id, nil)
	iter.EXPECT().Next(gomock.Any()).MaxTimes(1).Return("", storage.ErrIteratorDone)
	iter.EXPECT().Stop().MinTimes(1)
	iterChan <- &iterator.Msg{Iter: iter}
	close(iterChan)
	return iterChan
}

func TestFanChannels(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	chans := make([]<-chan *iterator.Msg, 0, 9)
	chans = append(chans,
		makeIterChan(ctrl, "1"),
		makeIterChan(ctrl, "2"),
		makeIterChan(ctrl, "3"),
		makeIterChan(ctrl, "4"),
		makeIterChan(ctrl, "5"),
		makeIterChan(ctrl, "6"),
		makeIterChan(ctrl, "7"),
		makeIterChan(ctrl, "8"),
		makeIterChan(ctrl, "9"))

	out := FanInChannels[*iterator.Msg](ctx, chans, iterator.CleanMsg)

	iterations := 0
	for msg := range out {
		id, err := msg.Iter.Next(ctx)
		require.NoError(t, err)
		i, err := strconv.Atoi(id)
		require.NoError(t, err)
		require.Positive(t, i)
		require.LessOrEqual(t, i, 9)
		_, err = msg.Iter.Next(ctx)
		require.Equal(t, storage.ErrIteratorDone, err)
		msg.Iter.Stop()
		iterations++
	}
	require.Equal(t, 9, iterations)
	cancellable, cancel := context.WithCancel(ctx)
	cancel() // Stop would still be called in all entries even tho its been cancelled
	chans = make([]<-chan *iterator.Msg, 0, 5)
	chans = append(chans,
		makeIterChan(ctrl, "1"),
		makeIterChan(ctrl, "2"),
		makeIterChan(ctrl, "3"),
		makeIterChan(ctrl, "4"),
		makeIterChan(ctrl, "5"),
	)

	out = FanInChannels[*iterator.Msg](cancellable, chans, func(msg *iterator.Msg) {
		if msg.Iter != nil {
			msg.Iter.Stop()
		}
	})
	iterations = 0
	for msg := range out {
		id, err := msg.Iter.Next(ctx)
		require.NoError(t, err)
		i, err := strconv.Atoi(id)
		require.NoError(t, err)
		require.Positive(t, i)
		require.LessOrEqual(t, i, 5)
		_, err = msg.Iter.Next(ctx)
		require.Equal(t, storage.ErrIteratorDone, err)
		msg.Iter.Stop()
		iterations++
	}
	require.LessOrEqual(t, iterations, 5)
}
