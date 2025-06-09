package iterator

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func makeIterChan(ctrl *gomock.Controller, id string, next bool) chan *Msg {
	iterChan := make(chan *Msg, 1)
	iter := mocks.NewMockIterator[string](ctrl)
	if next {
		iter.EXPECT().Next(gomock.Any()).MaxTimes(1).Return(id, nil)
		iter.EXPECT().Next(gomock.Any()).MaxTimes(1).Return("", storage.ErrIteratorDone)
	}
	iter.EXPECT().Stop().MinTimes(1)
	iterChan <- &Msg{Iter: iter}
	close(iterChan)
	return iterChan
}

func TestFanIn(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	fanin := NewFanIn(ctx, 5)

	fanin.Add(makeIterChan(ctrl, "1", true))
	fanin.Add(makeIterChan(ctrl, "2", true))
	fanin.Add(makeIterChan(ctrl, "3", true))
	fanin.Add(makeIterChan(ctrl, "4", true))
	fanin.Add(makeIterChan(ctrl, "5", true))
	fanin.Add(makeIterChan(ctrl, "6", true))
	fanin.Done()
	iterChan1 := makeIterChan(ctrl, "8", false)
	accepted := fanin.Add(iterChan1)
	require.False(t, accepted)
	Drain(iterChan1)
	iterChan2 := makeIterChan(ctrl, "9", false)
	accepted = fanin.Add(iterChan2)
	require.False(t, accepted)
	Drain(iterChan2)

	out := fanin.Out()
	iterations := 0
	for msg := range out {
		id, err := msg.Iter.Next(ctx)
		require.NoError(t, err)
		i, err := strconv.Atoi(id)
		require.NoError(t, err)
		require.Positive(t, i)
		require.LessOrEqual(t, i, 7)
		_, err = msg.Iter.Next(ctx)
		require.Equal(t, storage.ErrIteratorDone, err)
		msg.Iter.Stop()
		iterations++
		if iterations > 2 {
			break
		}
	}
	fanin.Stop()
	fanin.wg.Wait()
	Drain(fanin.Out()).Wait()
}

func TestFanInIteratorChannels(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctx := context.Background()
	cancellable, cancel := context.WithCancel(ctx)
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	chans := make([]chan *Msg, 0, 9)
	chans = append(chans,
		makeIterChan(ctrl, "1", true),
		makeIterChan(ctrl, "2", true),
		makeIterChan(ctrl, "3", true),
		makeIterChan(ctrl, "4", true),
		makeIterChan(ctrl, "5", true),
		makeIterChan(ctrl, "6", true),
		makeIterChan(ctrl, "7", true),
		makeIterChan(ctrl, "8", true),
		makeIterChan(ctrl, "9", true))

	out := FanInIteratorChannels(cancellable, chans)
	iterations := 0
	for msg := range out {
		id, err := msg.Iter.Next(ctx)
		require.NoError(t, err)
		i, err := strconv.Atoi(id)
		require.NoError(t, err)
		require.Positive(t, i)
		fmt.Println(i)
		require.LessOrEqual(t, i, 9)
		_, err = msg.Iter.Next(ctx)
		require.Equal(t, storage.ErrIteratorDone, err)
		msg.Iter.Stop()
		iterations++
		if iterations > 2 {
			cancel()
		}
	}
}
