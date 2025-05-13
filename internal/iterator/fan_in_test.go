package iterator

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

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

	logger := mocks.NewMockLogger(ctrl)
	fanin := NewFanIn(ctx, logger, 2)

	fanin.Add(makeIterChan(ctrl, "1", true))
	fanin.Add(makeIterChan(ctrl, "2", true))
	fanin.Add(makeIterChan(ctrl, "3", true))
	fanin.Add(makeIterChan(ctrl, "4", true))
	fanin.Add(makeIterChan(ctrl, "5", true))
	fanin.Add(makeIterChan(ctrl, "6", true))
	fanin.Add(makeIterChan(ctrl, "7", true))
	fanin.Done()
	fanin.Add(makeIterChan(ctrl, "8", false))
	fanin.Add(makeIterChan(ctrl, "9", false))

	require.Equal(t, 9, fanin.Count())

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
	fanin.Close()
	drained := <-fanin.drained
	require.True(t, drained)

	t.Run("should_log_when_run_panics", func(t *testing.T) {
		logger.EXPECT().ErrorWithContext(gomock.Any(), "panic recoverred", gomock.Any(), zap.String("function", "FanIn.handleChannel"))
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		fanin := NewFanIn(ctx, logger, 2)
		iterChan := make(chan *Msg, 1)
		iter := mocks.NewMockIterator[string](ctrl)
		iter.EXPECT().Stop().DoAndReturn(func() {
			panic("test panic")
		})
		iterChan <- &Msg{Iter: iter}
		close(iterChan)
		fanin.Add(iterChan)
		time.Sleep(2 * time.Second)
		fanin.Done()
	})
}

func TestHandleChannel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	logger := mocks.NewMockLogger(ctrl)

	t.Run("should_log_when_pool_panics", func(t *testing.T) {
		logger.EXPECT().ErrorWithContext(gomock.Any(), "panic recoverred", gomock.Any(), zap.String("function", "FanIn.handleChannel"))
		fanin := NewFanIn(ctx, logger, 2)
		iterChan := make(chan *Msg, 3)
		iter := mocks.NewMockIterator[string](ctrl)
		iter.EXPECT().Stop().DoAndReturn(func() {
			panic("test panic")
		})
		// iterChan <- nil
		iterChan <- &Msg{Iter: nil, Err: fmt.Errorf("test error")}
		iterChan <- &Msg{Iter: iter}
		iterChan <- &Msg{Iter: iter}
		// iterChan <- &Msg{Iter: iter, Err: fmt.Errorf("test error")}
		// go func() {
		// 	fanin.handleChannel(iterChan)
		// }()
		// iterChan <- &Msg{Iter: iter}
		close(iterChan)
		// fanin.Add(iterChan)
		time.Sleep(2 * time.Second)
		fanin.Done()
	})
}
