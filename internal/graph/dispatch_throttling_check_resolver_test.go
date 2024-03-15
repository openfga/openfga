package graph

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type countResolveCheck struct {
	count *atomic.Uint32
}

func newCountResolveCheck() *countResolveCheck {
	return &countResolveCheck{
		count: &atomic.Uint32{},
	}
}

func (c *countResolveCheck) ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
	c.count.Add(1)
	return &ResolveCheckResponse{Allowed: true}, nil
}

func (c *countResolveCheck) Close() {}

func (c *countResolveCheck) NumResolveCheck() uint32 {
	return c.count.Load()
}

func TestDispatchThrottlingCheckResolver(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("below_shaping_level_should_not_be_shaped", func(t *testing.T) {
		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			TimerTickerFrequency: 1 * time.Hour,
			Level:                200,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		c := newCountResolveCheck()
		dut.SetDelegate(c)

		dispatchCounter := &atomic.Uint32{}
		_, err := dut.ResolveCheck(context.Background(), &ResolveCheckRequest{DispatchCounter: dispatchCounter})
		require.NoError(t, err)
		require.Equal(t, uint32(1), c.NumResolveCheck())
		require.Equal(t, uint32(1), dispatchCounter.Load())
	})

	t.Run("above low shaping level should be shaped very aggressively", func(t *testing.T) {
		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			TimerTickerFrequency: 1 * time.Hour,
			Level:                200,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		c := newCountResolveCheck()
		dut.SetDelegate(c)

		dispatchCounter := &atomic.Uint32{}
		dispatchCounter.Store(201)

		var goFuncDone sync.WaitGroup
		goFuncDone.Add(1)

		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(1)

		go func() {
			defer goFuncDone.Done()
			goFuncInitiated.Done()
			_, err := dut.ResolveCheck(context.Background(), &ResolveCheckRequest{DispatchCounter: dispatchCounter})
			//nolint:testifylint
			require.NoError(t, err)
			//nolint:testifylint
			require.Equal(t, uint32(202), dispatchCounter.Load())
		}()

		goFuncInitiated.Wait()
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(0), c.NumResolveCheck())

		// simulate sending of tick
		dut.nonBlockingSend(dut.throttlingQueue)
		goFuncDone.Wait()
		require.Equal(t, uint32(1), c.NumResolveCheck())
	})
}
