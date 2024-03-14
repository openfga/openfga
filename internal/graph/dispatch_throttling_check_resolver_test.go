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
		rateLimitedCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			TimerTickerFrequency: 1 * time.Hour,
			LowPriorityLevel:     200,
			LowPriorityShaper:    8,
			MediumPriorityLevel:  100,
			MediumPriorityShaper: 4,
		}
		dut := NewDispatchThrottlingCheckResolver(rateLimitedCheckResolverConfig)
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
		rateLimitedCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			TimerTickerFrequency: 1 * time.Hour,
			LowPriorityLevel:     200,
			LowPriorityShaper:    2,
			MediumPriorityLevel:  100,
			MediumPriorityShaper: 8,
		}
		dut := NewDispatchThrottlingCheckResolver(rateLimitedCheckResolverConfig)
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

		// the first time tick will not release the job
		newCount := dut.handleTimeTick(0)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(0), c.NumResolveCheck())
		require.Equal(t, uint32(1), newCount)

		// the second time tick will release the job
		newCount = dut.handleTimeTick(newCount)
		goFuncDone.Wait()
		require.Equal(t, uint32(1), c.NumResolveCheck())
		require.Equal(t, uint32(0), newCount)
	})

	t.Run("above medium shaping level (just past threshold) should be shaped slightly", func(t *testing.T) {
		rateLimitedCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			TimerTickerFrequency: 1 * time.Hour,
			LowPriorityLevel:     180,
			LowPriorityShaper:    2,
			MediumPriorityLevel:  100,
			MediumPriorityShaper: 8,
		}
		dut := NewDispatchThrottlingCheckResolver(rateLimitedCheckResolverConfig)
		defer dut.Close()

		c := newCountResolveCheck()
		dut.SetDelegate(c)

		dispatchCounter := &atomic.Uint32{}
		dispatchCounter.Store(100)

		var goFuncDone sync.WaitGroup
		goFuncDone.Add(8)

		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(8)

		for i := 0; i < 8; i++ {
			go func() {
				defer goFuncDone.Done()
				goFuncInitiated.Done()
				_, err := dut.ResolveCheck(context.Background(), &ResolveCheckRequest{DispatchCounter: dispatchCounter})

				//nolint:testifylint
				require.NoError(t, err)
			}()
		}

		// 7 of the 8 requests should not be blocked
		goFuncInitiated.Wait()
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(7), c.NumResolveCheck())

		// the time tick will release the job
		newCount := dut.handleTimeTick(0)
		goFuncDone.Wait()
		require.Equal(t, uint32(8), c.NumResolveCheck())
		require.Equal(t, uint32(1), newCount)
	})

	t.Run("above medium shaping level (over one eighth) should be shaped slightly", func(t *testing.T) {
		rateLimitedCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			TimerTickerFrequency: 1 * time.Hour,
			LowPriorityLevel:     180,
			LowPriorityShaper:    2,
			MediumPriorityLevel:  100,
			MediumPriorityShaper: 8,
		}
		dut := NewDispatchThrottlingCheckResolver(rateLimitedCheckResolverConfig)
		defer dut.Close()

		c := newCountResolveCheck()
		dut.SetDelegate(c)

		dispatchCounter := &atomic.Uint32{}
		dispatchCounter.Store(110)

		var goFuncDone sync.WaitGroup
		goFuncDone.Add(8)

		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(8)

		for i := 0; i < 8; i++ {
			go func() {
				defer goFuncDone.Done()
				goFuncInitiated.Done()
				_, err := dut.ResolveCheck(context.Background(), &ResolveCheckRequest{DispatchCounter: dispatchCounter})

				//nolint:testifylint
				require.NoError(t, err)
			}()
		}

		// 6 of the 8 requests should not be blocked
		goFuncInitiated.Wait()
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(6), c.NumResolveCheck())

		// the first tick will release job 1
		newCount := dut.handleTimeTick(0)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(7), c.NumResolveCheck())
		require.Equal(t, uint32(1), newCount)

		newCount = dut.handleTimeTick(newCount)
		goFuncDone.Wait()
		require.Equal(t, uint32(8), c.NumResolveCheck())
		require.Equal(t, uint32(0), newCount)
	})

	t.Run("above medium shaping level (over one quarter) should be shaped more aggressively", func(t *testing.T) {
		rateLimitedCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			TimerTickerFrequency: 1 * time.Hour,
			LowPriorityLevel:     180,
			LowPriorityShaper:    2,
			MediumPriorityLevel:  100,
			MediumPriorityShaper: 8,
		}
		dut := NewDispatchThrottlingCheckResolver(rateLimitedCheckResolverConfig)
		defer dut.Close()

		c := newCountResolveCheck()
		dut.SetDelegate(c)

		dispatchCounter := &atomic.Uint32{}
		dispatchCounter.Store(120)

		var goFuncDone sync.WaitGroup
		goFuncDone.Add(8)

		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(8)

		for i := 0; i < 8; i++ {
			go func() {
				defer goFuncDone.Done()
				goFuncInitiated.Done()
				_, err := dut.ResolveCheck(context.Background(), &ResolveCheckRequest{DispatchCounter: dispatchCounter})

				//nolint:testifylint
				require.NoError(t, err)
			}()
		}

		// 4 of the 8 requests should not be blocked
		goFuncInitiated.Wait()
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(4), c.NumResolveCheck())

		// the first tick will release job 1
		newCount := dut.handleTimeTick(0)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(5), c.NumResolveCheck())
		require.Equal(t, uint32(1), newCount)

		newCount = dut.handleTimeTick(newCount)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(6), c.NumResolveCheck())
		require.Equal(t, uint32(0), newCount)

		newCount = dut.handleTimeTick(newCount)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(7), c.NumResolveCheck())
		require.Equal(t, uint32(1), newCount)

		newCount = dut.handleTimeTick(newCount)
		goFuncDone.Wait()
		require.Equal(t, uint32(8), c.NumResolveCheck())
		require.Equal(t, uint32(0), newCount)
	})

	t.Run("above medium shaping level (over half) should be shaped even more aggressively", func(t *testing.T) {
		rateLimitedCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			TimerTickerFrequency: 1 * time.Hour,
			LowPriorityLevel:     180,
			LowPriorityShaper:    2,
			MediumPriorityLevel:  100,
			MediumPriorityShaper: 8,
		}
		dut := NewDispatchThrottlingCheckResolver(rateLimitedCheckResolverConfig)
		defer dut.Close()

		c := newCountResolveCheck()
		dut.SetDelegate(c)

		dispatchCounter := &atomic.Uint32{}
		dispatchCounter.Store(140)

		var goFuncDone sync.WaitGroup
		goFuncDone.Add(8)

		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(8)

		for i := 0; i < 8; i++ {
			go func() {
				defer goFuncDone.Done()
				goFuncInitiated.Done()
				_, err := dut.ResolveCheck(context.Background(), &ResolveCheckRequest{DispatchCounter: dispatchCounter})

				//nolint:testifylint
				require.NoError(t, err)
			}()
		}

		// every request should be blocked
		goFuncInitiated.Wait()
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(0), c.NumResolveCheck())

		// the first tick will release job 1
		newCount := dut.handleTimeTick(0)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(1), c.NumResolveCheck())
		require.Equal(t, uint32(1), newCount)

		newCount = dut.handleTimeTick(newCount)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(2), c.NumResolveCheck())
		require.Equal(t, uint32(0), newCount)

		newCount = dut.handleTimeTick(newCount)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(3), c.NumResolveCheck())
		require.Equal(t, uint32(1), newCount)

		newCount = dut.handleTimeTick(newCount)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(4), c.NumResolveCheck())
		require.Equal(t, uint32(0), newCount)

		newCount = dut.handleTimeTick(newCount)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(5), c.NumResolveCheck())
		require.Equal(t, uint32(1), newCount)

		newCount = dut.handleTimeTick(newCount)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(6), c.NumResolveCheck())
		require.Equal(t, uint32(0), newCount)

		newCount = dut.handleTimeTick(newCount)
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, uint32(7), c.NumResolveCheck())
		require.Equal(t, uint32(1), newCount)

		newCount = dut.handleTimeTick(newCount)
		goFuncDone.Wait()
		require.Equal(t, uint32(8), c.NumResolveCheck())
		require.Equal(t, uint32(0), newCount)
	})
}
