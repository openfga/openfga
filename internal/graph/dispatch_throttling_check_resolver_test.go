package graph

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
)

func TestDispatchThrottlingCheckResolver(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("dispatch_below_threshold_resolves_immediately", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			// We set timer ticker to 1 hour to avoid it interfering with test
			TimerTickerFrequency: 1 * time.Hour,
			Level:                200,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		initialMockResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(initialMockResolver)

		// this is to simulate how many times request has been dispatched
		// Since this is a small value, we will not expect throttling
		reqDispatchCounter := &atomic.Uint32{}
		req := &ResolveCheckRequest{DispatchCounter: reqDispatchCounter}
		response := &ResolveCheckResponse{Allowed: true, ResolutionMetadata: &ResolutionMetadata{
			Depth:               3,
			DatastoreQueryCount: 10,
		}}

		// Here, we count how many times the resolve check is dispatched by the DUT
		resolveCheckDispatchedCounter := 0
		initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).DoAndReturn(func(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
			resolveCheckDispatchedCounter++
			return response, nil
		}).Times(1)

		_, err := dut.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, 1, resolveCheckDispatchedCounter)
		require.Equal(t, uint32(1), reqDispatchCounter.Load())
	})

	t.Run("above_threshold_should_be_throttled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			// We set timer ticker to 1 hour to avoid it interfering with test
			TimerTickerFrequency: 1 * time.Hour,
			Level:                200,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		initialMockResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(initialMockResolver)

		// this is to simulate how many times request has been dispatched
		// Since this is a large value, we expect throttling
		reqDispatchCounter := &atomic.Uint32{}
		reqDispatchCounter.Store(201)

		req := &ResolveCheckRequest{DispatchCounter: reqDispatchCounter}
		response := &ResolveCheckResponse{Allowed: true, ResolutionMetadata: &ResolutionMetadata{
			Depth:               3,
			DatastoreQueryCount: 10,
		}}

		// Here, we count how many times the resolve check is dispatched by the DUT
		resolveCheckDispatchedCounter := 0

		var goFuncDone sync.WaitGroup
		goFuncDone.Add(1)

		initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).DoAndReturn(func(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
			resolveCheckDispatchedCounter++
			return response, nil
		}).Times(1)

		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(1)

		go func() {
			defer goFuncDone.Done()
			goFuncInitiated.Done()
			_, err := dut.ResolveCheck(context.Background(), &ResolveCheckRequest{DispatchCounter: reqDispatchCounter})
			//nolint:testifylint
			require.NoError(t, err)
			//nolint:testifylint
			require.Equal(t, uint32(202), reqDispatchCounter.Load())
		}()

		goFuncInitiated.Wait()
		// Before we send the tick, we don't expect the dispatch to run
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, 0, resolveCheckDispatchedCounter)

		// simulate tick happening and we release a dispatch
		dut.nonBlockingSend(dut.throttlingQueue)
		goFuncDone.Wait()
		require.Equal(t, 1, resolveCheckDispatchedCounter)
	})
}
