package throttler

import (
	"context"
	"github.com/openfga/openfga/internal/graph"
	"sync"
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
			Frequency: 1 * time.Hour,
			Threshold: 200,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		initialMockResolver := graph.NewMockCheckResolver(ctrl)
		dut.SetDelegate(initialMockResolver)

		// this is to simulate how many times request has been dispatched
		// Since this is a small value, we will not expect throttling
		req := &graph.ResolveCheckRequest{RequestMetadata: graph.NewCheckRequestMetadata(10)}
		req.GetRequestMetadata().DispatchCounter.Store(190)
		response := &graph.ResolveCheckResponse{Allowed: true, ResolutionMetadata: &graph.ResolveCheckResponseMetadata{
			DatastoreQueryCount: 10,
		}}

		// Here, we count how many times the resolve check is dispatched by the DUT
		resolveCheckDispatchedCounter := 0
		initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
			resolveCheckDispatchedCounter++
			return response, nil
		}).Times(1)

		_, err := dut.ResolveCheck(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, 1, resolveCheckDispatchedCounter)
	})

	t.Run("above_threshold_should_be_throttled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			// We set timer ticker to 1 hour to avoid it interfering with test
			Frequency: 1 * time.Hour,
			Threshold: 200,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		initialMockResolver := graph.NewMockCheckResolver(ctrl)
		dut.SetDelegate(initialMockResolver)

		req := &graph.ResolveCheckRequest{RequestMetadata: graph.NewCheckRequestMetadata(10)}
		req.GetRequestMetadata().DispatchCounter.Store(201)
		response := &graph.ResolveCheckResponse{Allowed: true, ResolutionMetadata: &graph.ResolveCheckResponseMetadata{
			DatastoreQueryCount: 10,
		}}

		// Here, we count how many times the resolve check is dispatched by the DUT
		resolveCheckDispatchedCounter := 0

		var goFuncDone sync.WaitGroup
		goFuncDone.Add(1)

		initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
			resolveCheckDispatchedCounter++
			return response, nil
		}).Times(1)

		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(1)

		go func() {
			defer goFuncDone.Done()
			goFuncInitiated.Done()
			_, err := dut.ResolveCheck(context.Background(), req)
			//nolint:testifylint
			require.NoError(t, err)
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
