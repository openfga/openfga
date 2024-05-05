package graph

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/pkg/telemetry"
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
			Frequency:        1 * time.Hour,
			DefaultThreshold: 200,
			MaxThreshold:     200,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		initialMockResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(initialMockResolver)

		// this is to simulate how many times request has been dispatched
		// Since this is a small value, we will not expect throttling
		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata(10)}
		req.GetRequestMetadata().DispatchCounter.Store(190)
		response := &ResolveCheckResponse{Allowed: true, ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: 10,
		}}

		// Here, we count how many times the resolve check is dispatched by the DUT
		resolveCheckDispatchedCounter := 0
		initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).DoAndReturn(func(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
			resolveCheckDispatchedCounter++
			return response, nil
		}).Times(1)

		ctx := context.Background()

		_, err := dut.ResolveCheck(ctx, req)
		require.NoError(t, err)
		require.Equal(t, 1, resolveCheckDispatchedCounter)
		require.False(t, req.GetRequestMetadata().WasThrottled.Load())
	})

	t.Run("zero_max_should_interpret_as_default", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			// We set timer ticker to 1 hour to avoid it interfering with test
			Frequency:        1 * time.Hour,
			DefaultThreshold: 200,
			MaxThreshold:     0,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		initialMockResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(initialMockResolver)

		// this is to simulate how many times request has been dispatched
		// Since this is a small value, we will not expect throttling
		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata(10)}
		req.GetRequestMetadata().DispatchCounter.Store(190)
		response := &ResolveCheckResponse{Allowed: true, ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: 10,
		}}

		// Here, we count how many times the resolve check is dispatched by the DUT
		resolveCheckDispatchedCounter := 0
		initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).DoAndReturn(func(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
			resolveCheckDispatchedCounter++
			return response, nil
		}).Times(1)

		ctx := context.Background()

		_, err := dut.ResolveCheck(ctx, req)
		require.NoError(t, err)
		require.Equal(t, 1, resolveCheckDispatchedCounter)
		require.False(t, req.GetRequestMetadata().WasThrottled.Load())
	})

	t.Run("dispatch_should_use_request_threshold_if_available", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			// We set timer ticker to 1 hour to avoid it interfering with test
			Frequency:        1 * time.Hour,
			DefaultThreshold: 200,
			MaxThreshold:     210,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		initialMockResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(initialMockResolver)

		// this is to simulate how many times request has been dispatched
		// Since this is a small value, we will not expect throttling
		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata(10)}
		req.GetRequestMetadata().DispatchCounter.Store(203)
		response := &ResolveCheckResponse{Allowed: true, ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: 202,
		}}

		// Here, we count how many times the resolve check is dispatched by the DUT
		resolveCheckDispatchedCounter := 0
		initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).DoAndReturn(func(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
			resolveCheckDispatchedCounter++
			return response, nil
		}).Times(1)

		ctx := context.Background()
		ctx = telemetry.ContextWithDispatchThrottlingThreshold(ctx, uint32(205))

		_, err := dut.ResolveCheck(ctx, req)
		require.NoError(t, err)
		require.Equal(t, 1, resolveCheckDispatchedCounter)
		require.False(t, req.GetRequestMetadata().WasThrottled.Load())
	})

	t.Run("above_threshold_should_be_throttled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			Frequency:        1 * time.Microsecond,
			DefaultThreshold: 200,
			MaxThreshold:     200,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		initialMockResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(initialMockResolver)

		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata(10)}
		req.GetRequestMetadata().DispatchCounter.Store(201)
		response := &ResolveCheckResponse{Allowed: true, ResolutionMetadata: &ResolveCheckResponseMetadata{
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

		ctx := context.Background()

		var resolveCheckErr error

		go func() {
			defer goFuncDone.Done()
			_, resolveCheckErr = dut.ResolveCheck(ctx, req)
		}()

		goFuncDone.Wait()
		require.Equal(t, 1, resolveCheckDispatchedCounter)
		require.True(t, req.GetRequestMetadata().WasThrottled.Load())
		require.NoError(t, resolveCheckErr)
	})

	t.Run("should_respect_max_threshold", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			Frequency:        1 * time.Microsecond,
			DefaultThreshold: 200,
			MaxThreshold:     0,
		}
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottlingCheckResolverConfig)
		defer dut.Close()

		initialMockResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(initialMockResolver)

		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata(10)}
		req.GetRequestMetadata().DispatchCounter.Store(201)
		response := &ResolveCheckResponse{Allowed: true, ResolutionMetadata: &ResolveCheckResponseMetadata{
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

		ctx := context.Background()
		ctx = telemetry.ContextWithDispatchThrottlingThreshold(ctx, uint32(205))

		var resolveCheckErr error

		go func() {
			defer goFuncDone.Done()
			_, resolveCheckErr = dut.ResolveCheck(ctx, req)
		}()

		goFuncDone.Wait()
		require.Equal(t, 1, resolveCheckDispatchedCounter)
		require.True(t, req.GetRequestMetadata().WasThrottled.Load())
		require.NoError(t, resolveCheckErr)
	})
}
