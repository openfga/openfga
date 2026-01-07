package graph

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/dispatch"
)

func TestDispatchThrottlingCheckResolver(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("dispatch_below_threshold_doesnt_call_throttle", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			DefaultThreshold: 200,
			MaxThreshold:     200,
		}
		mockThrottler := mocks.NewMockThrottler(ctrl)

		dut := NewDispatchThrottlingCheckResolver(
			WithDispatchThrottlingCheckResolverConfig(dispatchThrottlingCheckResolverConfig),
			WithThrottler(mockThrottler),
		)
		t.Cleanup(func() {
			mockThrottler.EXPECT().Close().Times(1)
			dut.Close()
		})

		mockCheckResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(mockCheckResolver)

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(0)

		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata()}
		req.GetRequestMetadata().DispatchCounter.Store(190)

		ctx := context.Background()

		_, err := dut.ResolveCheck(ctx, req)
		require.NoError(t, err)

		require.False(t, req.GetRequestMetadata().DispatchThrottled.Load())
	})

	t.Run("above_threshold_should_call_throttle", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockThrottler := mocks.NewMockThrottler(ctrl)

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			DefaultThreshold: 200,
			MaxThreshold:     200,
		}
		dut := NewDispatchThrottlingCheckResolver(
			WithDispatchThrottlingCheckResolverConfig(dispatchThrottlingCheckResolverConfig),
			WithThrottler(mockThrottler),
		)
		t.Cleanup(func() {
			mockThrottler.EXPECT().Close().Times(1)
			dut.Close()
		})

		mockCheckResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(mockCheckResolver)

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)

		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata()}
		req.GetRequestMetadata().DispatchCounter.Store(201)

		ctx := context.Background()

		_, err := dut.ResolveCheck(ctx, req)
		require.NoError(t, err)

		require.True(t, req.GetRequestMetadata().DispatchThrottled.Load())
	})

	t.Run("zero_max_should_interpret_as_default", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			DefaultThreshold: 200,
			MaxThreshold:     0,
		}
		mockThrottler := mocks.NewMockThrottler(ctrl)

		dut := NewDispatchThrottlingCheckResolver(
			WithDispatchThrottlingCheckResolverConfig(dispatchThrottlingCheckResolverConfig),
			WithThrottler(mockThrottler),
		)
		t.Cleanup(func() {
			mockThrottler.EXPECT().Close().Times(1)
			dut.Close()
		})

		mockCheckResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(mockCheckResolver)

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(0)

		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata()}
		req.GetRequestMetadata().DispatchCounter.Store(190)

		ctx := context.Background()

		_, err := dut.ResolveCheck(ctx, req)
		require.NoError(t, err)

		require.False(t, req.GetRequestMetadata().DispatchThrottled.Load())
	})

	t.Run("dispatch_should_use_request_threshold_if_available", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			DefaultThreshold: 0,
			MaxThreshold:     210,
		}
		mockThrottler := mocks.NewMockThrottler(ctrl)

		dut := NewDispatchThrottlingCheckResolver(
			WithDispatchThrottlingCheckResolverConfig(dispatchThrottlingCheckResolverConfig),
			WithThrottler(mockThrottler),
		)
		t.Cleanup(func() {
			mockThrottler.EXPECT().Close().Times(1)
			dut.Close()
		})

		mockCheckResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(mockCheckResolver)

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)

		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata()}
		req.GetRequestMetadata().DispatchCounter.Store(201)

		ctx := context.Background()
		ctx = dispatch.ContextWithThrottlingThreshold(ctx, 200)

		_, err := dut.ResolveCheck(ctx, req)
		require.NoError(t, err)

		require.True(t, req.GetRequestMetadata().DispatchThrottled.Load())
	})

	t.Run("should_respect_max_threshold", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingCheckResolverConfig := DispatchThrottlingCheckResolverConfig{
			DefaultThreshold: 200,
			MaxThreshold:     300,
		}
		mockThrottler := mocks.NewMockThrottler(ctrl)

		dut := NewDispatchThrottlingCheckResolver(
			WithDispatchThrottlingCheckResolverConfig(dispatchThrottlingCheckResolverConfig),
			WithThrottler(mockThrottler),
		)
		t.Cleanup(func() {
			mockThrottler.EXPECT().Close().Times(1)
			dut.Close()
		})

		mockCheckResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(mockCheckResolver)

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)

		ctx := context.Background()
		ctx = dispatch.ContextWithThrottlingThreshold(ctx, 1000)

		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata()}
		req.GetRequestMetadata().DispatchCounter.Store(301)

		_, err := dut.ResolveCheck(ctx, req)
		require.NoError(t, err)

		require.True(t, req.GetRequestMetadata().DispatchThrottled.Load())
	})
}
