package graph

import (
	"context"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestDispatchThrottlingCheckResolver(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("dispatch_below_threshold_doesnt_call_throttle", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockThrottler := mockstorage.NewMockThrottler(ctrl)

		dut := NewDispatchThrottlingCheckResolver(200, mockThrottler)

		mockCheckResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(mockCheckResolver)

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(0)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(0)

		// this is to simulate how many times request has been dispatched
		// Since this is a small value, we will not expect throttling
		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata(10)}
		req.GetRequestMetadata().DispatchCounter.Store(190)

		ctx := context.Background()

		_, err := dut.ResolveCheck(ctx, req)

		require.NoError(t, err)
	})

	t.Run("above_threshold_should_call_throttle", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockThrottler := mockstorage.NewMockThrottler(ctrl)

		dut := NewDispatchThrottlingCheckResolver(200, mockThrottler)

		mockCheckResolver := NewMockCheckResolver(ctrl)
		dut.SetDelegate(mockCheckResolver)

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)

		// this is to simulate how many times request has been dispatched
		// Since this is a small value, we will not expect throttling
		req := &ResolveCheckRequest{RequestMetadata: NewCheckRequestMetadata(10)}
		req.GetRequestMetadata().DispatchCounter.Store(201)

		ctx := context.Background()

		_, err := dut.ResolveCheck(ctx, req)

		require.NoError(t, err)
	})
}
