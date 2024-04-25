package graph

import (
	"context"
	"github.com/openfga/openfga/internal/throttler"
	"sync"
	"testing"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
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

		dispatchThrottlingConfig := throttler.DispatchThrottlingConfig{
			// We set timer ticker to 1 hour to avoid it interfering with test
			Frequency: 1 * time.Hour,
			Threshold: 200,
		}
		dispatchThrottler := throttler.NewDispatchThrottler(dispatchThrottlingConfig)
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottler)
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
		require.False(t, grpc_ctxtags.Extract(ctx).Has(telemetry.Throttled))
	})

	t.Run("above_threshold_should_be_throttled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dispatchThrottlingConfig := throttler.DispatchThrottlingConfig{
			// We set timer ticker to 1 hour to avoid it interfering with test
			Frequency: 1 * time.Second,
			Threshold: 200,
		}
		dispatchThrottler := throttler.NewDispatchThrottler(dispatchThrottlingConfig)
		dut := NewDispatchThrottlingCheckResolver(dispatchThrottler)
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

		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(1)

		ctx := context.Background()

		go func() {
			defer goFuncDone.Done()
			goFuncInitiated.Done()
			ctx = grpc_ctxtags.SetInContext(ctx, grpc_ctxtags.NewTags())
			_, err := dut.ResolveCheck(ctx, req)
			//nolint:testifylint
			require.NoError(t, err)
		}()

		goFuncInitiated.Wait()
		// Before we send the tick, we don't expect the dispatch to run
		time.Sleep(1 * time.Millisecond)
		require.Equal(t, 0, resolveCheckDispatchedCounter)

		// simulate tick happening and we release a dispatch
		dispatchThrottler.ReleaseDispatch()
		goFuncDone.Wait()
		require.Equal(t, 1, resolveCheckDispatchedCounter)
		require.True(t, grpc_ctxtags.Extract(ctx).Has(telemetry.Throttled))
	})
}
