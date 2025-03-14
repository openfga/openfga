package graph

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
)

func TestShadowResolver_ResolveCheck(t *testing.T) {
	t.Run("should_noop_on_error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		main := NewMockCheckResolver(ctrl)
		main.EXPECT().Close().MaxTimes(1)
		shadow := NewMockCheckResolver(ctrl)
		shadow.EXPECT().Close().MaxTimes(1)
		checker := NewShadowChecker(main, shadow)
		defer checker.Close()
		expectedErr := errors.New("test error")
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(nil, expectedErr)
		_, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
		require.ErrorIs(t, expectedErr, err)
	})
	t.Run("should_log_warn_on_error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		main := NewMockCheckResolver(ctrl)
		main.EXPECT().Close().MaxTimes(1)
		shadow := NewMockCheckResolver(ctrl)
		shadow.EXPECT().Close().MaxTimes(1)
		logger := mocks.NewMockLogger(ctrl)
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger), ShadowResolverWithSamplePercentage(100))
		defer checker.Close()
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: false,
		}, nil)
		shadow.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(nil, context.Canceled)
		logger.EXPECT().WarnWithContext(gomock.Any(), "shadow check errored", gomock.Any())
		res, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
		checker.wg.Wait()
		require.NoError(t, err)
		require.False(t, res.Allowed)
	})
	t.Run("should_log_on_difference", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		main := NewMockCheckResolver(ctrl)
		main.EXPECT().Close().MaxTimes(1)
		shadow := NewMockCheckResolver(ctrl)
		shadow.EXPECT().Close().MaxTimes(1)
		logger := mocks.NewMockLogger(ctrl)
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger), ShadowResolverWithSamplePercentage(100))
		defer checker.Close()
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: false,
		}, nil)
		shadow.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{Allowed: true}, nil)
		logger.EXPECT().InfoWithContext(gomock.Any(), "shadow check difference", gomock.Any())
		res, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
		checker.wg.Wait()
		require.NoError(t, err)
		require.False(t, res.Allowed)
	})
	t.Run("should_sample", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		main := NewMockCheckResolver(ctrl)
		main.EXPECT().Close().MaxTimes(1)
		shadow := NewMockCheckResolver(ctrl)
		shadow.EXPECT().Close().MaxTimes(1)
		logger := mocks.NewMockLogger(ctrl)
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger), ShadowResolverWithSamplePercentage(10), ShadowResolverWithTimeout(1*time.Second))
		defer checker.Close()
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).MaxTimes(100).Return(&ResolveCheckResponse{
			Allowed: false,
		}, nil)
		shadow.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).MaxTimes(25).Return(&ResolveCheckResponse{Allowed: true}, nil)
		logger.EXPECT().InfoWithContext(gomock.Any(), "shadow check difference", gomock.Any()).MaxTimes(25)
		for i := 0; i < 100; i++ {
			res, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
			require.NoError(t, err)
			require.False(t, res.Allowed)
		}
		checker.wg.Wait()
	})
	t.Run("should_recover_from_panic", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		main := NewMockCheckResolver(ctrl)
		main.EXPECT().Close().MaxTimes(1)
		shadow := NewMockCheckResolver(ctrl)
		shadow.EXPECT().Close().MaxTimes(1)
		logger := mocks.NewMockLogger(ctrl)
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger), ShadowResolverWithSamplePercentage(100), ShadowResolverWithTimeout(1*time.Second))
		defer checker.Close()
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: false,
		}, nil)
		shadow.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *ResolveCheckRequest) (*ResolveCheckResponse, error) {
			panic(errors.New("test error"))
		})
		logger.EXPECT().ErrorWithContext(gomock.Any(), "shadow check panic", gomock.Any())
		res, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
		require.NoError(t, err)
		require.False(t, res.Allowed)
		checker.wg.Wait()
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: false,
		}, nil)
		shadow.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: true,
		}, nil)
		logger.EXPECT().InfoWithContext(gomock.Any(), "shadow check difference", gomock.Any())
		res, err = checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
		require.NoError(t, err)
		require.False(t, res.Allowed)
		checker.wg.Wait()
	})
}
