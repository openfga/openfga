package graph

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

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
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger))
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
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger))
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
	t.Run("should_recover_from_panic", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		main := NewMockCheckResolver(ctrl)
		main.EXPECT().Close().MaxTimes(1)
		shadow := NewMockCheckResolver(ctrl)
		shadow.EXPECT().Close().MaxTimes(1)
		logger := mocks.NewMockLogger(ctrl)
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger), ShadowResolverWithTimeout(1*time.Second))
		defer checker.Close()
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: false,
		}, nil)
		shadow.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *ResolveCheckRequest) (*ResolveCheckResponse, error) {
			panic(errors.New("test error"))
		})
		logger.EXPECT().ErrorWithContext(gomock.Any(), "panic recovered", gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), zap.String("function", "ShadowResolver.ResolveCheck"))
		res, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
		require.NoError(t, err)
		require.False(t, res.Allowed)
		checker.wg.Wait()
	})
	t.Run("output_difference", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		main := NewMockCheckResolver(ctrl)
		main.EXPECT().Close().MaxTimes(1)
		shadow := NewMockCheckResolver(ctrl)
		shadow.EXPECT().Close().MaxTimes(1)
		logger := mocks.NewMockLogger(ctrl)
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger), ShadowResolverWithTimeout(1*time.Second))
		defer checker.Close()
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: false,
		}, nil)
		shadow.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: true,
		}, nil)
		logger.EXPECT().InfoWithContext(gomock.Any(), "shadow check difference", gomock.Any())
		res, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
		require.NoError(t, err)
		require.False(t, res.Allowed)
		checker.wg.Wait()
	})
}
