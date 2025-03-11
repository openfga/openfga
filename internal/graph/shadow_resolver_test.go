package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
)

func TestShadowResolver_ResolveCheck(t *testing.T) {
	t.Run("should_noop_on_error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		main := NewMockCheckResolver(ctrl)
		checker := NewShadowChecker(main, NewMockCheckResolver(ctrl))
		expectedErr := errors.New("test error")
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(nil, expectedErr)
		_, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
		require.ErrorIs(t, expectedErr, err)
	})
	t.Run("should_log_warn_on_error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		main := NewMockCheckResolver(ctrl)
		shadow := NewMockCheckResolver(ctrl)
		logger := mocks.NewMockLogger(ctrl)
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger))
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: ResolveCheckResponseMetadata{
				DatastoreQueryCount: 5,
			},
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
		main := NewMockCheckResolver(ctrl)
		shadow := NewMockCheckResolver(ctrl)
		logger := mocks.NewMockLogger(ctrl)
		checker := NewShadowChecker(main, shadow, ShadowResolverWithLogger(logger))
		main.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: ResolveCheckResponseMetadata{
				DatastoreQueryCount: 5,
			},
		}, nil)
		shadow.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{Allowed: true}, nil)
		logger.EXPECT().InfoWithContext(gomock.Any(), "shadow check difference", gomock.Any())
		res, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{})
		checker.wg.Wait()
		require.NoError(t, err)
		require.False(t, res.Allowed)
	})
}
