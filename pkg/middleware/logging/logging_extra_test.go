package logging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/metadata"

	"github.com/openfga/openfga/pkg/logger"
)

func TestNewStreamingLoggingInterceptor(t *testing.T) {
	require.NotNil(t, NewStreamingLoggingInterceptor(logger.NewNoopLogger()))
	require.NotNil(t, NewLoggingInterceptor(logger.NewNoopLogger()))
}

func TestUserAgentFromContext(t *testing.T) {
	t.Run("gateway_user_agent_header", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(gatewayUserAgentHeader, "grpc-gateway/1.0"))
		ua, ok := userAgentFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, "grpc-gateway/1.0", ua)
	})

	t.Run("user_agent_header", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs(userAgentHeader, "grpc-go/1.0"))
		ua, ok := userAgentFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, "grpc-go/1.0", ua)
	})

	t.Run("no_user_agent", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
		_, ok := userAgentFromContext(ctx)
		require.False(t, ok)
	})

	t.Run("no_metadata", func(t *testing.T) {
		_, ok := userAgentFromContext(context.Background())
		require.False(t, ok)
	})
}

func TestReporter_PostCall(t *testing.T) {
	newReporter := func(core zapcore.Core, service string) *reporter {
		return &reporter{
			ctx:         context.Background(),
			logger:      &logger.ZapLogger{Logger: zap.New(core)},
			serviceName: service,
		}
	}

	t.Run("success_logs_info", func(t *testing.T) {
		core, logs := observer.New(zap.DebugLevel)
		r := newReporter(core, "openfga.v1.OpenFGAService")
		r.PostCall(nil, 5*time.Millisecond)

		entries := logs.FilterMessage(grpcReqCompleteKey).All()
		require.Len(t, entries, 1)
		require.Equal(t, zapcore.InfoLevel, entries[0].Level)
	})

	t.Run("health_service_logs_debug", func(t *testing.T) {
		core, logs := observer.New(zap.DebugLevel)
		r := newReporter(core, healthCheckService)
		r.PostCall(nil, time.Millisecond)

		entries := logs.FilterMessage(grpcReqCompleteKey).All()
		require.Len(t, entries, 1)
		require.Equal(t, zapcore.DebugLevel, entries[0].Level)
	})

	t.Run("non_internal_error_logs_info_with_error", func(t *testing.T) {
		core, logs := observer.New(zap.DebugLevel)
		r := newReporter(core, "openfga.v1.OpenFGAService")
		r.PostCall(errors.New("boom"), time.Millisecond)

		entries := logs.FilterMessage(grpcReqCompleteKey).All()
		require.Len(t, entries, 1)
		require.Equal(t, zapcore.InfoLevel, entries[0].Level)
	})
}
