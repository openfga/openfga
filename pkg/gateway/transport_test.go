package gateway

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/openfga/openfga/pkg/logger"
)

func TestRPCTransport(t *testing.T) {
	observerLogger, logs := observer.New(zap.ErrorLevel)
	logger := logger.ZapLogger{Logger: zap.New(observerLogger)}
	transport := NewRPCTransport(&logger)
	transport.SetHeader(context.Background(), "test", "test")
	log := logs.All()[0]

	require.Contains(t, log.Message, "failed to set grpc header")
}
