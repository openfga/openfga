package gateway

import (
	"context"
	"strings"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRPCTransport(t *testing.T) {
	observerLogger, logs := observer.New(zap.ErrorLevel)
	logger := logger.ZapLogger{Logger: zap.New(observerLogger)}
	transport := NewRPCTransport(&logger)
	transport.SetHeader(context.Background(), "test", "test")
	log := logs.All()[0]
	if !strings.Contains(log.Message, "failed to set grpc header") {
		t.Fatalf("expected to fail setting the header, got %v", log)
	}
}
