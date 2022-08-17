package gateway

import (
	"context"
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
	if len(logs.All()) > 0 {
		t.Fatalf("expected no errors, got %v", logs.All())
	}
}
