package gateway

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/logger"
)

func TestRPCTransport(t *testing.T) {
	logger, logs := logger.NewObserverLogger("error")
	transport := NewRPCTransport(logger)
	transport.SetHeader(context.Background(), "test", "test")
	log := logs.All()[0]

	require.Contains(t, log.Message, "failed to set grpc header")
}
