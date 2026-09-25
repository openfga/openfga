package telemetry

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

func TestMustNewLoggerProviderUnreachableEndpoint(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	endpoint := ln.Addr().String()
	require.NoError(t, ln.Close())

	lp := MustNewLoggerProvider(
		WithLogOTLPEndpoint(endpoint),
		WithLogOTLPInsecure(),
		WithLogAttributes(attribute.String("service.name", "test")),
	)
	require.NotNil(t, lp)

	require.NoError(t, lp.Shutdown(context.Background()))
}
