package telemetry

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
)

// startGRPCServer starts a bare gRPC server on a random loopback port and
// returns its address. It is enough for the exporter's lazy dial to connect
// without any registered service.
func startGRPCServer(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(ln)
	}()
	t.Cleanup(srv.Stop)

	return ln.Addr().String()
}

func TestMustNewLoggerProvider(t *testing.T) {
	lp := MustNewLoggerProvider(
		WithLogOTLPEndpoint(startGRPCServer(t)),
		WithLogOTLPInsecure(),
		WithLogAttributes(attribute.String("service.name", "test")),
	)
	require.NotNil(t, lp)

	require.NoError(t, lp.Shutdown(context.Background()))
}

func TestMustNewLoggerProviderUnreachableEndpoint(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	endpoint := ln.Addr().String()
	require.NoError(t, ln.Close())

	lp := MustNewLoggerProvider(
		WithLogOTLPEndpoint(endpoint),
		WithLogOTLPInsecure(),
	)
	require.NotNil(t, lp)

	require.NoError(t, lp.Shutdown(context.Background()))
}
