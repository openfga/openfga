package telemetry

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestMustNewLoggerProvider(t *testing.T) {
	// Verify the provider can be created with a valid (but unreachable)
	// endpoint and insecure mode. The gRPC connection is lazy, so this
	// should not block or fail.
	lp := MustNewLoggerProvider(
		WithLogOTLPEndpoint("localhost:4317"),
		WithLogOTLPInsecure(),
		WithLogAttributes(attribute.String("service.name", "test")),
	)
	if lp == nil {
		t.Fatal("expected non-nil LoggerProvider")
	}
	// Shutdown immediately to clean up resources.
	if err := lp.Shutdown(t.Context()); err != nil {
		t.Fatalf("unexpected error during shutdown: %v", err)
	}
}
