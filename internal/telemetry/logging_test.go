package telemetry

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

func TestOTLPLogsEnabled(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset_is_disabled", value: "", want: false},
		{name: "none_is_disabled", value: "none", want: false},
		{name: "otlp_is_enabled", value: "otlp", want: true},
		{name: "console_is_enabled", value: "console", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_LOGS_EXPORTER", tt.value)
			require.Equal(t, tt.want, OTLPLogsEnabled())
		})
	}
}

func TestMustNewLoggerProviderUnreachableEndpoint(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	endpoint := ln.Addr().String()
	require.NoError(t, ln.Close())

	t.Setenv("OTEL_LOGS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", endpoint)
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_INSECURE", "true")

	lp := MustNewLoggerProvider(
		context.Background(),
		WithLogAttributes(attribute.String("service.name", "test")),
	)
	require.NotNil(t, lp)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, lp.Shutdown(ctx))
}

func TestMustNewLoggerProviderInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "invalid_exporter",
			env:  map[string]string{"OTEL_LOGS_EXPORTER": "bogus"},
		},
		{
			name: "invalid_protocol",
			env: map[string]string{
				"OTEL_LOGS_EXPORTER":               "otlp",
				"OTEL_EXPORTER_OTLP_LOGS_PROTOCOL": "bogus",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_LOGS_EXPORTER", "")
			t.Setenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			require.Panics(t, func() {
				MustNewLoggerProvider(context.Background())
			})
		})
	}
}
