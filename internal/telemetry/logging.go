package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/grpc/credentials"
)

// LoggerOption configures the OTLP log logger provider built by [MustNewLoggerProvider].
type LoggerOption func(d *customLogger)

// WithLogOTLPEndpoint sets the OTLP collector endpoint for log export.
func WithLogOTLPEndpoint(endpoint string) LoggerOption {
	return func(d *customLogger) {
		d.endpoint = endpoint
	}
}

// WithLogOTLPInsecure disables TLS for the OTLP log export connection.
func WithLogOTLPInsecure() LoggerOption {
	return func(d *customLogger) {
		d.insecure = true
	}
}

// WithLogAttributes sets the resource attributes attached to emitted logs.
func WithLogAttributes(attrs ...attribute.KeyValue) LoggerOption {
	return func(d *customLogger) {
		d.attributes = attrs
	}
}

type customLogger struct {
	endpoint   string
	insecure   bool
	attributes []attribute.KeyValue
}

// MustNewLoggerProvider builds an OTLP log provider from opts, panicking if the
// resource cannot be built. The exporter dials lazily, so an unreachable
// collector does not panic here.
func MustNewLoggerProvider(opts ...LoggerOption) *sdklog.LoggerProvider {
	l := &customLogger{
		attributes: []attribute.KeyValue{},
	}

	for _, opt := range opts {
		opt(l)
	}

	baseRes, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(l.attributes...))
	if err != nil {
		panic(err)
	}

	res, err := resource.Merge(baseRes, resource.Environment())
	if err != nil {
		panic(err)
	}

	endpoint, schemeSecure := ParseOTLPEndpoint(l.endpoint)
	secure := ResolveOTLPSecurity(!l.insecure, schemeSecure)

	options := []otlploggrpc.Option{
		otlploggrpc.WithEndpoint(endpoint),
	}

	// Explicit creds keep OpenFGA's resolved security authoritative over the OTel insecure env vars.
	if secure {
		options = append(options, otlploggrpc.WithTLSCredentials(credentials.NewTLS(nil)))
	} else {
		options = append(options, otlploggrpc.WithInsecure())
	}

	exp, err := otlploggrpc.New(context.Background(), options...)
	if err != nil {
		panic(fmt.Sprintf("failed to establish a connection with the otlp log exporter: %v", err))
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
	)

	return lp
}
