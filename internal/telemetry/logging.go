package telemetry

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

// LoggerOption configures the OTLP log logger provider built by [MustNewLoggerProvider].
type LoggerOption func(d *customLogger)

// WithLogAttributes sets the resource attributes attached to emitted logs.
func WithLogAttributes(attrs ...attribute.KeyValue) LoggerOption {
	return func(d *customLogger) {
		d.attributes = attrs
	}
}

type customLogger struct {
	attributes []attribute.KeyValue
}

// OTLPLogsEnabled reports whether OTLP log export is configured via the
// standard OTEL_LOGS_EXPORTER environment variable.
//
// Deviation from the OpenTelemetry specification: the spec defaults
// OTEL_LOGS_EXPORTER to "otlp", which would make OpenFGA export logs by
// default and change behavior on upgrade. OpenFGA deliberately treats an
// unset (or empty) value as disabled; "none" is also disabled.
//
// "console" is accepted for standard-OTel parity, but it duplicates
// OpenFGA's own stdout sink (every log is written to stdout twice), so it is
// only useful for debugging.
func OTLPLogsEnabled() bool {
	exporter := os.Getenv("OTEL_LOGS_EXPORTER")
	return exporter != "" && exporter != "none"
}

// MustNewLoggerProvider builds an OTLP log provider from opts, panicking if the
// resource or exporter cannot be built. The exporter is selected by the
// standard OTEL_LOGS_EXPORTER environment variable and dials lazily, so an
// unreachable collector does not panic here.
func MustNewLoggerProvider(ctx context.Context, opts ...LoggerOption) *sdklog.LoggerProvider {
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

	exp, err := autoexport.NewLogExporter(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to build the otlp log exporter: %v", err))
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
	)

	return lp
}
