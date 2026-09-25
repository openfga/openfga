package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type LoggerOption func(d *customLogger)

func WithLogOTLPEndpoint(endpoint string) LoggerOption {
	return func(d *customLogger) {
		d.endpoint = endpoint
	}
}

func WithLogOTLPInsecure() LoggerOption {
	return func(d *customLogger) {
		d.insecure = true
	}
}

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

// probeCollector blocks until a gRPC connection to the collector is ready or
// ctx expires, making startup fail fast on an unreachable collector.
// The otlploggrpc exporter dials lazily and ignores grpc.WithBlock, so the
// connectivity wait cannot be delegated to it.
func probeCollector(ctx context.Context, endpoint string, secure bool) error {
	creds := insecure.NewCredentials()
	if secure {
		creds = credentials.NewTLS(nil)
	}

	// nolint:staticcheck // grpc.DialContext is the only blocking dial API; grpc.NewClient ignores WithBlock.
	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return err
	}

	_ = conn.Close()
	return nil
}

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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	endpoint, schemeSecure := ParseOTLPEndpoint(l.endpoint)
	secure := ResolveOTLPSecurity(!l.insecure, schemeSecure)

	if err := probeCollector(ctx, endpoint, secure); err != nil {
		panic(fmt.Sprintf("failed to connect to OTLP log collector %q: %v", endpoint, err))
	}

	options := []otlploggrpc.Option{
		otlploggrpc.WithEndpoint(endpoint),
	}

	// Pin the exporter's transport credentials to the same security the probe
	// just verified. Passing them explicitly prevents
	// OTEL_EXPORTER_OTLP_LOGS_INSECURE / OTEL_EXPORTER_OTLP_INSECURE from
	// silently downgrading a TLS connection the probe established.
	if secure {
		options = append(options, otlploggrpc.WithTLSCredentials(credentials.NewTLS(nil)))
	} else {
		options = append(options, otlploggrpc.WithInsecure())
	}

	exp, err := otlploggrpc.New(ctx, options...)
	if err != nil {
		panic(fmt.Sprintf("failed to establish a connection with the otlp log exporter: %v", err))
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
	)

	return lp
}
