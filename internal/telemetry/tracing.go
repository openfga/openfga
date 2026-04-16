// Package telemetry contains code that emits telemetry (logging, metrics, tracing).
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

type TracerOption func(d *customTracer)

func WithOTLPEndpoint(endpoint string) TracerOption {
	return func(d *customTracer) {
		d.endpoint = endpoint
	}
}

func WithOTLPInsecure() TracerOption {
	return func(d *customTracer) {
		d.insecure = true
	}
}

func WithSampler(sampler string) TracerOption {
	return func(d *customTracer) {
		d.sampler = sampler
	}
}

func WithSamplingRatio(samplingRatio float64) TracerOption {
	return func(d *customTracer) {
		d.samplingRatio = samplingRatio
	}
}

func WithAttributes(attrs ...attribute.KeyValue) TracerOption {
	return func(d *customTracer) {
		d.attributes = attrs
	}
}

type customTracer struct {
	endpoint   string
	insecure   bool
	attributes []attribute.KeyValue

	sampler       string
	samplingRatio float64
}

// ParseOTLPEndpoint strips the scheme from an endpoint string that may contain
// a URI (e.g. "http://host:4317"). The OTEL_EXPORTER_OTLP_ENDPOINT env var
// uses full URIs per the OpenTelemetry spec, but the gRPC exporter's
// WithEndpoint expects a bare authority (host or host:port).
//
// For http/https URIs the authority (u.Host) is returned, which may or may not
// include a port. For non-http(s) schemes or bare host:port inputs the string
// is returned unchanged.
//
// The second return value indicates whether the scheme specifies TLS (true for
// https://, false otherwise). Use [ResolveOTLPSecurity] to combine this with
// the configured TLS flag.
func ParseOTLPEndpoint(endpoint string) (string, bool) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		// Not a valid URI — treat as a bare host:port.
		return endpoint, false
	}

	switch u.Scheme {
	case "https":
		return u.Host, true
	case "http":
		return u.Host, false
	default:
		// Unknown scheme — return as-is.
		return endpoint, false
	}
}

// ResolveOTLPSecurity returns true if TLS should be used for the OTLP
// connection. TLS is enabled if either the configuration flag or the URI
// scheme (https://) indicates it.
func ResolveOTLPSecurity(configSecure, schemeSecure bool) bool {
	return configSecure || schemeSecure
}

// ResolveSampler maps a sampler name (as defined by the OTEL_TRACES_SAMPLER spec)
// and a sampling ratio to the corresponding Go SDK sampler.
func ResolveSampler(name string, ratio float64) sdktrace.Sampler {
	switch strings.ToLower(name) {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratio)
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	default:
		return sdktrace.TraceIDRatioBased(ratio)
	}
}

func MustNewTracerProvider(opts ...TracerOption) *sdktrace.TracerProvider {
	tracer := &customTracer{
		endpoint:      "",
		attributes:    []attribute.KeyValue{},
		samplingRatio: 0,
	}

	for _, opt := range opts {
		opt(tracer)
	}

	baseRes, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(tracer.attributes...))
	if err != nil {
		panic(err)
	}

	res, err := resource.Merge(baseRes, resource.Environment())

	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	endpoint, schemeSecure := ParseOTLPEndpoint(tracer.endpoint)
	secure := ResolveOTLPSecurity(!tracer.insecure, schemeSecure)

	options := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithDialOption(
			// nolint:staticcheck // ignoring gRPC deprecations
			grpc.WithBlock(),
		),
	}

	if !secure {
		options = append(options, otlptracegrpc.WithInsecure())
	}

	var exp sdktrace.SpanExporter
	exp, err = otlptracegrpc.New(ctx, options...)
	if err != nil {
		panic(fmt.Sprintf("failed to establish a connection with the otlp exporter: %v", err))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(ResolveSampler(tracer.sampler, tracer.samplingRatio)),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exp)),
	)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	otel.SetTracerProvider(tp)

	return tp
}

// TraceError marks the span as having an error, except if the error is context.Canceled,
// in which case it does nothing.
func TraceError(span trace.Span, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
