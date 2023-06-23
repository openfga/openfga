package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/openfga/openfga/internal/build"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

type TracerOption func(d *CustomTracer)

func WithOTLPEndpoint(endpoint string) TracerOption {
	return func(d *CustomTracer) {
		d.endpoint = endpoint
	}
}

func WithServiceName(serviceName string) TracerOption {
	return func(d *CustomTracer) {
		d.serviceName = serviceName
	}
}

func WithSamplingRatio(samplingRatio float64) TracerOption {
	return func(d *CustomTracer) {
		d.samplingRatio = samplingRatio
	}
}

func WithEnableTailLatencySpanExporter(enable bool) TracerOption {
	return func(d *CustomTracer) {
		d.enableTailLatencySpanExporter = enable
	}
}

func WithTailLatencyInMillisecond(latency int) TracerOption {
	return func(d *CustomTracer) {
		d.tailLatencyInMs = latency
	}
}

type CustomTracer struct {
	endpoint    string
	serviceName string

	samplingRatio float64

	enableTailLatencySpanExporter bool
	tailLatencyInMs               int
}

func MustNewTracerProvider(opts ...TracerOption) *sdktrace.TracerProvider {
	tracer := &CustomTracer{
		endpoint:                      "",
		serviceName:                   "",
		samplingRatio:                 0,
		enableTailLatencySpanExporter: false,
		tailLatencyInMs:               0,
	}

	for _, opt := range opts {
		opt(tracer)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceNameKey.String(tracer.serviceName),
			semconv.ServiceVersionKey.String(build.Version),
		))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var exp sdktrace.SpanExporter
	exp, err = otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(tracer.endpoint),
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to establish a connection with the otlp exporter: %v", err))
	}

	if tracer.enableTailLatencySpanExporter {
		exp = NewTailLatencySpanExporter(exp, WithLatencyInMs(tracer.tailLatencyInMs))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(tracer.samplingRatio)),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exp)),
	)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	otel.SetTracerProvider(tp)

	return tp
}

func TraceError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
