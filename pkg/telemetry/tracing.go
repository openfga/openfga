// Package telemetry contains code that emits telemetry (logging, metrics, tracing).
package telemetry

import (
	"context"
	"fmt"
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
type OTLPOption func(d *otlp)

func WithOTLPEndpoint(endpoint string) OTLPOption {
	return func(d *otlp) {
		d.endpoint = endpoint
	}
}

func WithOTLPInsecure() OTLPOption {
	return func(d *otlp) {
		d.insecure = true
	}
}

func WithOTLP(opts ...OTLPOption) TracerOption {
	return func(d *customTracer) {
		d.otlp = &otlp{}

		for _, opt := range opts {
			opt(d.otlp)
		}
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

type otlp struct {
	endpoint string
	insecure bool
}
type customTracer struct {
	otlp *otlp

	attributes    []attribute.KeyValue
	samplingRatio float64
}

func MustNewTracerProvider(opts ...TracerOption) TracerProvider {
	tracer := &customTracer{
		attributes:    []attribute.KeyValue{},
		samplingRatio: 0,
	}

	for _, opt := range opts {
		opt(tracer)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(tracer.attributes...))
	if err != nil {
		panic(err)
	}

	var exp sdktrace.SpanExporter

	if tracer.otlp != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		options := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(tracer.otlp.endpoint),
			otlptracegrpc.WithDialOption(grpc.WithBlock()),
		}

		if tracer.otlp.insecure {
			options = append(options, otlptracegrpc.WithInsecure())
		}

		exp, err = otlptracegrpc.New(ctx, options...)
		if err != nil {
			panic(fmt.Sprintf("failed to establish a connection with the otlp exporter: %v", err))
		}
	}

	providerOptions := []sdktrace.TracerProviderOption{
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(tracer.samplingRatio)),
		sdktrace.WithResource(res),
	}

	if exp != nil {
		providerOptions = append(
			providerOptions,
			sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exp)),
		)
	}

	tp := sdktrace.NewTracerProvider(providerOptions...)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	otel.SetTracerProvider(tp)

	return &tracerProvider{tp: tp}
}

func TraceError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
