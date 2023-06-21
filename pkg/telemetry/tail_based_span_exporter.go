package telemetry

import (
	"context"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	DefaultLatencyInMs = 1000
)

type tailBasedSpanExporter struct {
	wrappedExporter sdktrace.SpanExporter

	latency time.Duration
}

type TailBasedSpanExporterOption func(o *TailBasedSpanExporterOptions)

type TailBasedSpanExporterOptions struct {
	Latency time.Duration
}

func WithLatencyInMs(latency int) TailBasedSpanExporterOption {
	return func(o *TailBasedSpanExporterOptions) {
		o.Latency = time.Duration(latency) * time.Millisecond
	}
}

var _ sdktrace.SpanExporter = (*tailBasedSpanExporter)(nil)

// NewTailLatencySpanExporter creates a new SpanExporter that will send spans to the given exporter
// only if the span's duration is equal to or above a minimum configurable latency.
//
// If the exporter is nil, the span processor will do nothing.
func NewTailLatencySpanExporter(exporter sdktrace.SpanExporter, options ...TailBasedSpanExporterOption) *tailBasedSpanExporter {
	o := TailBasedSpanExporterOptions{
		Latency: time.Duration(DefaultLatencyInMs) * time.Millisecond,
	}
	for _, opt := range options {
		opt(&o)
	}

	t := &tailBasedSpanExporter{
		wrappedExporter: exporter,
		latency:         o.Latency,
	}

	return t
}

func (t tailBasedSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if t.wrappedExporter == nil {
		return nil
	}

	interestingTraces := make(map[trace.TraceID]struct{})

	// first we find the trace IDs that we want to export
	for _, span := range spans {
		if !span.Parent().IsValid() {
			// top level span!

			duration := span.EndTime().Sub(span.StartTime())

			if duration >= t.latency {
				interestingTraces[span.SpanContext().TraceID()] = struct{}{}
			}
		}
	}

	interestingSpans := make([]sdktrace.ReadOnlySpan, 0, len(spans))

	// then we only export the spans that are associated to those trace IDs
	for _, span := range spans {
		if _, ok := interestingTraces[span.SpanContext().TraceID()]; ok {
			interestingSpans = append(interestingSpans, span)
		}
	}

	return t.wrappedExporter.ExportSpans(ctx, interestingSpans)
}

func (t tailBasedSpanExporter) Shutdown(ctx context.Context) error {
	return t.wrappedExporter.Shutdown(ctx)
}
