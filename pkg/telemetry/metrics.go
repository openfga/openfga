package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/openfga/openfga/pkg/logger"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	sdkMetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
)

const providerName = "OpenFGA"

func NewNoopMeter() metric.Meter {
	return metric.NewNoopMeter()
}

func NewOTLPMeter(ctx context.Context, logger logger.Logger, protocol, endpoint string) (metric.Meter, error) {
	var exporter sdkMetrics.Exporter
	var err error
	switch protocol {
	case "http":
		var options []otlpmetrichttp.Option
		options = append(options, otlpmetrichttp.WithInsecure())

		if endpoint != "" {
			options = append(options, otlpmetrichttp.WithEndpoint(endpoint))
		}
		exporter, err = otlpmetrichttp.New(ctx, options...)
	case "grpc":
		var options []otlpmetricgrpc.Option
		options = append(options, otlpmetricgrpc.WithInsecure())

		if endpoint != "" {
			options = append(options, otlpmetricgrpc.WithEndpoint(endpoint))
		}
		exporter, err = otlpmetricgrpc.New(ctx, options...)
	default:
		return nil, fmt.Errorf("unknown open telemetry protocol %q", protocol)
	}
	if err != nil {
		return nil, err
	}
	reader := sdkMetrics.NewPeriodicReader(exporter)
	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceNameKey.String("openfga"),
			semconv.TelemetrySDKNameKey.String("opentelemetry"),
			semconv.TelemetrySDKLanguageKey.String("go"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize otlp resource: %w", err)
	}

	meterProvider := sdkMetrics.NewMeterProvider(sdkMetrics.WithReader(reader), sdkMetrics.WithResource(res))
	global.SetMeterProvider(meterProvider)

	runtime.WithMinimumReadMemStatsInterval(time.Second)
	runtime.WithMeterProvider(meterProvider)
	if err := host.Start(host.WithMeterProvider(meterProvider)); err != nil {
		return nil, err
	}

	return meterProvider.Meter(providerName), nil
}
