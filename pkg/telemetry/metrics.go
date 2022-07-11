package telemetry

import (
	"go.opentelemetry.io/otel/metric"
)

func NewNoopMeter() metric.Meter {
	return metric.NewNoopMeter()
}
