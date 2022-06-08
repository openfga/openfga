package telemetry

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/nonrecording"
)

func NewNoopMeter() metric.Meter {
	return nonrecording.NewNoopMeter()
}
