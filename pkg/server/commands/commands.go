package commands

import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("openfga/pkg/server/commands")
