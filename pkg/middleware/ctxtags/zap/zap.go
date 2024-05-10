package zap

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/openfga/openfga/pkg/middleware/ctxtags"
)

type ctxMarker struct{}

type ctxLogger struct {
	logger *zap.Logger
}

var (
	ctxMarkerKey = &ctxMarker{}
)

// TagsToFields transforms the Tags on the supplied context into zap fields.
func TagsToFields(ctx context.Context) []zapcore.Field {
	fields := []zapcore.Field{}
	tags := ctxtags.Extract(ctx)
	for k, v := range tags.Values() {
		fields = append(fields, zap.Any(k, v))
	}
	return fields
}

// ToContext adds the zap.Logger to the context for extraction later.
// Returning the new context that has been created.
func ToContext(ctx context.Context, logger *zap.Logger) context.Context {
	l := &ctxLogger{
		logger: logger,
	}
	return context.WithValue(ctx, ctxMarkerKey, l)
}
