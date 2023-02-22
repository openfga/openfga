package storeid

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	storeIDTraceKey = "store_id"
	storeIDHeader   = "openfga-store-id"
)

type hasGetStoreID interface {
	GetStoreId() string
}

// NewUnaryInterceptor creates a grpc.UnaryServerInterceptor which must come
// after the trace interceptor.
func NewUnaryInterceptor() grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(reportable())
}

// NewStreamingInterceptor creates a grpc.StreamServerInterceptor which must
// come after the trace interceptor.
func NewStreamingInterceptor() grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(reportable())
}

type reporter struct {
	ctx context.Context
}

func (r *reporter) PostCall(error, time.Duration) {}

func (r *reporter) PostMsgSend(interface{}, error, time.Duration) {}

func (r *reporter) PostMsgReceive(msg interface{}, _ error, _ time.Duration) {
	if m, ok := msg.(hasGetStoreID); ok {
		storeID := m.GetStoreId()

		// Add the storeID to the span
		trace.SpanFromContext(r.ctx).SetAttributes(attribute.String(storeIDTraceKey, storeID))

		// Add the storeID to the response headers
		_ = grpc.SetHeader(r.ctx, metadata.Pairs(storeIDHeader, storeID))
	}
}

func reportable() interceptors.CommonReportableFunc {
	return func(ctx context.Context, c interceptors.CallMeta, isClient bool) (interceptors.Reporter, context.Context) {
		return &reporter{ctx}, ctx
	}
}
