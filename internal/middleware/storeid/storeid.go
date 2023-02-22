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
	storeIDCtxKey   = "store-id-context-key"
	storeIDTraceKey = "store_id"
	storeIDHeader   = "openfga-store-id"
)

func FromContext(ctx context.Context) (string, bool) {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if vals := md.Get(storeIDCtxKey); len(vals) > 0 {
			return vals[0], true
		}
	}
	return "", false
}

type hasGetStoreID interface {
	GetStoreId() string
}

// NewUnaryInterceptor creates a grpc.UnaryServerInterceptor which must come
// after the trace interceptor and before the logging interceptor.
func NewUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if r, ok := req.(hasGetStoreID); ok {
			storeID := r.GetStoreId()

			// Add the storeID to the span
			trace.SpanFromContext(ctx).SetAttributes(attribute.String(storeIDTraceKey, storeID))

			// Add the storeID to the response headers
			_ = grpc.SetHeader(ctx, metadata.Pairs(storeIDHeader, storeID))
		}

		return handler(ctx, req)
	}
}

// NewStreamingInterceptor creates a grpc.StreamServerInterceptor and
// must come after the trace interceptor and before the logging interceptor.
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
