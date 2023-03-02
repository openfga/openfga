package storeid

import (
	"context"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ctxKey string

const (
	storeIDCtxKey ctxKey = "store-id-context-key"
	storeIDKey    string = "store_id"
	storeIDHeader string = "openfga-store-id"
)

type storeidHandle struct {
	storeid string
}

func StoreIDFromContext(ctx context.Context) (string, bool) {
	if c := ctx.Value(storeIDCtxKey); c != nil {
		handle := c.(*storeidHandle)
		return handle.storeid, true
	}

	return "", false
}

func contextWithHandle(ctx context.Context) context.Context {
	return context.WithValue(ctx, storeIDCtxKey, &storeidHandle{})
}

func SetStoreIDInContext(ctx context.Context, req interface{}) error {
	switch req := req.(type) {
	case hasGetStoreID:
		handle := ctx.Value(storeIDCtxKey)
		if handle == nil {
			return nil
		}

		handle.(*storeidHandle).storeid = req.GetStoreId()
		return nil
	default:
		return nil
	}
}

type hasGetStoreID interface {
	GetStoreId() string
}

// NewUnaryInterceptor creates a grpc.UnaryServerInterceptor which injects
// store_id metadata into the RPC context if an RPC message is received with
// a GetStoreId method.
func NewUnaryInterceptor() grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(reportable())
}

// NewStreamingInterceptor creates a grpc.StreamServerInterceptor which injects
// store_id metadata into the RPC context if an RPC message is received with a
// GetStoreId method.
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

		SetStoreIDInContext(r.ctx, msg)
		trace.SpanFromContext(r.ctx).SetAttributes(attribute.String(storeIDKey, storeID))

		grpc_ctxtags.Extract(r.ctx).Set(storeIDKey, storeID)

		_ = grpc.SetHeader(r.ctx, metadata.Pairs(storeIDHeader, storeID))
	}
}

func reportable() interceptors.CommonReportableFunc {
	return func(ctx context.Context, c interceptors.CallMeta, isClient bool) (interceptors.Reporter, context.Context) {

		ctx = contextWithHandle(ctx)

		r := reporter{ctx}
		return &r, r.ctx
	}
}
