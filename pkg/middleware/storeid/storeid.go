package storeid

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/openfga/openfga/pkg/middleware/grpc_ctxtags"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ctxKey string

const (
	storeIDCtxKey ctxKey = "store-id-context-key"
	storeIDKey    string = "store_id"

	// StoreIDHeader represents the HTTP header name used to
	// specify the OpenFGA store identifier in API requests.
	StoreIDHeader string = "Openfga-Store-Id"
)

type storeidHandle struct {
	storeid string
}

// StoreIDFromContext retrieves the store ID stored in the provided context.
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

// SetStoreIDInContext sets the store ID in the provided context based on information from the request.
func SetStoreIDInContext(ctx context.Context, req interface{}) {
	handle := ctx.Value(storeIDCtxKey)
	if handle == nil {
		return
	}

	if r, ok := req.(hasGetStoreID); ok {
		handle.(*storeidHandle).storeid = r.GetStoreId()
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

// PostCall is a placeholder for handling actions after a gRPC call.
func (r *reporter) PostCall(error, time.Duration) {}

// PostMsgSend is a placeholder for handling actions after sending a message in streaming requests.
func (r *reporter) PostMsgSend(interface{}, error, time.Duration) {}

// PostMsgReceive is invoked after receiving a message in streaming requests.
func (r *reporter) PostMsgReceive(msg interface{}, _ error, _ time.Duration) {
	if m, ok := msg.(hasGetStoreID); ok {
		storeID := m.GetStoreId()

		SetStoreIDInContext(r.ctx, msg)
		trace.SpanFromContext(r.ctx).SetAttributes(attribute.String(storeIDKey, storeID))

		grpc_ctxtags.Extract(r.ctx).Set(storeIDKey, storeID)

		_ = grpc.SetHeader(r.ctx, metadata.Pairs(StoreIDHeader, storeID))
	}
}

func reportable() interceptors.CommonReportableFunc {
	return func(ctx context.Context, c interceptors.CallMeta) (interceptors.Reporter, context.Context) {
		ctx = contextWithHandle(ctx)

		r := reporter{ctx}
		return &r, r.ctx
	}
}
