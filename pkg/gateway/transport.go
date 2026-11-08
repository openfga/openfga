package gateway

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/openfga/openfga/pkg/logger"
)

// Transport is the interface to work with the transport layer.
type Transport interface {
	// SetHeader sets a response header with a key and a value.
	// It should not be called after a response has been sent.
	SetHeader(ctx context.Context, key, value string)
}

// NoopTransport defines a no-op transport.
type NoopTransport struct {
}

var _ Transport = (*NoopTransport)(nil)

func NewNoopTransport() *NoopTransport {
	return &NoopTransport{}
}

func (n *NoopTransport) SetHeader(_ context.Context, key, value string) {

}

// RPCTransport defines a transport for gRPC.
type RPCTransport struct {
	logger logger.Logger
}

var _ Transport = (*RPCTransport)(nil)

// NewRPCTransport returns a transport for gRPC.
func NewRPCTransport(l logger.Logger) *RPCTransport {
	return &RPCTransport{logger: l}
}

// SetHeader tries to set a header. If an error occurred, it logs an error,
// unless the request context is already done: the client has cancelled or
// disconnected, so the stream is closed and nothing is left to receive the
// header. That case is logged at debug level.
func (g *RPCTransport) SetHeader(ctx context.Context, key, value string) {
	err := grpc.SetHeader(ctx, metadata.Pairs(key, value))
	if err == nil {
		return
	}

	fields := []zap.Field{zap.Error(err), zap.String("header", key)}
	if ctx.Err() != nil {
		g.logger.DebugWithContext(ctx, "failed to set grpc header", append(fields, zap.NamedError("context_error", ctx.Err()))...)
		return
	}

	g.logger.ErrorWithContext(ctx, "failed to set grpc header", fields...)
}
