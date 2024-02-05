package gateway

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/openfga/openfga/pkg/logger"
)

type Transport interface {
	SetHeader(context.Context, string, string)
}

type NoopTransport struct {
}

var _ Transport = (*NoopTransport)(nil)

func NewNoopTransport() *NoopTransport {
	return &NoopTransport{}
}

func (n *NoopTransport) SetHeader(_ context.Context, key, value string) {

}

type RPCTransport struct {
	logger logger.Logger
}

var _ Transport = (*RPCTransport)(nil)

func NewRPCTransport(l logger.Logger) *RPCTransport {
	return &RPCTransport{logger: l}
}

func (g *RPCTransport) SetHeader(ctx context.Context, key, value string) {
	if err := grpc.SetHeader(ctx, metadata.Pairs(key, value)); err != nil {
		g.logger.ErrorWithContext(
			ctx,
			"failed to set grpc header",
			zap.Error(err),
			zap.String("header", key),
		)
	}
}
