package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/openfga/openfga/pkg/logger"
)

// mockServerGRPCStream mocks grpc server stream and only returns context
// otherwise noop, and we don't care whether it is called or not.
type mockServerGRPCStream struct {
	ctx context.Context
}

func (m mockServerGRPCStream) SetHeader(metadata.MD) error {
	return nil
}
func (m mockServerGRPCStream) SendHeader(metadata.MD) error {
	return nil
}
func (m mockServerGRPCStream) SetTrailer(metadata.MD) {}
func (m mockServerGRPCStream) Context() context.Context {
	return m.ctx
}
func (m mockServerGRPCStream) SendMsg(any) error {
	return nil
}
func (m mockServerGRPCStream) RecvMsg(any) error {
	return nil
}

func TestNewUnaryTimeoutInterceptor(t *testing.T) {
	timeoutInterceptor := TimeoutInterceptor{
		timeout: 5 * time.Millisecond,
		logger:  logger.NewNoopLogger(),
	}

	handler := func(ctx context.Context, req any) (any, error) {
		select {
		case <-time.After(20 * time.Millisecond):
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	interceptor := timeoutInterceptor.NewUnaryTimeoutInterceptor()
	_, err := interceptor(context.Background(), nil, nil, handler)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestNewStreamTimeoutInterceptor(t *testing.T) {
	timeoutInterceptor := TimeoutInterceptor{
		timeout: 5 * time.Millisecond,
		logger:  logger.NewNoopLogger(),
	}

	handler := func(srv any, stream grpc.ServerStream) error {
		ctx := stream.Context()
		select {
		case <-time.After(20 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	interceptor := timeoutInterceptor.NewStreamTimeoutInterceptor()
	err := interceptor(nil, mockServerGRPCStream{ctx: context.Background()}, nil, handler)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
