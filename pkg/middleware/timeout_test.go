package middleware

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/openfga/openfga/pkg/logger"
)

// mockServerGRPCStream mocks grpc server stream and only returns context
// otherwise noop, and we don't care whether it is called or not
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
		goFuncInit := make(chan struct{})
		goFuncDone := make(chan struct{})
		go func() {
			goFuncInit <- struct{}{}
			time.Sleep(20 * time.Millisecond)
			goFuncDone <- struct{}{}
		}()

		<-goFuncInit
		select {
		case <-ctx.Done():
			return nil, nil
		case <-goFuncDone:
			return nil, fmt.Errorf("should have timeout")
		}
	}
	interceptor := timeoutInterceptor.NewUnaryTimeoutInterceptor()
	_, err := interceptor(context.Background(), nil, nil, handler)
	require.NoError(t, err)
}

func TestNewStreamTimeoutInterceptor(t *testing.T) {
	timeoutInterceptor := TimeoutInterceptor{
		timeout: 5 * time.Millisecond,
		logger:  logger.NewNoopLogger(),
	}

	handler := func(srv any, stream grpc.ServerStream) error {
		ctx := stream.Context()
		goFuncInit := make(chan struct{})
		goFuncDone := make(chan struct{})
		go func() {
			goFuncInit <- struct{}{}
			time.Sleep(20 * time.Millisecond)
			goFuncDone <- struct{}{}
		}()

		<-goFuncInit
		select {
		case <-ctx.Done():
			return nil
		case <-goFuncDone:
			return fmt.Errorf("should have timeout")
		}
	}
	interceptor := timeoutInterceptor.NewStreamTimeoutInterceptor()
	err := interceptor(nil, mockServerGRPCStream{ctx: context.Background()}, nil, handler)
	require.NoError(t, err)
}
