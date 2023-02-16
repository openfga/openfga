package middleware

import (
	"context"

	"google.golang.org/grpc"
)

type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *mockServerStream) Context() context.Context {
	return s.ctx
}

func (s *mockServerStream) RecvMsg(m interface{}) error {
	return nil
}
