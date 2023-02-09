package middleware

import (
	"context"

	"google.golang.org/grpc"
)

type ctxKey string

type wrappedServerStream struct {
	grpc.ServerStream
	wrappedContext context.Context
}

func newWrapperServerStream(stream grpc.ServerStream) *wrappedServerStream {
	if existing, ok := stream.(*wrappedServerStream); ok {
		return existing
	}
	return &wrappedServerStream{
		ServerStream:   stream,
		wrappedContext: stream.Context(),
	}
}

func (s *wrappedServerStream) Context() context.Context {
	return s.wrappedContext
}

func (s *wrappedServerStream) RecvMsg(m interface{}) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	s.wrappedContext = context.WithValue(s.Context(), storeIDCtxKey, s)

	if r, ok := m.(hasGetStoreID); ok {
		storeID := r.GetStoreId()
		s.wrappedContext = context.WithValue(s.Context(), storeIDCtxKey, storeID)

	}

	return nil
}
