package middleware

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const grpcReqReceivedKey = "grpc_req_received"

type ctxKey string

type wrappedServerStream struct {
	grpc.ServerStream
	wrappedContext context.Context
	fields         []zap.Field
}

func newWrappedServerStream(stream grpc.ServerStream) *wrappedServerStream {
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
	// recvMsgErr handled below after preparing the log fields
	recvMsgErr := s.ServerStream.RecvMsg(m)

	var fields []zap.Field

	if r, ok := m.(hasGetStoreID); ok {
		storeID := r.GetStoreId()
		s.wrappedContext = context.WithValue(s.Context(), storeIDCtxKey, storeID)
		fields = append(fields, zap.String(storeIDKey, storeID))
	}

	jsonM, err := json.Marshal(m)
	if err == nil {
		fields = append(fields, zap.Any(rawRequestKey, json.RawMessage(jsonM)))
	}

	s.fields = fields

	if recvMsgErr != nil {
		return err
	}

	return nil
}
