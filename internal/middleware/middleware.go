package middleware

import (
	"context"
	"encoding/json"

	"github.com/openfga/openfga/pkg/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const grpcReqReceivedKey = "grpc_req_received"

type ctxKey string

type wrappedServerStream struct {
	grpc.ServerStream
	wrappedContext context.Context
	logger         logger.Logger
}

func newWrappedServerStream(stream grpc.ServerStream, logger logger.Logger) *wrappedServerStream {
	if existing, ok := stream.(*wrappedServerStream); ok {
		return existing
	}
	return &wrappedServerStream{
		ServerStream:   stream,
		wrappedContext: stream.Context(),
		logger:         logger,
	}
}

func (s *wrappedServerStream) Context() context.Context {
	return s.wrappedContext
}

func (s *wrappedServerStream) RecvMsg(m interface{}) error {
	// recvMsgErr handled below after preparing the log fields
	recvMsgErr := s.ServerStream.RecvMsg(m)

	fields := []zap.Field{zap.String(grpcTypeKey, "server_stream")}

	if requestID, ok := RequestIDFromContext(s.Context()); ok {
		fields = append(fields, zap.String(requestIDKey, requestID))
	}

	if r, ok := m.(hasGetStoreID); ok {
		storeID := r.GetStoreId()
		s.wrappedContext = context.WithValue(s.Context(), storeIDCtxKey, storeID)
		fields = append(fields, zap.String(storeIDKey, storeID))
	}

	jsonM, err := json.Marshal(m)
	if err == nil {
		fields = append(fields, zap.Any(rawRequestKey, json.RawMessage(jsonM)))
	}

	if recvMsgErr != nil {
		s.logger.Error(err.Error(), fields...)
		return err
	}

	s.logger.Info(grpcReqReceivedKey, fields...)

	return nil
}
