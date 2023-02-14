package middleware

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/openfga/openfga/pkg/storage"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ctxKey string

type wrappedServerStream struct {
	grpc.ServerStream
	wrappedContext context.Context
	fields         []zap.Field
	datastore      storage.AuthorizationModelReadBackend
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
	// err handled below after preparing the log fields
	err := s.ServerStream.RecvMsg(m)

	var fields []zap.Field

	var storeID string
	if r, ok := m.(hasGetStoreID); ok {
		storeID = r.GetStoreId()
		s.wrappedContext = context.WithValue(s.Context(), storeIDCtxKey, storeID)
		fields = append(fields, zap.String(storeIDKey, storeID))
	}

	if r, ok := m.(hasGetAuthorizationModelId); ok {
		modelID := r.GetAuthorizationModelId()
		if modelID == "" {
			modelID, err = s.datastore.FindLatestAuthorizationModelID(s.Context(), storeID)
			if err != nil {
				return fmt.Errorf("failed to retrieve model id: %w", err)
			}
		}

		s.wrappedContext = context.WithValue(s.Context(), modelIDCtxKey, modelID)
		fields = append(fields, zap.String(modelIDKey, modelID))

		// Add the modelID to the return header
		_ = grpc.SetHeader(s.Context(), metadata.Pairs(modelIDHeader, modelID))
	}

	if jsonM, err := json.Marshal(m); err == nil {
		fields = append(fields, zap.Any(rawRequestKey, json.RawMessage(jsonM)))
	}

	s.fields = append(s.fields, fields...)

	if err != nil {
		return err
	}

	return nil
}
