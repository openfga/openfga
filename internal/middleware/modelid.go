package middleware

import (
	"context"
	"errors"

	"github.com/openfga/openfga/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	modelIDCtxKey ctxKey = "model-id-context-key"

	modelIDTraceKey = "model_id"
	modelIDHeader   = "model-id"
)

func ModelIDFromContext(ctx context.Context) (string, bool) {
	modelID, ok := ctx.Value(modelIDCtxKey).(string)
	return modelID, ok
}

type hasGetAuthorizationModelID interface {
	GetStoreId() string
	GetAuthorizationModelId() string
}

// NewModelIDInterceptor must come after the trace and storeID interceptors
func NewModelIDInterceptor(ds storage.AuthorizationModelReadBackend) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if r, ok := req.(hasGetAuthorizationModelID); ok {
			modelID := r.GetAuthorizationModelId()

			if modelID == "" {
				storeID := r.GetStoreId()
				if storeID == "" {
					return nil, errors.New("no store id")
				}

				var err error
				modelID, err = ds.FindLatestAuthorizationModelID(ctx, storeID)
				if err != nil {
					if errors.Is(err, storage.ErrNotFound) {
						return nil, errors.New("no latest model id in datastore")
					}
					return nil, err
				}
			}

			// Add the modelID to the context
			ctx = context.WithValue(ctx, modelIDCtxKey, modelID)

			// Add the modelID to the span
			trace.SpanFromContext(ctx).SetAttributes(attribute.String(modelIDTraceKey, modelID))

			// Add the modelID to the return header
			_ = grpc.SetHeader(ctx, metadata.Pairs(modelIDHeader, modelID))
		}

		return handler(ctx, req)
	}
}

// NewStreamingModelIDInterceptor must come after the trace and storeID interceptors
func NewStreamingModelIDInterceptor(ds storage.AuthorizationModelReadBackend) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ss := newWrappedServerStream(stream)
		ss.datastore = ds
		return handler(srv, ss)
	}
}
