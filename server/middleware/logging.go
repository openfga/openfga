package middleware

import (
	"context"
	"time"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func NewLoggingInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx = context.WithValue(ctx, storage.DBCounterCtxKey, &storage.DBCounter{})

		start := time.Now()

		resp, err := handler(ctx, req)

		dbCounter := ctx.Value(storage.DBCounterCtxKey).(*storage.DBCounter)
		fields := []zap.Field{
			zap.Duration("took", time.Since(start)),
			zap.String("method", info.FullMethod),
			zap.Int32("db_writes", dbCounter.Writes.Load()),
			zap.Int32("db_reads", dbCounter.Reads.Load()),
		}

		if err != nil {
			if e, ok := err.(serverErrors.InternalError); ok {
				fields = append(fields, zap.Error(e.Internal()))
			}
			fields = append(fields, zap.String("public_error", err.Error()))

			logger.Error("grpc_error", fields...)

			return nil, err
		}

		logger.Info("grpc_complete", fields...)

		return resp, nil
	}
}

func NewStreamingErrorLoggingInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		if err != nil {
			var e error
			if internalError, ok := err.(serverErrors.InternalError); ok {
				e = internalError.Internal()
			}
			logger.Error("grpc_error", zap.Error(e), zap.String("public_error", err.Error()))
		}

		return err
	}
}
