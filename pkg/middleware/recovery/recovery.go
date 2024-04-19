package recovery

import (
	"context"
	"net/http"
	"runtime/debug"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/logger"
)

func Panic(next http.Handler, logger logger.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("Recovered from panic",
					zap.Any("panic_info", err),
					zap.ByteString("stack_trace", debug.Stack()))
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// UnaryPanicInterceptor recovers from panics in unary handlers
func UnaryPanicInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		grpc_recovery.StreamServerInterceptor(
			grpc_recovery.WithRecoveryHandlerContext(func(ctx context.Context, p any) (err error) {
				logger.Error("Recovered from panic in unary RPC",
					zap.Any("panic_info", p),
					zap.ByteString("stack_trace", debug.Stack()))
				return status.Errorf(codes.Unknown, http.StatusText(http.StatusInternalServerError))
			}))
		return handler(ctx, req)
	}
}

// StreamPanicInterceptor recovers from panics in stream handlers
func StreamPanicInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		grpc_recovery.StreamServerInterceptor(
			grpc_recovery.WithRecoveryHandlerContext(func(ctx context.Context, p any) (err error) {
				logger.Error("Recovered from panic in stream RPC",
					zap.Any("panic_info", p),
					zap.ByteString("stack_trace", debug.Stack()))
				return status.Errorf(codes.Unknown, http.StatusText(http.StatusInternalServerError))
			}))
		return handler(srv, stream)
	}
}
