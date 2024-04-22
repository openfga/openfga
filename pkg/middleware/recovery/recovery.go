package recovery

import (
	"context"
	"net/http"

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
					zap.Any("panic_info", err))
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// UnaryPanicInterceptor recovers from panics in unary handlers
func UnaryPanicInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return grpc_recovery.UnaryServerInterceptor(
		grpc_recovery.WithRecoveryHandlerContext(func(ctx context.Context, p any) error {
			logger.Error("Recovered from panic in unary RPC",
				zap.Any("panic_info", p))
			return status.Errorf(codes.Unknown, http.StatusText(http.StatusInternalServerError))
		}))
}

// StreamPanicInterceptor recovers from panics in stream handlers
func StreamPanicInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return grpc_recovery.StreamServerInterceptor(
		grpc_recovery.WithRecoveryHandlerContext(func(ctx context.Context, p any) error {
			logger.Error("Recovered from panic in stream RPC",
				zap.Any("panic_info", p))
			return status.Errorf(codes.Unknown, http.StatusText(http.StatusInternalServerError))
		}))
}
