package recovery

import (
	"context"
	"net/http"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/logger"
)

// HTTPPanicRecoveryHandler recover from panic for http services
func HTTPPanicRecoveryHandler(next http.Handler, logger logger.Logger) http.Handler {
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

// PanicRecoveryHandler recovers from panics for unary/stream services
func PanicRecoveryHandler(logger logger.Logger) grpc_recovery.RecoveryHandlerFuncContext {
	return func(ctx context.Context, p any) error {
		logger.Error("Recovered from panic in RPC",
			zap.Any("panic_info", p))

		return status.Errorf(codes.Unknown, http.StatusText(http.StatusInternalServerError))
	}
}
