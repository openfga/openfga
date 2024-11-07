package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/errors"
)

// HTTPPanicRecoveryHandler recover from panic for http services.
func HTTPPanicRecoveryHandler(next http.Handler, logger logger.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("HTTPPanicRecoveryHandler has recovered a panic",
					zap.Error(fmt.Errorf("%v", err)),
					zap.ByteString("stacktrace", debug.Stack()),
				)
				w.Header().Set("content-type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)

				responseBody, err := json.Marshal(map[string]string{
					"code":    openfgav1.InternalErrorCode_internal_error.String(),
					"message": errors.InternalServerErrorMsg,
				})
				if err != nil {
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
					return
				}

				_, err = w.Write(responseBody)
				if err != nil {
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// PanicRecoveryHandler recovers from panics for unary/stream services.
func PanicRecoveryHandler(logger logger.Logger) grpc_recovery.RecoveryHandlerFuncContext {
	return func(ctx context.Context, p any) error {
		logger.Error("PanicRecoveryHandler has recovered a panic",
			zap.Error(fmt.Errorf("%v", p)),
			zap.ByteString("stacktrace", debug.Stack()),
		)

		return status.Errorf(codes.Internal, errors.InternalServerErrorMsg)
	}
}
