package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/errors"
)

// HTTPPanicRecoveryHandler recover from panic for http services
func HTTPPanicRecoveryHandler(next http.Handler, logger logger.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("HTTPPanicRecoveryHandler has recovered a panic",
					zap.Error(fmt.Errorf("%v", err)))

				w.WriteHeader(http.StatusInternalServerError)
				w.Header().Set("content-type", "application/json")
				responseBody, err := json.Marshal(map[string]string{
					"code":    openfgav1.InternalErrorCode_internal_error.String(),
					"message": errors.InternalServerErrorMsg,
				})
				if err != nil {
					logger.Error("failed to JSON marshal HTTP response body", zap.Error(err))
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				} else {
					_, err = w.Write(responseBody)
					if err != nil {
						logger.Error("failed to write HTTP response body", zap.Error(err))
						http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
					}
				}
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// PanicRecoveryHandler recovers from panics for unary/stream services
func PanicRecoveryHandler(logger logger.Logger) grpc_recovery.RecoveryHandlerFuncContext {
	return func(ctx context.Context, p any) error {
		logger.Error("PanicRecoveryHandler has recovered a panic",
			zap.Error(fmt.Errorf("%v", p)))

		return status.Errorf(codes.Internal, http.StatusText(http.StatusInternalServerError))
	}
}
