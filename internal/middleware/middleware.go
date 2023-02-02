package middleware

import (
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/openfga/openfga/internal/authn"
	"github.com/openfga/openfga/pkg/logger"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

type ctxKey string

type Config struct {
	Logr           logger.Logger
	Authenticator  authn.Authenticator
	TracerProvider trace.TracerProvider
}

// NewInterceptors creates and returns a number of unary and streaming
// interceptors. There is some dependencies on the order for things to work
// properly:
//  1. The trace interceptor should go before the requestID interceptor as the
//     latter will add the requestID to the trace
//  2. The trace and requestID interceptors should go before the logging
//     interceptor as the logging interceptor will extract information about
//     them from the context.
func NewInterceptors(cfg *Config) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		grpc_validator.UnaryServerInterceptor(),
		otelgrpc.UnaryServerInterceptor(otelgrpc.WithTracerProvider(cfg.TracerProvider)),
		NewRequestIDInterceptor(cfg.Logr),
		NewLoggingInterceptor(cfg.Logr),
		grpc_auth.UnaryServerInterceptor(AuthFunc(cfg.Authenticator)),
	}

	streamingInterceptors := []grpc.StreamServerInterceptor{
		grpc_validator.StreamServerInterceptor(),
		otelgrpc.StreamServerInterceptor(otelgrpc.WithTracerProvider(cfg.TracerProvider)),
		NewStreamingRequestIDInterceptor(cfg.Logr),
		NewStreamingLoggingInterceptor(cfg.Logr),
		grpc_auth.StreamServerInterceptor(AuthFunc(cfg.Authenticator)),
	}

	return unaryInterceptors, streamingInterceptors
}
