package middleware

import (
	"context"

	"github.com/openfga/openfga/server/authentication"

	"google.golang.org/grpc"
)

func NewAuthenticationInterceptor(authenticator authentication.Authenticator) func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		_, err := authenticator.Authenticate(ctx, req)
		if err != nil {
			return nil, err
		}

		// now we can add principal information to the context
		// ctx = WithAuthPrincipal(ctx, principal)
		return handler(ctx, req)
	}
}
