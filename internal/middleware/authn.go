package middleware

import (
	"context"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/openfga/openfga/server/authn"
)

func AuthFunc(authenticator authn.Authenticator) grpc_auth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		claims, err := authenticator.Authenticate(ctx)
		if err != nil {
			return nil, err
		}

		return authn.ContextWithAuthClaims(ctx, claims), nil
	}
}
