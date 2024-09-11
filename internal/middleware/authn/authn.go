package authn

import (
	"context"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"

	"github.com/openfga/openfga/internal/authn"
	"github.com/openfga/openfga/pkg/authclaims"
)

func AuthFunc(authenticator authn.Authenticator) grpcauth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		claims, err := authenticator.Authenticate(ctx)
		if err != nil {
			return nil, err
		}

		return authclaims.ContextWithAuthClaims(ctx, claims), nil
	}
}
