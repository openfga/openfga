package presharedkey

import (
	"context"
	"errors"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"

	"github.com/openfga/openfga/internal/authn"
	"github.com/openfga/openfga/pkg/authclaims"
)

type PresharedKeyAuthenticator struct {
	ValidKeys map[string]struct{}
}

var _ authn.Authenticator = (*PresharedKeyAuthenticator)(nil)

func NewPresharedKeyAuthenticator(validKeys []string) (*PresharedKeyAuthenticator, error) {
	if len(validKeys) < 1 {
		return nil, errors.New("invalid auth configuration, please specify at least one key")
	}
	vKeys := make(map[string]struct{})
	for _, k := range validKeys {
		vKeys[k] = struct{}{}
	}

	return &PresharedKeyAuthenticator{ValidKeys: vKeys}, nil
}

func (pka *PresharedKeyAuthenticator) Authenticate(ctx context.Context) (*authclaims.AuthClaims, error) {
	authHeader, err := grpcauth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return nil, authn.ErrMissingBearerToken
	}

	if _, found := pka.ValidKeys[authHeader]; found {
		return &authclaims.AuthClaims{
			Subject: "", // no user information in this auth method
		}, nil
	}

	return nil, authn.ErrUnauthenticated
}

func (pka *PresharedKeyAuthenticator) Close() {}
