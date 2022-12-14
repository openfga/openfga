package presharedkey

import (
	"context"
	"errors"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/openfga/openfga/internal/authn"
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

func (pka *PresharedKeyAuthenticator) Authenticate(ctx context.Context) (*authn.AuthClaims, error) {
	authHeader, err := grpc_auth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return nil, authn.ErrMissingBearerToken
	}

	if _, found := pka.ValidKeys[authHeader]; found {
		return &authn.AuthClaims{
			Subject: "", // no user information in this auth method
		}, nil
	}

	return nil, authn.ErrUnauthenticated
}

func (pka *PresharedKeyAuthenticator) Close() {}
