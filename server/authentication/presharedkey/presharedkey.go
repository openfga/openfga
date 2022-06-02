package presharedkey

import (
	"context"

	"github.com/go-errors/errors"
	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"github.com/openfga/openfga/server/authentication"
)

type PresharedKeyAuthenticator struct {
	ValidKeys map[string]struct{}
}

var _ authentication.Authenticator = (*PresharedKeyAuthenticator)(nil)

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

func (pka *PresharedKeyAuthenticator) Authenticate(ctx context.Context, requestParameters any) (*authentication.AuthClaims, error) {
	authHeader, err := grpcAuth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return nil, errors.New("missing bearer token")
	}

	if _, found := pka.ValidKeys[authHeader]; found {
		return &authentication.AuthClaims{
			Subject: "", // no user information in this auth method
		}, nil
	}

	return nil, errors.New("unauthorized")
}

func (pka *PresharedKeyAuthenticator) Close() {
	// do nothing
}
