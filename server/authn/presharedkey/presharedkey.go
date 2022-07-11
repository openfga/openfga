package presharedkey

import (
	"context"

	"github.com/go-errors/errors"
	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/server/authn"
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
	authHeader, err := grpcAuth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "missing bearer token")
	}

	if _, found := pka.ValidKeys[authHeader]; found {
		return &authn.AuthClaims{
			Subject: "", // no user information in this auth method
		}, nil
	}

	return nil, status.Errorf(codes.PermissionDenied, "unauthorized")
}

func (pka *PresharedKeyAuthenticator) Close() {}
