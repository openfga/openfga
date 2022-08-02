package presharedkey

import (
	"context"

	"github.com/go-errors/errors"
	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/openfga/openfga/server/authn"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrMissingBearerToken = status.Error(codes.Code(openfgapb.AuthErrorCode_bearer_token_missing), "missing bearer token")
	ErrUnauthenticated    = status.Error(codes.Code(openfgapb.AuthErrorCode_unauthenticated), "unauthenticated")
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
		return nil, ErrMissingBearerToken
	}

	if _, found := pka.ValidKeys[authHeader]; found {
		return &authn.AuthClaims{
			Subject: "", // no user information in this auth method
		}, nil
	}

	return nil, ErrUnauthenticated
}

func (pka *PresharedKeyAuthenticator) Close() {}
