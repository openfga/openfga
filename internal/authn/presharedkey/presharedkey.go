package presharedkey

import (
	"context"
	"crypto/subtle"
	"errors"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"

	"github.com/openfga/openfga/internal/authn"
	"github.com/openfga/openfga/pkg/authclaims"
)

type PresharedKeyAuthenticator struct {
	ValidKeys [][]byte
}

var _ authn.Authenticator = (*PresharedKeyAuthenticator)(nil)

func NewPresharedKeyAuthenticator(validKeys []string) (*PresharedKeyAuthenticator, error) {
	if len(validKeys) < 1 {
		return nil, errors.New("invalid auth configuration, please specify at least one key")
	}
	vKeys := make([][]byte, 0, len(validKeys))
	for _, k := range validKeys {
		vKeys = append(vKeys, []byte(k))
	}

	return &PresharedKeyAuthenticator{ValidKeys: vKeys}, nil
}

func (pka *PresharedKeyAuthenticator) Authenticate(ctx context.Context) (*authclaims.AuthClaims, error) {
	authHeader, err := grpcauth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return nil, authn.ErrMissingBearerToken
	}

	// Compare against every configured key without early-return so total
	// time depends only on the number of configured keys, not on which
	// (if any) matched.
	token := []byte(authHeader)
	var matched int
	for _, k := range pka.ValidKeys {
		matched |= subtle.ConstantTimeCompare(token, k)
	}
	if matched == 1 {
		return &authclaims.AuthClaims{
			Subject: "", // no user information in this auth method
		}, nil
	}

	return nil, authn.ErrUnauthenticated
}

func (pka *PresharedKeyAuthenticator) Close() {}
