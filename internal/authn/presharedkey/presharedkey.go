package presharedkey

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"

	"github.com/openfga/openfga/internal/authn"
	"github.com/openfga/openfga/pkg/authclaims"
)

type PresharedKeyAuthenticator struct {
	// validKeyHashes holds SHA-256 digests of each configured key. Storing
	// fixed-length hashes lets Authenticate use subtle.ConstantTimeCompare
	// without the length-dependent early-return that would otherwise leak
	// configured key lengths through timing.
	validKeyHashes [][sha256.Size]byte
}

var _ authn.Authenticator = (*PresharedKeyAuthenticator)(nil)

func NewPresharedKeyAuthenticator(validKeys []string) (*PresharedKeyAuthenticator, error) {
	if len(validKeys) < 1 {
		return nil, errors.New("invalid auth configuration, please specify at least one key")
	}
	hashes := make([][sha256.Size]byte, 0, len(validKeys))
	for _, k := range validKeys {
		hashes = append(hashes, sha256.Sum256([]byte(k)))
	}

	return &PresharedKeyAuthenticator{validKeyHashes: hashes}, nil
}

func (pka *PresharedKeyAuthenticator) Authenticate(ctx context.Context) (*authclaims.AuthClaims, error) {
	authHeader, err := grpcauth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return nil, authn.ErrMissingBearerToken
	}

	// Hash the presented token and compare against every configured key
	// hash without early-return. All inputs to ConstantTimeCompare are
	// fixed-length, so total time depends only on the number of configured
	// keys — not on which (if any) matched, nor on configured key lengths.
	tokenHash := sha256.Sum256([]byte(authHeader))
	var matched int
	for _, kh := range pka.validKeyHashes {
		matched |= subtle.ConstantTimeCompare(tokenHash[:], kh[:])
	}
	if matched == 1 {
		return &authclaims.AuthClaims{
			Subject: "", // no user information in this auth method
		}, nil
	}

	return nil, authn.ErrUnauthenticated
}

func (pka *PresharedKeyAuthenticator) Close() {}
