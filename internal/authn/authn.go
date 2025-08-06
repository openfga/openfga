package authn

import (
	"context"

	"github.com/MicahParks/keyfunc/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/authclaims"
)

var (
	ErrUnauthenticated    = status.Error(codes.Code(openfgav1.AuthErrorCode_unauthenticated), "unauthenticated")
	ErrMissingBearerToken = status.Error(codes.Code(openfgav1.AuthErrorCode_bearer_token_missing), "missing bearer token")
)

type Authenticator interface {
	// Authenticate returns a nil error and the AuthClaims info (if available) if the subject is authenticated or a
	// non-nil error with an appropriate error cause otherwise.
	Authenticate(requestContext context.Context) (*authclaims.AuthClaims, error)
}

type NoopAuthenticator struct{}

var _ Authenticator = (*NoopAuthenticator)(nil)

func (n NoopAuthenticator) Authenticate(requestContext context.Context) (*authclaims.AuthClaims, error) {
	return &authclaims.AuthClaims{
		Subject: "",
		Scopes:  nil,
	}, nil
}

func (n NoopAuthenticator) Close() {}

// OidcConfig contains authorization server metadata. See https://datatracker.ietf.org/doc/html/rfc8414#section-2
type OidcConfig struct {
	Issuer  string `json:"issuer"`
	JWKsURI string `json:"jwks_uri"`
}

type OIDCAuthenticator interface {
	GetConfiguration() (*OidcConfig, error)
	GetKeys() (keyfunc.Keyfunc, error)
}
