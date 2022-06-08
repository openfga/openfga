package authentication

import (
	"context"

	"github.com/MicahParks/keyfunc"
)

type Authenticator interface {
	// Authenticate returns a nil error and the AuthClaims info (if available) if the subject is authenticated or a
	// non-nil error with an appropriate error cause otherwise.
	Authenticate(requestContext context.Context, requestParameters any) (*AuthClaims, error)

	// Close Cleans up the authenticator.
	Close()
}

type NoopAuthenticator struct{}

func (n NoopAuthenticator) Authenticate(requestContext context.Context, requestParameters any) (*AuthClaims, error) {
	return &AuthClaims{
		Subject: "",
		Scopes:  nil,
	}, nil
}

func (n NoopAuthenticator) Close() {
	// do nothing
}

var _ Authenticator = (*NoopAuthenticator)(nil)

//AuthClaims contains claims that are included in OIDC standard claims. https://openid.net/specs/openid-connect-core-1_0.html#IDToken
type AuthClaims struct {
	Subject string
	Scopes  map[string]bool
}

// OidcConfig contains authorization server metadata. See https://datatracker.ietf.org/doc/html/rfc8414#section-2
type OidcConfig struct {
	Issuer  string `json:"issuer"`
	JWKsURI string `json:"jwks_uri"`
}

type OidcAuthenticator interface {
	GetConfiguration() (*OidcConfig, error)
	GetKeys() (*keyfunc.JWKS, error)
}
