package authn

import (
	"context"

	"github.com/MicahParks/keyfunc/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ctxKey string

var (
	authClaimsContextKey = ctxKey("auth-claims")

	ErrUnauthenticated    = status.Error(codes.Code(openfgav1.AuthErrorCode_unauthenticated), "unauthenticated")
	ErrMissingBearerToken = status.Error(codes.Code(openfgav1.AuthErrorCode_bearer_token_missing), "missing bearer token")
)

type Authenticator interface {
	// Authenticate returns a nil error and the AuthClaims info (if available) if the subject is authenticated or a
	// non-nil error with an appropriate error cause otherwise.
	Authenticate(requestContext context.Context) (*AuthClaims, error)

	// Close Cleans up the authenticator.
	Close()
}

type NoopAuthenticator struct{}

var _ Authenticator = (*NoopAuthenticator)(nil)

func (n NoopAuthenticator) Authenticate(requestContext context.Context) (*AuthClaims, error) {
	return &AuthClaims{
		Subject: "",
		Scopes:  nil,
	}, nil
}

func (n NoopAuthenticator) Close() {}

// AuthClaims contains claims that are included in OIDC standard claims. https://openid.net/specs/openid-connect-core-1_0.html#IDToken
type AuthClaims struct {
	Subject string
	Scopes  map[string]bool
}

// ContextWithAuthClaims injects the provided AuthClaims into the parent context.
func ContextWithAuthClaims(parent context.Context, claims *AuthClaims) context.Context {
	return context.WithValue(parent, authClaimsContextKey, claims)
}

// AuthClaimsFromContext extracts the AuthClaims from the provided ctx (if any).
func AuthClaimsFromContext(ctx context.Context) (*AuthClaims, bool) {
	claims, ok := ctx.Value(authClaimsContextKey).(*AuthClaims)
	if !ok {
		return nil, false
	}

	return claims, true
}

// OidcConfig contains authorization server metadata. See https://datatracker.ietf.org/doc/html/rfc8414#section-2
type OidcConfig struct {
	Issuer  string `json:"issuer"`
	JWKsURI string `json:"jwks_uri"`
}

type OIDCAuthenticator interface {
	GetConfiguration() (*OidcConfig, error)
	GetKeys() (*keyfunc.JWKS, error)
}
