package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc"
	"github.com/golang-jwt/jwt/v4"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/hashicorp/go-retryablehttp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/internal/authn"
)

type RemoteOidcAuthenticator struct {
	IssuerURLs []string
	Audience   string

	JwksURI string
	JWKs    *keyfunc.JWKS

	httpClient *http.Client
}

var (
	jwkRefreshInterval = 48 * time.Hour

	errInvalidAudience = status.Error(codes.Code(openfgav1.AuthErrorCode_auth_failed_invalid_audience), "invalid audience")
	errInvalidClaims   = status.Error(codes.Code(openfgav1.AuthErrorCode_invalid_claims), "invalid claims")
	errInvalidIssuer   = status.Error(codes.Code(openfgav1.AuthErrorCode_auth_failed_invalid_issuer), "invalid issuer")
	errInvalidSubject  = status.Error(codes.Code(openfgav1.AuthErrorCode_auth_failed_invalid_subject), "invalid subject")
	errInvalidToken    = status.Error(codes.Code(openfgav1.AuthErrorCode_auth_failed_invalid_bearer_token), "invalid bearer token")

	fetchJWKs = fetchJWK
)

var _ authn.Authenticator = (*RemoteOidcAuthenticator)(nil)
var _ authn.OIDCAuthenticator = (*RemoteOidcAuthenticator)(nil)

func NewRemoteOidcAuthenticator(issuerURLs []string, audience string) (*RemoteOidcAuthenticator, error) {
	client := retryablehttp.NewClient()
	client.Logger = nil
	oidc := &RemoteOidcAuthenticator{
		IssuerURLs: issuerURLs,
		Audience:   audience,
		httpClient: client.StandardClient(),
	}
	err := fetchJWKs(oidc)
	if err != nil {
		return nil, err
	}
	return oidc, nil
}

func (oidc *RemoteOidcAuthenticator) Authenticate(requestContext context.Context) (*authn.AuthClaims, error) {
	authHeader, err := grpcauth.AuthFromMD(requestContext, "Bearer")
	if err != nil {
		return nil, authn.ErrMissingBearerToken
	}

	jwtParser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))

	token, err := jwtParser.Parse(authHeader, func(token *jwt.Token) (any, error) {
		return oidc.JWKs.Keyfunc(token)
	})
	if err != nil {
		return nil, errInvalidToken
	}

	if !token.Valid {
		return nil, errInvalidToken
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errInvalidClaims
	}

	ok = slices.ContainsFunc(oidc.IssuerURLs, func(issuer string) bool {
		return claims.VerifyIssuer(issuer, true)
	})

	if !ok {
		return nil, errInvalidIssuer
	}

	if ok := claims.VerifyAudience(oidc.Audience, true); !ok {
		return nil, errInvalidAudience
	}

	// optional subject
	var subject = ""
	if subjectClaim, ok := claims["sub"]; ok {
		if subject, ok = subjectClaim.(string); !ok {
			return nil, errInvalidSubject
		}
	}

	principal := &authn.AuthClaims{
		Subject: subject,
		Scopes:  make(map[string]bool),
	}

	// optional scopes
	if scopeKey, ok := claims["scope"]; ok {
		if scope, ok := scopeKey.(string); ok {
			scopes := strings.Split(scope, " ")
			for _, s := range scopes {
				principal.Scopes[s] = true
			}
		}
	}

	return principal, nil
}

func fetchJWK(oidc *RemoteOidcAuthenticator) error {
	oidcConfig, err := oidc.GetConfiguration()
	if err != nil {
		return fmt.Errorf("error fetching OIDC configuration: %w", err)
	}

	oidc.JwksURI = oidcConfig.JWKsURI
	jwks, err := oidc.GetKeys()
	if err != nil {
		return fmt.Errorf("error fetching OIDC keys: %w", err)
	}

	oidc.JWKs = jwks

	return nil
}

func (oidc *RemoteOidcAuthenticator) GetKeys() (*keyfunc.JWKS, error) {
	jwks, err := keyfunc.Get(oidc.JwksURI, keyfunc.Options{
		Client:          oidc.httpClient,
		RefreshInterval: jwkRefreshInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("error fetching keys from %v: %w", oidc.JwksURI, err)
	}
	return jwks, nil
}

func (oidc *RemoteOidcAuthenticator) GetConfiguration() (*authn.OidcConfig, error) {
	wellKnown := strings.TrimSuffix(oidc.IssuerURLs[0], "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequest("GET", wellKnown, nil)
	if err != nil {
		return nil, fmt.Errorf("error forming request to get OIDC: %w", err)
	}

	res, err := oidc.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error getting OIDC: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code getting OIDC: %v", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	oidcConfig := &authn.OidcConfig{}
	if err := json.Unmarshal(body, oidcConfig); err != nil {
		return nil, fmt.Errorf("failed parsing document: %w", err)
	}

	if oidcConfig.Issuer == "" {
		return nil, errors.New("missing issuer value")
	}

	if oidcConfig.JWKsURI == "" {
		return nil, errors.New("missing jwks_uri value")
	}
	return oidcConfig, nil
}

func (oidc *RemoteOidcAuthenticator) Close() {
	oidc.JWKs.EndBackground()
}
