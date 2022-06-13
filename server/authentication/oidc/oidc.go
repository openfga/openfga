package oidc

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc"
	"github.com/go-errors/errors"
	"github.com/golang-jwt/jwt/v4"
	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/openfga/openfga/pkg/retryablehttp"

	"github.com/openfga/openfga/server/authentication"
)

type RemoteOidcAuthenticator struct {
	IssuerURL string
	Audience  string

	JwksURI string
	JWKs    *keyfunc.JWKS

	httpClient retryablehttp.RetryableHTTPClient
}

var (
	JWKRefreshInterval, _ = time.ParseDuration("48h")
)

var _ authentication.Authenticator = (*RemoteOidcAuthenticator)(nil)
var _ authentication.OidcAuthenticator = (*RemoteOidcAuthenticator)(nil)

func NewRemoteOidcAuthenticator(issuerURL, audience string) (*RemoteOidcAuthenticator, error) {
	oidc := &RemoteOidcAuthenticator{
		IssuerURL:  issuerURL,
		Audience:   audience,
		httpClient: retryablehttp.RetryableHTTPClient{},
	}
	err := oidc.fetchKeys()
	if err != nil {
		return nil, err
	}
	return oidc, nil
}

func (oidc *RemoteOidcAuthenticator) Authenticate(requestContext context.Context, requestParameters any) (*authentication.AuthClaims, error) {
	authHeader, err := grpcAuth.AuthFromMD(requestContext, "Bearer")
	if err != nil {
		return nil, errors.New("missing bearer token")
	}

	jwtParser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))

	token, err := jwtParser.Parse(authHeader, func(token *jwt.Token) (any, error) {
		return oidc.JWKs.Keyfunc(token)
	})

	if err != nil {
		return nil, errors.Errorf("error parsing token: %v", err)
	}

	if !token.Valid {
		return nil, errors.Errorf("invalid token")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.Errorf("invalid claims")
	}

	if ok := claims.VerifyIssuer(oidc.IssuerURL, true); !ok {
		return nil, errors.New("invalid issuer")
	}

	if ok := claims.VerifyAudience(oidc.Audience, true); !ok {
		return nil, errors.New("invalid audience")
	}

	// optional subject
	var subject = ""
	if subjectClaim, ok := claims["sub"]; ok {
		if subject, ok = subjectClaim.(string); !ok {
			return nil, errors.New("invalid subject")
		}
	}

	principal := &authentication.AuthClaims{
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

func (oidc *RemoteOidcAuthenticator) fetchKeys() error {
	oidcConfig, err := oidc.GetConfiguration()
	if err != nil {
		return errors.Errorf("Error fetching OIDC configuration: %v", err)
	}

	oidc.JwksURI = oidcConfig.JWKsURI

	jwks, err := oidc.GetKeys()
	if err != nil {
		return errors.Errorf("Error fetching OIDC keys: %v", err)
	}

	oidc.JWKs = jwks

	return nil
}

func (oidc *RemoteOidcAuthenticator) GetKeys() (*keyfunc.JWKS, error) {
	jwks, err := keyfunc.Get(oidc.JwksURI, keyfunc.Options{
		Client:          retryablehttp.NewClient().StandardClient(),
		RefreshInterval: JWKRefreshInterval,
	})
	if err != nil {
		return nil, errors.Errorf("Error fetching keys from %v: %v", oidc.JwksURI, err)
	}
	return jwks, nil
}

func (oidc *RemoteOidcAuthenticator) GetConfiguration() (*authentication.OidcConfig, error) {
	wellKnown := strings.TrimSuffix(oidc.IssuerURL, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequest("GET", wellKnown, nil)
	if err != nil {
		return nil, errors.Errorf("error forming request to get OIDC: %v", err)
	}

	res, err := oidc.httpClient.Do(req)
	if err != nil {
		return nil, errors.Errorf("error getting OIDC: %v", err)
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Errorf("error reading response body: %v", err)
	}

	if res.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected status code getting OIDC: %v", res.StatusCode)
	}

	oidcConfig := &authentication.OidcConfig{}
	if err := json.Unmarshal(body, oidcConfig); err != nil {
		return nil, errors.Errorf("failed parsing document: %v", err)
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
