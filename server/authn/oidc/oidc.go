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
	"github.com/openfga/openfga/server/authn"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RemoteOidcAuthenticator struct {
	IssuerURL string
	Audience  string

	JwksURI string
	JWKs    *keyfunc.JWKS

	httpClient *http.Client
}

var (
	jwkRefreshInterval, _ = time.ParseDuration("48h")

	errInvalidAudience = status.Error(codes.Code(openfgapb.AuthErrorCode_auth_failed_invalid_audience), "invalid audience")
	errInvalidClaims   = status.Error(codes.Code(openfgapb.AuthErrorCode_invalid_claims), "invalid claims")
	errInvalidIssuer   = status.Error(codes.Code(openfgapb.AuthErrorCode_auth_failed_invalid_issuer), "invalid issuer")
	errInvalidSubject  = status.Error(codes.Code(openfgapb.AuthErrorCode_auth_failed_invalid_subject), "invalid subject")
	errInvalidToken    = status.Error(codes.Code(openfgapb.AuthErrorCode_auth_failed_invalid_bearer_token), "invalid bearer token")
)

var _ authn.Authenticator = (*RemoteOidcAuthenticator)(nil)
var _ authn.OIDCAuthenticator = (*RemoteOidcAuthenticator)(nil)

func NewRemoteOidcAuthenticator(issuerURL, audience string) (*RemoteOidcAuthenticator, error) {
	oidc := &RemoteOidcAuthenticator{
		IssuerURL:  issuerURL,
		Audience:   audience,
		httpClient: retryablehttp.New().StandardClient(),
	}
	err := oidc.fetchKeys()
	if err != nil {
		return nil, err
	}
	return oidc, nil
}

func (oidc *RemoteOidcAuthenticator) Authenticate(requestContext context.Context) (*authn.AuthClaims, error) {
	authHeader, err := grpcAuth.AuthFromMD(requestContext, "Bearer")
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

	if ok := claims.VerifyIssuer(oidc.IssuerURL, true); !ok {
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
		Client:          retryablehttp.New().StandardClient(),
		RefreshInterval: jwkRefreshInterval,
	})
	if err != nil {
		return nil, errors.Errorf("Error fetching keys from %v: %v", oidc.JwksURI, err)
	}
	return jwks, nil
}

func (oidc *RemoteOidcAuthenticator) GetConfiguration() (*authn.OidcConfig, error) {
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

	if res.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected status code getting OIDC: %v", res.StatusCode)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Errorf("error reading response body: %v", err)
	}

	oidcConfig := &authn.OidcConfig{}
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
