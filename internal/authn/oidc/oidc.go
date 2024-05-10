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
	MainIssuer    string
	IssuerAliases []string
	Audience      string
	Roles         []string
	ClaimName     string
	ClaimValues   []string

	JwksURI string
	JWKs    *keyfunc.JWKS

	httpClient *http.Client
}

var (
	jwkRefreshInterval = 48 * time.Hour

	errInvalidAudience      = status.Error(codes.Code(openfgav1.AuthErrorCode_auth_failed_invalid_audience), "invalid audience")
	errInvalidClaims        = status.Error(codes.Code(openfgav1.AuthErrorCode_invalid_claims), "invalid claims")
	errInvalidIssuer        = status.Error(codes.Code(openfgav1.AuthErrorCode_auth_failed_invalid_issuer), "invalid issuer")
	errInvalidSubject       = status.Error(codes.Code(openfgav1.AuthErrorCode_auth_failed_invalid_subject), "invalid subject")
	errInvalidToken         = status.Error(codes.Code(openfgav1.AuthErrorCode_auth_failed_invalid_bearer_token), "invalid bearer token")
	errInvalidRole          = status.Error(codes.Code(openfgav1.AuthErrorCode_invalid_claims), "invalid role")
	errInvalidRequiredClaim = status.Error(codes.Code(openfgav1.AuthErrorCode_invalid_claims), "invalid required claims")

	fetchJWKs = fetchJWK
)

var _ authn.Authenticator = (*RemoteOidcAuthenticator)(nil)
var _ authn.OIDCAuthenticator = (*RemoteOidcAuthenticator)(nil)

func NewRemoteOidcAuthenticator(mainIssuer string, issuerAliases []string, audience string, roles []string, claimName string, claimValues []string) (*RemoteOidcAuthenticator, error) {
	client := retryablehttp.NewClient()
	client.Logger = nil
	oidc := &RemoteOidcAuthenticator{
		MainIssuer:    mainIssuer,
		IssuerAliases: issuerAliases,
		Audience:      audience,
		Roles:         roles,
		ClaimName:     claimName,
		ClaimValues:   claimValues,
		httpClient:    client.StandardClient(),
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

	validIssuers := []string{
		oidc.MainIssuer,
	}
	validIssuers = append(validIssuers, oidc.IssuerAliases...)

	ok = slices.ContainsFunc(validIssuers, func(issuer string) bool {
		return claims.VerifyIssuer(issuer, true)
	})

	if !ok {
		return nil, errInvalidIssuer
	}

	if ok := claims.VerifyAudience(oidc.Audience, true); !ok {
		return nil, errInvalidAudience
	}

	// optional role check
	ok = len(oidc.Roles) == 0 || slices.ContainsFunc(oidc.Roles, func(role string) bool {
		return verifyClaimContains(claims, "roles", role, true)
	})

	if !ok {
		return nil, errInvalidRole
	}

	// optional claim check
	if len(oidc.ClaimName) > 0 {
		if len(oidc.ClaimValues) > 0 {
			// if required values are specified, check if the claim contains any of the required values
			ok = slices.ContainsFunc(oidc.ClaimValues, func(value string) bool {
				return verifyClaimContains(claims, oidc.ClaimName, value, true)
			})
		} else {
			// if no required values are specified, just check if the claim exists
			ok = len(readClaimValues(claims, oidc.ClaimName)) > 0
		}

		if !ok {
			return nil, errInvalidRequiredClaim
		}
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

func verifyClaimContains(claims jwt.MapClaims, claim string, cmp string, req bool) bool {
	var claimValues = readClaimValues(claims, claim)
	if len(claimValues) == 0 {
		return !req
	}
	return slices.ContainsFunc(claimValues, func(item string) bool {
		return item == cmp
	})
}

func readClaimValues(claims jwt.MapClaims, claim string) []string {
	var claimValues []string
	if claimKey, ok := claims[claim]; ok {
		switch v := claimKey.(type) {
		case string:
			claimValues = append(claimValues, v)
		case []string:
			claimValues = v
		case []interface{}:
			for _, a := range v {
				vs, ok := a.(string)
				if ok {
					claimValues = append(claimValues, vs)
				}
			}
		}
	}
	return claimValues
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
	wellKnown := strings.TrimSuffix(oidc.MainIssuer, "/") + "/.well-known/openid-configuration"
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
