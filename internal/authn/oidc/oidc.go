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

	"github.com/MicahParks/keyfunc/v3"
	jwt "github.com/golang-jwt/jwt/v5"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/hashicorp/go-retryablehttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/authn"
	"github.com/openfga/openfga/pkg/authclaims"
)

type RemoteOidcAuthenticator struct {
	MainIssuer     string
	IssuerAliases  []string
	Audience       string
	Subjects       []string
	ClientIDClaims []string

	JwksURI string
	JWKs    keyfunc.Keyfunc
	cancel  context.CancelFunc

	httpClient *http.Client
}

var (
	jwkRefreshInterval = 48 * time.Hour

	errInvalidClaims = status.Error(codes.Code(openfgav1.AuthErrorCode_invalid_claims), "invalid claims")
	fetchJWKs        = fetchJWK
)

var _ authn.Authenticator = (*RemoteOidcAuthenticator)(nil)
var _ authn.OIDCAuthenticator = (*RemoteOidcAuthenticator)(nil)

func NewRemoteOidcAuthenticator(mainIssuer string, issuerAliases []string, audience string, subjects []string, clientIDClaims []string) (*RemoteOidcAuthenticator, error) {
	client := retryablehttp.NewClient()
	client.Logger = nil
	oidc := &RemoteOidcAuthenticator{
		MainIssuer:     mainIssuer,
		IssuerAliases:  issuerAliases,
		Audience:       audience,
		Subjects:       subjects,
		httpClient:     client.StandardClient(),
		ClientIDClaims: clientIDClaims,
	}

	// Client ID is:
	// 1. If the user has set it in configuration, use that
	// 2, If the user has not set it in configuration, use the following as default:
	// 2.a. Use `azp`: the OpenID standard https://openid.net/specs/openid-connect-core-1_0.html#IDToken
	// 3.b. Use `client_id` in RFC9068 https://www.rfc-editor.org/rfc/rfc9068.html#name-data-structure
	if len(oidc.ClientIDClaims) == 0 {
		oidc.ClientIDClaims = []string{"azp", "client_id"}
	}

	err := fetchJWKs(oidc)
	if err != nil {
		return nil, err
	}
	return oidc, nil
}

func (oidc *RemoteOidcAuthenticator) Authenticate(requestContext context.Context) (*authclaims.AuthClaims, error) {
	authHeader, err := grpcauth.AuthFromMD(requestContext, "Bearer")
	if err != nil {
		return nil, authn.ErrMissingBearerToken
	}

	options := []jwt.ParserOption{
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuedAt(),
		jwt.WithExpirationRequired(),
	}

	if strings.TrimSpace(oidc.Audience) != "" {
		options = append(options, jwt.WithAudience(oidc.Audience))
	}

	jwtParser := jwt.NewParser(options...)

	token, err := jwtParser.Parse(authHeader, KeyfuncWithFallback(authHeader, oidc.JWKs))
	if err != nil || !token.Valid {
		return nil, errInvalidClaims
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
		v := jwt.NewValidator(jwt.WithIssuer(issuer))
		err := v.Validate(claims)
		return err == nil
	})

	if !ok {
		return nil, errInvalidClaims
	}

	if len(oidc.Subjects) > 0 {
		ok = slices.ContainsFunc(oidc.Subjects, func(subject string) bool {
			v := jwt.NewValidator(jwt.WithSubject(subject))
			err := v.Validate(claims)
			return err == nil
		})
		if !ok {
			return nil, errInvalidClaims
		}
	}

	// optional subject
	var subject = ""
	if subjectClaim, ok := claims["sub"]; ok {
		if subject, ok = subjectClaim.(string); !ok {
			return nil, errInvalidClaims
		}
	}

	clientID := ""
	for _, claimString := range oidc.ClientIDClaims {
		clientID, ok = claims[claimString].(string)
		if ok {
			break
		}
	}

	principal := &authclaims.AuthClaims{
		Subject:  subject,
		Scopes:   make(map[string]bool),
		ClientID: clientID,
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

func (oidc *RemoteOidcAuthenticator) GetKeys() (keyfunc.Keyfunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	oidc.cancel = cancel
	jwks, err := keyfunc.NewDefaultOverrideCtx(ctx, []string{oidc.JwksURI}, keyfunc.Override{
		RefreshInterval: jwkRefreshInterval,
	})
	if err != nil {
		cancel()
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

func KeyfuncWithFallback(authHeader string, jwks keyfunc.Keyfunc) jwt.Keyfunc {
	return func(t *jwt.Token) (interface{}, error) {
		kid, _ := t.Header["kid"].(string)

		if kid != "" {
			return jwks.Keyfunc(t)
		}

		var allKeys jwt.VerificationKeySet
		allKeys, err := jwks.VerificationKeySet(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to fetch all keys: %w", err)
		}

		var lastErr error
		for _, key := range allKeys.Keys {
			tk := jwt.NewParser()
			_, err := tk.Parse(authHeader, func(_ *jwt.Token) (interface{}, error) {
				return key, nil
			})
			if err == nil {
				return key, nil
			}
			lastErr = err
		}

		return nil, fmt.Errorf("verification failed using all keys: %w", lastErr)
	}
}

func (oidc *RemoteOidcAuthenticator) Close() {
	if oidc.cancel != nil {
		oidc.cancel()
	}
}
