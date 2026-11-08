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

	"github.com/MicahParks/keyfunc/v2"
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
	JWKs    *keyfunc.JWKS

	httpClient *http.Client
}

var (
	jwkRefreshInterval  = 48 * time.Hour
	jwkRefreshRateLimit = 1 * time.Minute

	errInvalidClaims = status.Error(codes.Code(openfgav1.AuthErrorCode_invalid_claims), "invalid claims")
	fetchJWKs        = fetchJWK
)

var (
	ErrMissingIssuer   = errors.New("oidc: issuer must be set")
	ErrMissingAudience = errors.New("oidc: audience must be set")
)

var _ authn.Authenticator = (*RemoteOidcAuthenticator)(nil)
var _ authn.OIDCAuthenticator = (*RemoteOidcAuthenticator)(nil)

func NewRemoteOidcAuthenticator(mainIssuer string, issuerAliases []string, audience string, subjects []string, clientIDClaims []string) (*RemoteOidcAuthenticator, error) {
	// both issuer and audience are StringOrURI values (RFC 7519 §4.1.1, §4.1.3); whitespace is valid, so only reject strictly empty
	if mainIssuer == "" {
		return nil, ErrMissingIssuer
	}
	if audience == "" {
		return nil, ErrMissingAudience
	}

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

	// constructor enforces non-empty Audience; unconditional to make the invariant explicit
	options = append(options, jwt.WithAudience(oidc.Audience))

	jwtParser := jwt.NewParser(options...)

	token, err := jwtParser.Parse(authHeader, oidc.verificationKeys)
	if err != nil && tokenHasNoKid(authHeader) {
		// A token without a kid never reaches the unknown-kid refresh of the JWKS, so after a key
		// rotation it would fail until the next scheduled refresh. Refresh here (rate limited) and retry once.
		if refreshErr := oidc.JWKs.Refresh(context.Background(), keyfunc.RefreshOptions{}); refreshErr == nil {
			token, err = jwtParser.Parse(authHeader, oidc.verificationKeys)
		}
	}
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

// verificationKeys returns the key, or the keys, that may have signed the token. A token with a `kid`
// header is matched to the key with that ID. Some identity providers issue tokens without a `kid`;
// for those, every key of the JWKS that is allowed to verify the token is returned, and the token is
// accepted if any of them verifies it.
func (oidc *RemoteOidcAuthenticator) verificationKeys(token *jwt.Token) (any, error) {
	kid, _ := token.Header["kid"].(string)
	if kid != "" {
		return oidc.JWKs.Keyfunc(token)
	}

	var verificationKeys []jwt.VerificationKey
	for keyID := range oidc.JWKs.ReadOnlyKeys() {
		// Look the key up by ID so that the "use" and "alg" checks of the JWKS apply to it, the same way
		// as for a token that names the key.
		candidate := &jwt.Token{Method: token.Method, Header: map[string]any{"kid": keyID, "alg": token.Header["alg"]}}
		key, err := oidc.JWKs.Keyfunc(candidate)
		if err != nil {
			continue
		}
		verificationKeys = append(verificationKeys, key)
	}
	if len(verificationKeys) == 0 {
		return nil, errors.New("oidc: the JWT has no kid and the JWKS has no key that can verify it")
	}
	return jwt.VerificationKeySet{Keys: verificationKeys}, nil
}

func tokenHasNoKid(rawToken string) bool {
	token, _, err := jwt.NewParser().ParseUnverified(rawToken, jwt.MapClaims{})
	if err != nil {
		return false
	}
	kid, _ := token.Header["kid"].(string)
	return kid == ""
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
		Client:            oidc.httpClient,
		RefreshInterval:   jwkRefreshInterval,
		RefreshUnknownKID: true,
		RefreshRateLimit:  jwkRefreshRateLimit,
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
