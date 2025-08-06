package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/openfga/openfga/internal/authn"
)

func TestRemoteOidcAuthenticator_Authenticate(t *testing.T) {
	t.Run("when_the_authorization_header_is_missing_from_the_gRPC_metadata_of_the_request,_returns_'missing_bearer_token'_error", func(t *testing.T) {
		authenticator := &RemoteOidcAuthenticator{}
		_, err := authenticator.Authenticate(context.Background())
		require.Equal(t, authn.ErrMissingBearerToken, err)
	})
	errorTestCases := []struct {
		testDescription string
		testSetup       func() (*RemoteOidcAuthenticator, context.Context, Config, error)
		expectedError   string
	}{
		{
			testDescription: "when_the_token_has_expired,_return_'invalid_bearer_token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_1",
					jwtKid:         "kid_1",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       []string{"openfga client"},
					clientIDClaims: []string{"azp"},
					jwtClaims: jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
						"sub": "openfga client",
						"exp": time.Now().Add(-10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
			expectedError: "invalid claims",
		},
		{
			testDescription: "when_no_expiration_set,_return_'invalid_bearer_token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_1",
					jwtKid:         "kid_1",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       []string{"openfga client"},
					clientIDClaims: []string{"azp"},
					jwtClaims: jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
						"sub": "openfga client",
					},
					privateKeyOverride: nil,
				})
			},
			expectedError: "invalid claims",
		},
		{
			testDescription: "when_the_JWT_contains_a_future_'iat',_return_'invalid_bearer_token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_1",
					jwtKid:         "kid_1",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       []string{"openfga client"},
					clientIDClaims: []string{"azp"},
					jwtClaims: jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
						"sub": "openfga client",
						"iat": time.Now().Add(10 * time.Minute).Unix(),
						"exp": time.Now().Add(100 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
			expectedError: "invalid claims",
		},
		{
			testDescription: "when_JWT_and_JWK_kid_don't_match,_returns_'invalid_bearer_token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_1",
					jwtKid:         "kid_2",
					issuerURL:      "",
					audience:       "",
					issuerAliases:  nil,
					subjects:       []string{"openfga client"},
					clientIDClaims: []string{"azp"},
					jwtClaims: jwt.MapClaims{
						"exp": time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
			expectedError: "invalid claims",
		},
		{
			testDescription: "when_token_is_signed_using_different_public/private_key_pairs,_returns__'invalid_bearer_token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				privateKey, _ := generateJWTSignatureKeys()
				return quickConfigSetup(Config{
					jwkKid:         "kid_1",
					jwtKid:         "kid_1",
					issuerURL:      "",
					audience:       "",
					issuerAliases:  nil,
					subjects:       []string{"openfga client"},
					clientIDClaims: []string{"azp"},
					jwtClaims: jwt.MapClaims{
						"exp": time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: privateKey,
				})
			},
			expectedError: "invalid claims",
		},
		{
			testDescription: "when_token's_issuer_does_not_match_the_one_provided_in_the_server_configuration,_MUST_return_'invalid_issuer'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_1",
					jwtKid:         "kid_1",
					issuerURL:      "right_issuer",
					audience:       "",
					issuerAliases:  nil,
					subjects:       []string{"openfga client"},
					clientIDClaims: []string{"azp"},
					jwtClaims: jwt.MapClaims{
						"iss": "wrong_issuer",
						"exp": time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
			expectedError: "invalid claims",
		},
		{
			testDescription: "when_token's_audience_does_not_match_the_one_provided_in_the_server_configuration,_MUST_return_'invalid_audience'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_1",
					jwtKid:         "kid_1",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       []string{"openfga client"},
					clientIDClaims: []string{"azp"},
					jwtClaims: jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "wrong_audience",
						"exp": time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
			expectedError: "invalid claims",
		},
		{
			testDescription: "when_the_subject_of_the_token_is_not_a_string,_MUST_return_'invalid_subject'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_1",
					jwtKid:         "kid_1",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       []string{"openfga client"},
					clientIDClaims: []string{"azp"},
					jwtClaims: jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
						"sub": 12,
						"exp": time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
			expectedError: "invalid claims",
		},
		{
			testDescription: "when_the_subject_of_the_token_is_a_string_but_subject_is_not_valid,_MUST_return_'invalid_subject'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_1",
					jwtKid:         "kid_1",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       []string{"valid-sub-1", "valid-sub-2"},
					clientIDClaims: []string{"azp"},
					jwtClaims: jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
						"sub": "openfga client",
						"exp": time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
			expectedError: "invalid claims",
		},
	}

	for _, testC := range errorTestCases {
		t.Run(testC.testDescription, func(t *testing.T) {
			if testC.expectedError == "" {
				t.Fatal("this suite is to test error cases and this test didn't have an error expectation")
			}
			oidc, requestContext, _, _ := testC.testSetup()
			_, err := oidc.Authenticate(requestContext)
			require.Contains(t, err.Error(), testC.expectedError)
		})
	}

	// Success testcases

	azpClientIDClaims := []string{"azp"}
	clientID := "client-id"
	customClientIDClaims := []string{"custom-id"}
	customClientID := "custom-client-id"
	scopes := "offline_access read write delete"
	successTestCases := []struct {
		testDescription string
		testSetup       func() (*RemoteOidcAuthenticator, context.Context, Config, error)
	}{
		{
			testDescription: "empty_audience",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_2",
					jwtKid:         "kid_2",
					issuerURL:      "right_issuer",
					audience:       "",
					issuerAliases:  nil,
					subjects:       nil,
					clientIDClaims: []string{"custom_claim", "custom_claim_2"},
					jwtClaims: jwt.MapClaims{
						"iss":            "right_issuer",
						"aud":            "",
						"sub":            "some-user",
						"custom_claim_2": customClientID,
						"scope":          scopes,
						"exp":            time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
		},
		{
			testDescription: "when_the_token_is_valid,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_2",
					jwtKid:         "kid_2",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       []string{"openfga client"},
					clientIDClaims: azpClientIDClaims,
					jwtClaims: jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"sub":   "openfga client",
						"azp":   clientID,
						"scope": scopes,
						"exp":   time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
		},
		{
			testDescription: "when_the_token_is_valid_with_issuer_alias,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_2",
					jwtKid:         "kid_2",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  []string{"issuer_alias"},
					subjects:       []string{"openfga client"},
					clientIDClaims: customClientIDClaims,
					jwtClaims: jwt.MapClaims{
						"iss":       "issuer_alias",
						"aud":       "right_audience",
						"sub":       "openfga client",
						"custom-id": customClientID,
						"scope":     scopes,
						"exp":       time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
		},
		{
			testDescription: "when_issuer_alias_set_token_is_valid_with_orig_issuer,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_2",
					jwtKid:         "kid_2",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  []string{"issuer_alias"},
					subjects:       []string{"openfga client"},
					clientIDClaims: azpClientIDClaims,
					jwtClaims: jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"sub":   "openfga client",
						"azp":   clientID,
						"scope": scopes,
						"exp":   time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
		},
		{
			testDescription: "when_validation_subjects_is_empty_and_pass_any_sub,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_2",
					jwtKid:         "kid_2",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       nil,
					clientIDClaims: azpClientIDClaims,
					jwtClaims: jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"sub":   "some-user",
						"azp":   clientID,
						"scope": scopes,
						"exp":   time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
		},
		{
			testDescription: "when_client_id_claims_is_empty_use_azp_for_client_id",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_2",
					jwtKid:         "kid_2",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       nil,
					clientIDClaims: nil,
					jwtClaims: jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"sub":   "some-user",
						"azp":   clientID,
						"scope": scopes,
						"exp":   time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
		},
		{
			testDescription: "when_client_id_claims_is_empty_and_azp_is_missing_use_client_id_for_client_id",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_2",
					jwtKid:         "kid_2",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       nil,
					clientIDClaims: nil,
					jwtClaims: jwt.MapClaims{
						"iss":       "right_issuer",
						"aud":       "right_audience",
						"sub":       "some-user",
						"client_id": clientID,
						"scope":     scopes,
						"exp":       time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
		},
		{
			testDescription: "when_client_id_claims_is_set_use_first_existing_claim_as_client_id",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_2",
					jwtKid:         "kid_2",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  nil,
					subjects:       nil,
					clientIDClaims: []string{"custom_claim", "custom_claim_2"},
					jwtClaims: jwt.MapClaims{
						"iss":            "right_issuer",
						"aud":            "right_audience",
						"sub":            "some-user",
						"custom_claim_2": customClientID,
						"scope":          scopes,
						"exp":            time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
		},
		{
			testDescription: "verify_token_successfully_when_jwt_header_lacks_kid",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, Config, error) {
				return quickConfigSetup(Config{
					jwkKid:         "kid_2",
					jwtKid:         "",
					issuerURL:      "right_issuer",
					audience:       "right_audience",
					issuerAliases:  []string{"issuer_alias"},
					subjects:       []string{"openfga client"},
					clientIDClaims: customClientIDClaims,
					jwtClaims: jwt.MapClaims{
						"iss":       "issuer_alias",
						"aud":       "right_audience",
						"sub":       "openfga client",
						"custom-id": customClientID,
						"scope":     scopes,
						"exp":       time.Now().Add(10 * time.Minute).Unix(),
					},
					privateKeyOverride: nil,
				})
			},
		},
	}

	for _, testC := range successTestCases {
		t.Run(testC.testDescription, func(t *testing.T) {
			oidc, requestContext, config, err := testC.testSetup()
			if err != nil {
				t.Fatal(err)
			}
			authClaims, err := oidc.Authenticate(requestContext)
			require.NoError(t, err)

			// only verify subjects when authn.oidc.subjects is not empty
			// when it is empty, it indicates any 'sub' should pass
			if len(oidc.Subjects) != 0 {
				require.Equal(t, "openfga client", authClaims.Subject)
			}

			// only verify default clientIDClaims when clientIDClaims are not set,
			// or are set to the defaults
			if config.clientIDClaims == nil || config.clientIDClaims[0] == azpClientIDClaims[0] {
				require.Equal(t, clientID, authClaims.ClientID)
			} else {
				require.Equal(t, customClientID, authClaims.ClientID)
			}

			scopesList := strings.Split(scopes, " ")
			require.Len(t, authClaims.Scopes, len(scopesList))
			for _, scope := range scopesList {
				_, ok := authClaims.Scopes[scope]
				require.True(t, ok)
			}
		})
	}
}

type Config struct {
	jwkKid             string
	jwtKid             string
	issuerURL          string
	audience           string
	issuerAliases      []string
	subjects           []string
	clientIDClaims     []string
	jwtClaims          jwt.MapClaims
	privateKeyOverride *rsa.PrivateKey
}

// quickConfigSetup sets up a basic configuration for testing purposes.
func quickConfigSetup(c Config) (*RemoteOidcAuthenticator, context.Context, Config, error) {
	// Generate JWT signature keys
	privateKey, publicKey := generateJWTSignatureKeys()
	if c.privateKeyOverride != nil {
		privateKey = c.privateKeyOverride
	}
	// assign mocked JWKS fetching function to global function
	fetchJWKs = fetchKeysMock(publicKey, c.jwkKid)

	// Initialize RemoteOidcAuthenticator
	oidc, err := NewRemoteOidcAuthenticator(c.issuerURL, c.issuerAliases, c.audience, c.subjects, c.clientIDClaims)
	if err != nil {
		return nil, nil, c, err
	}

	// Generate JWT token
	token := generateJWT(privateKey, c.jwtKid, c.jwtClaims)

	// Generate context with JWT token
	requestContext := generateContext(token)

	return oidc, requestContext, c, nil
}

func generateContext(token string) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewIncomingContext(context.Background(), md)
}

// generateJWTSignatureKeys generates a private key for signing JWT tokens
// and a corresponding public key for verifying JWT token signatures.
func generateJWTSignatureKeys() (*rsa.PrivateKey, *rsa.PublicKey) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatal("Private key cannot be created.", err.Error())
	}
	return privateKey, &privateKey.PublicKey
}

// fetchKeysMock returns a function that sets up a mock JWKS.
func fetchKeysMock(publicKey *rsa.PublicKey, kid string) func(oidc *RemoteOidcAuthenticator) error {
	// Return a function that sets up the mock JWKS with the provided kid
	return func(oidc *RemoteOidcAuthenticator) error {
		// Create a JWK Set JSON with the RSA public key
		n := base64.RawURLEncoding.EncodeToString(publicKey.N.Bytes())
		e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(publicKey.E)).Bytes())

		jwkSetJSON := fmt.Sprintf(`{
			"keys": [{
				"kty": "RSA",
				"kid": "%s",
				"use": "sig",
				"alg": "RS256",
				"n": "%s",
				"e": "%s"
			}]
		}`, kid, n, e)

		// Create keyfunc from JWK Set JSON
		jwks, err := keyfunc.NewJWKSetJSON(json.RawMessage(jwkSetJSON))
		if err != nil {
			log.Fatalf("failed to create keyfunc from JWK Set: %v", err)
		}

		oidc.JWKs = jwks
		return nil
	}
}

// generateJWT generates Json Web Tokens signed with the provided privateKey.
func generateJWT(privateKey *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	signedToken, err := token.SignedString(privateKey)
	if err != nil {
		log.Fatal("Failed to sign JWT token:", err)
	}
	return signedToken
}
