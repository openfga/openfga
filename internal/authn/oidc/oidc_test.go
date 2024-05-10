package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/MicahParks/keyfunc"
	"github.com/golang-jwt/jwt/v4"
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
		testSetup       func() (*RemoteOidcAuthenticator, context.Context, error)
		expectedError   string
	}{
		{
			testDescription: "when_the_token_has_expired,_return_'invalid_bearer_token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"",
					nil,
					jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
						"sub": "openfga client",
						"exp": time.Now().Add(-10 * time.Minute).Unix(),
					},
					nil,
				)
			},
			expectedError: "invalid bearer token",
		},
		{
			testDescription: "when_the_JWT_contains_a_future_'iat',_return_'invalid_bearer_token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"",
					nil,
					jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
						"sub": "openfga client",
						"iat": time.Now().Add(10 * time.Minute).Unix(),
					},
					nil,
				)
			},
			expectedError: "invalid bearer token",
		},
		{
			testDescription: "when_JWT_and_JWK_kid_don't_match,_returns_'invalid_bearer_token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup("kid_1", "kid_2", "", "", nil, nil, "", nil, jwt.MapClaims{}, nil)
			},
			expectedError: "invalid bearer token",
		},
		{
			testDescription: "when_token_is_signed_using_different_public/private_key_pairs,_returns__'invalid_bearer_token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				privateKey, _ := generateJWTSignatureKeys()
				return quickConfigSetup("kid_1", "kid_1", "", "", nil, nil, "", nil, jwt.MapClaims{}, privateKey)
			},
			expectedError: "invalid bearer token",
		},
		{
			testDescription: "when_token's_issuer_does_not_match_the_one_provided_in_the_server_configuration,_MUST_return_'invalid_issuer'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"",
					nil,
					nil,
					"",
					nil,
					jwt.MapClaims{
						"iss": "wrong_issuer",
					},
					nil,
				)
			},
			expectedError: "invalid issuer",
		},
		{
			testDescription: "when_token's_audience_does_not_match_the_one_provided_in_the_server_configuration,_MUST_return_'invalid_audience'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"",
					nil,
					jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "wrong_audience",
					},
					nil,
				)
			},
			expectedError: "invalid audience",
		},
		{
			testDescription: "when_the_subject_of_the_token_is_not_a_string,_MUST_return_'invalid_subject'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"",
					nil,
					jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
						"sub": 12,
					},
					nil,
				)
			},
			expectedError: "invalid subject",
		},
		{
			testDescription: "when_the_role_claim_is_missing_and_role_is_required,_MUST_return_'invalid_role'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					[]string{"role"},
					"",
					nil,
					jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
					},
					nil,
				)
			},
			expectedError: "invalid role",
		},
		{
			testDescription: "when_the_role_claim_contains_incorrect_role_and_role_is_required,_MUST_return_'invalid_role'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					[]string{"role"},
					"",
					nil,
					jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"roles": "admin",
					},
					nil,
				)
			},
			expectedError: "invalid role",
		},
		{
			testDescription: "when_the_role_claim_contains_incorrect_roles_and_role_is_required,_MUST_return_'invalid_role'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					[]string{"role"},
					"",
					nil,
					jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"roles": "[\"admin\",\"user\"]",
					},
					nil,
				)
			},
			expectedError: "invalid role",
		},
		{
			testDescription: "when_claim_is_required_and_claim_is_missing,_MUST_return_'invalid_claim'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"claim",
					nil,
					jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "right_audience",
					},
					nil,
				)
			},
			expectedError: "invalid required claims",
		},
		{
			testDescription: "when_claim_is_required_and_claim_contains_incorrect_value,_MUST_return_'invalid_claim'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"claim",
					[]string{"value"},
					jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"claim": "tataloco",
					},
					nil,
				)
			},
			expectedError: "invalid required claims",
		},
		{
			testDescription: "when_claim_is_required_and_claim_contains_incorrect_values,_MUST_return_'invalid_claim'_error",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"claim",
					[]string{"value"},
					jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"claim": "[\"tata\",\"loco\"]",
					},
					nil,
				)
			},
			expectedError: "invalid required claims",
		},
	}

	for _, testC := range errorTestCases {
		t.Run(testC.testDescription, func(t *testing.T) {
			if testC.expectedError == "" {
				t.Fatal("this suite is to test error cases and this test didn't have an error expectation")
			}
			oidc, requestContext, _ := testC.testSetup()
			_, err := oidc.Authenticate(requestContext)
			require.Contains(t, err.Error(), testC.expectedError)
		})
	}

	// Success testcases

	scopes := "offline_access read write delete"
	successTestCases := []struct {
		testDescription string
		testSetup       func() (*RemoteOidcAuthenticator, context.Context, error)
	}{
		{
			testDescription: "when_the_token_is_valid,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_2",
					"kid_2",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"",
					nil,
					jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"sub":   "openfga client",
						"scope": scopes,
					},
					nil,
				)
			},
		},
		{
			testDescription: "when_the_token_is_valid_with_issuer_alias,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_2",
					"kid_2",
					"right_issuer",
					"right_audience",
					[]string{"issuer_alias"},
					nil,
					"",
					nil,
					jwt.MapClaims{
						"iss":   "issuer_alias",
						"aud":   "right_audience",
						"sub":   "openfga client",
						"scope": scopes,
					},
					nil,
				)
			},
		},
		{
			testDescription: "when_the_token_is_valid_with_role_required,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_2",
					"kid_2",
					"right_issuer",
					"right_audience",
					nil,
					[]string{"role"},
					"",
					nil,
					jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"sub":   "openfga client",
						"scope": scopes,
						"roles": "role",
					},
					nil,
				)
			},
		},
		{
			testDescription: "when_the_token_is_valid_with_one_of_the_roles_required,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_2",
					"kid_2",
					"right_issuer",
					"right_audience",
					nil,
					[]string{"role", "role2"},
					"",
					nil,
					jwt.MapClaims{
						"iss":   "right_issuer",
						"aud":   "right_audience",
						"sub":   "openfga client",
						"scope": scopes,
						"roles": []string{"role", "role3"},
					},
					nil,
				)
			},
		},
		{
			testDescription: "when_the_token_is_valid_with_required_claim,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_2",
					"kid_2",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"theClaim",
					nil,
					jwt.MapClaims{
						"iss":      "right_issuer",
						"aud":      "right_audience",
						"sub":      "openfga client",
						"scope":    scopes,
						"theClaim": "",
					},
					nil,
				)
			},
		},
		{
			testDescription: "when_the_token_is_valid_with_required_claim_and_no_value,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_2",
					"kid_2",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"theClaim",
					[]string{},
					jwt.MapClaims{
						"iss":      "right_issuer",
						"aud":      "right_audience",
						"sub":      "openfga client",
						"scope":    scopes,
						"theClaim": "",
					},
					nil,
				)
			},
		},
		{
			testDescription: "when_the_token_is_valid_with_required_claim_and_value,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_2",
					"kid_2",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"theClaim",
					[]string{"required"},
					jwt.MapClaims{
						"iss":      "right_issuer",
						"aud":      "right_audience",
						"sub":      "openfga client",
						"scope":    scopes,
						"theClaim": "required",
					},
					nil,
				)
			},
		},
		{
			testDescription: "when_the_token_is_valid_with_required_claim_and_values,_it_MUST_return_the_token_subject_and_its_associated_scopes",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_2",
					"kid_2",
					"right_issuer",
					"right_audience",
					nil,
					nil,
					"theClaim",
					[]string{"required"},
					jwt.MapClaims{
						"iss":      "right_issuer",
						"aud":      "right_audience",
						"sub":      "openfga client",
						"scope":    scopes,
						"theClaim": []string{"required", "not_required"},
					},
					nil,
				)
			},
		},
	}

	for _, testC := range successTestCases {
		t.Run(testC.testDescription, func(t *testing.T) {
			oidc, requestContext, err := testC.testSetup()
			if err != nil {
				t.Fatal(err)
			}
			authClaims, err := oidc.Authenticate(requestContext)
			require.NoError(t, err)
			require.Equal(t, "openfga client", authClaims.Subject)
			scopesList := strings.Split(scopes, " ")
			require.Equal(t, len(scopesList), len(authClaims.Scopes))
			for _, scope := range scopesList {
				_, ok := authClaims.Scopes[scope]
				require.True(t, ok)
			}
		})
	}
}

// quickConfigSetup sets up a basic configuration for testing purposes.
func quickConfigSetup(jwkKid, jwtKid, issuerURL, audience string, issuerAliases []string, roles []string, claimName string, claimValues []string, jwtClaims jwt.MapClaims, privateKeyOverride *rsa.PrivateKey) (*RemoteOidcAuthenticator, context.Context, error) {
	// Generate JWT signature keys
	privateKey, publicKey := generateJWTSignatureKeys()
	if privateKeyOverride != nil {
		privateKey = privateKeyOverride
	}
	// assign mocked JWKS fetching function to global function
	fetchJWKs = fetchKeysMock(publicKey, jwkKid)

	// Initialize RemoteOidcAuthenticator
	oidc, err := NewRemoteOidcAuthenticator(issuerURL, issuerAliases, audience, roles, claimName, claimValues)
	if err != nil {
		return nil, nil, err
	}

	// Generate JWT token
	token := generateJWT(privateKey, jwtKid, jwtClaims)

	// Generate context with JWT token
	requestContext := generateContext(token)

	return oidc, requestContext, nil
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
	// Create a keyfunc with the given RSA public key and RS256 algorithm
	givenKeys := keyfunc.NewGivenRSACustomWithOptions(publicKey, keyfunc.GivenKeyOptions{
		Algorithm: "RS256",
	})
	// Return a function that sets up the mock JWKS with the provided kid
	return func(oidc *RemoteOidcAuthenticator) error {
		jwks := keyfunc.NewGiven(map[string]keyfunc.GivenKey{
			kid: givenKeys,
		})
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
