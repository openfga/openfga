package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"github.com/MicahParks/keyfunc"
	"github.com/golang-jwt/jwt/v4"
	"github.com/openfga/openfga/internal/authn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"log"
	"strings"
	"testing"
)

func TestRemoteOidcAuthenticator_Authenticate(t *testing.T) {
	t.Run("When the authorization header is missing from the gRPC metadata of the request, returns 'missing bearer token' error.", func(t *testing.T) {
		//given
		authenticator := &RemoteOidcAuthenticator{}
		//when
		_, err := authenticator.Authenticate(context.Background())
		////then
		require.Equal(t, authn.ErrMissingBearerToken, err)
	})
	testCases := []struct {
		testDescription string
		testSetup       func() (*RemoteOidcAuthenticator, context.Context, error)
		expectedError   string
	}{
		{
			testDescription: "When JWT and JWK kid don't match, returns 'invalid bearer token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup("kid_1", "kid_2", "", "", jwt.MapClaims{}, nil)
			},
			expectedError: "invalid bearer token",
		},
		{
			testDescription: "When token is signed using different public/private key pairs, returns  'invalid bearer token'",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				privateKey, _ := generateJWTSignatureKeys()
				return quickConfigSetup("kid_1", "kid_1", "", "", jwt.MapClaims{}, privateKey)
			},
			expectedError: "invalid bearer token",
		},
		{
			testDescription: "When token's issuer does not match the one provided in the server configuration, MUST return 'invalid issuer' error.",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"",
					jwt.MapClaims{
						"iss": "wrong_issuer",
					},
					nil,
				)
			},
			expectedError: "invalid issuer",
		},
		{
			testDescription: "When token's audience does not match the one provided in the server configuration, MUST return 'invalid audience' error.",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
					jwt.MapClaims{
						"iss": "right_issuer",
						"aud": "wrong_audience",
					},
					nil,
				)
			},
		},
		{
			testDescription: "When the subject of the token is not a string, MUST return 'invalid subject' error.",
			testSetup: func() (*RemoteOidcAuthenticator, context.Context, error) {
				return quickConfigSetup(
					"kid_1",
					"kid_1",
					"right_issuer",
					"right_audience",
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
	}

	for _, testC := range testCases {
		t.Run(testC.testDescription, func(t *testing.T) {
			//given
			oidc, requestContext, err := testC.testSetup()
			//when
			_, err = oidc.Authenticate(requestContext)
			//then
			require.Contains(t, err.Error(), testC.expectedError)
		})
	}

	// Success testcases
	t.Run("When the token is valid, it MUST return the token subject and its associated scopes.", func(t *testing.T) {
		// given
		scopes := "offline_access read write delete"
		oidc, requestContext, err := quickConfigSetup(
			"kid_1",
			"kid_1",
			"right_issuer",
			"right_audience",
			jwt.MapClaims{
				"iss":   "right_issuer",
				"aud":   "right_audience",
				"sub":   "openfga client",
				"scope": scopes,
			},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		// when
		authClaims, err := oidc.Authenticate(requestContext)
		// then
		require.Nil(t, err)
		require.Equal(t, "openfga client", authClaims.Subject)
		scopesList := strings.Split(scopes, " ")
		require.Equal(t, len(scopesList), len(authClaims.Scopes))
		for _, scope := range scopesList {
			_, ok := authClaims.Scopes[scope]
			require.True(t, ok)
		}

	})
}

// quickConfigSetup sets up a basic configuration for testing purposes.
func quickConfigSetup(jwkKid, jwtKid, issuerURL, audience string, jwtClaims jwt.MapClaims, privateKeyOverride *rsa.PrivateKey) (*RemoteOidcAuthenticator, context.Context, error) {
	// Generate JWT signature keys
	privateKey, publicKey := generateJWTSignatureKeys()
	if privateKeyOverride != nil {
		privateKey = privateKeyOverride
	}
	// assign mocked JWKS fetching function to global function
	fetchJWKs = fetchKeysMock(publicKey, jwkKid)

	// Initialize RemoteOidcAuthenticator
	oidc, err := NewRemoteOidcAuthenticator(issuerURL, audience)
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

// fetchKeysMock returns a function that sets up a mock JWKS
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
