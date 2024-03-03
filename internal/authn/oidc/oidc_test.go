package oidc

import (
	"context"
	"github.com/openfga/openfga/internal/authn"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRemoteOidcAuthenticator_Authenticate(t *testing.T) {

	testCases := []struct {
		name            string
		authenticator   *RemoteOidcAuthenticator
		requestContext  context.Context
		expectedOutcome error
	}{
		{
			name:            "When the authorization header is missing from the gRPC metadata of the request, returns 'missing bearer token' error.",
			authenticator:   &RemoteOidcAuthenticator{},
			requestContext:  context.Background(),
			expectedOutcome: authn.ErrMissingBearerToken,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			//when
			_, err := testCase.authenticator.Authenticate(testCase.requestContext)
			//then
			require.Equal(t, authn.ErrMissingBearerToken, err)
		})
	}
}
