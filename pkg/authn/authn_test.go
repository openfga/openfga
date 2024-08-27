package authn

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextWithAuthClaims(t *testing.T) {
	claims := AuthClaims{
		Subject:  "openfga client",
		Scopes:   map[string]bool{"offline_access": true, "read": true, "write": true, "delete": true},
		ClientID: "openfga",
	}
	ctx := ContextWithAuthClaims(context.Background(), &claims)
	claimsInContext, value := AuthClaimsFromContext(ctx)
	require.Equal(t, claims, *claimsInContext)
	require.True(t, value)
}

func TestAuthClaimsFromContext(t *testing.T) {
	ctx := context.Background()
	claims, value := AuthClaimsFromContext(ctx)
	require.Nil(t, claims)
	require.False(t, value)
}
