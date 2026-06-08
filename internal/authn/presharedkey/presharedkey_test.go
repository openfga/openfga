package presharedkey

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/openfga/openfga/internal/authn"
)

func ctxWithBearer(token string) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestNewPresharedKeyAuthenticator(t *testing.T) {
	t.Run("rejects_empty_key_list", func(t *testing.T) {
		_, err := NewPresharedKeyAuthenticator(nil)
		require.Error(t, err)
	})

	t.Run("accepts_single_key", func(t *testing.T) {
		pka, err := NewPresharedKeyAuthenticator([]string{"key1"})
		require.NoError(t, err)
		require.Len(t, pka.ValidKeys, 1)
	})

	t.Run("accepts_multiple_keys", func(t *testing.T) {
		pka, err := NewPresharedKeyAuthenticator([]string{"key1", "key2", "key3"})
		require.NoError(t, err)
		require.Len(t, pka.ValidKeys, 3)
	})
}

func TestAuthenticate(t *testing.T) {
	pka, err := NewPresharedKeyAuthenticator([]string{"key1", "key2"})
	require.NoError(t, err)

	t.Run("missing_bearer_token", func(t *testing.T) {
		_, err := pka.Authenticate(context.Background())
		require.ErrorIs(t, err, authn.ErrMissingBearerToken)
	})

	t.Run("valid_first_key", func(t *testing.T) {
		claims, err := pka.Authenticate(ctxWithBearer("key1"))
		require.NoError(t, err)
		require.NotNil(t, claims)
	})

	t.Run("valid_second_key", func(t *testing.T) {
		claims, err := pka.Authenticate(ctxWithBearer("key2"))
		require.NoError(t, err)
		require.NotNil(t, claims)
	})

	t.Run("wrong_key", func(t *testing.T) {
		_, err := pka.Authenticate(ctxWithBearer("not-a-key"))
		require.ErrorIs(t, err, authn.ErrUnauthenticated)
	})

	t.Run("empty_token", func(t *testing.T) {
		_, err := pka.Authenticate(ctxWithBearer(""))
		// AuthFromMD treats empty bearer as missing.
		require.Error(t, err)
	})

	t.Run("prefix_of_valid_key_rejected", func(t *testing.T) {
		_, err := pka.Authenticate(ctxWithBearer("key"))
		require.ErrorIs(t, err, authn.ErrUnauthenticated)
	})

	t.Run("longer_than_valid_key_rejected", func(t *testing.T) {
		_, err := pka.Authenticate(ctxWithBearer("key1extra"))
		require.ErrorIs(t, err, authn.ErrUnauthenticated)
	})
}
