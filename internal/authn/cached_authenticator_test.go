package authn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/storage"
)

type mockAuthenticator struct {
	callCount int
	claims    *authclaims.AuthClaims
	err       error
}

func (m *mockAuthenticator) Authenticate(_ context.Context) (*authclaims.AuthClaims, error) {
	m.callCount++
	return m.claims, m.err
}

func (m *mockAuthenticator) Close() {}

func contextWithBearer(token string) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestCachedAuthenticator_CachesSuccessfulAuth(t *testing.T) {
	cache, err := storage.NewInMemoryLRUCache[any](storage.WithMaxCacheSize[any](100))
	require.NoError(t, err)
	defer cache.Stop()

	delegate := &mockAuthenticator{
		claims: &authclaims.AuthClaims{
			Subject:  "user:alice",
			ClientID: "my-client",
			Scopes:   map[string]bool{"read": true},
		},
	}

	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)

	ctx := contextWithBearer("test-token-123")

	// First call should hit the delegate
	claims, err := cached.Authenticate(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:alice", claims.Subject)
	require.Equal(t, "my-client", claims.ClientID)
	require.Equal(t, 1, delegate.callCount)

	// Second call with same token should use cache
	claims, err = cached.Authenticate(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:alice", claims.Subject)
	require.Equal(t, 1, delegate.callCount)
}

func TestCachedAuthenticator_DifferentTokensNotCached(t *testing.T) {
	cache, err := storage.NewInMemoryLRUCache[any](storage.WithMaxCacheSize[any](100))
	require.NoError(t, err)
	defer cache.Stop()

	delegate := &mockAuthenticator{
		claims: &authclaims.AuthClaims{
			Subject: "user:alice",
		},
	}

	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)

	_, err = cached.Authenticate(contextWithBearer("token-a"))
	require.NoError(t, err)
	require.Equal(t, 1, delegate.callCount)

	_, err = cached.Authenticate(contextWithBearer("token-b"))
	require.NoError(t, err)
	require.Equal(t, 2, delegate.callCount)
}

func TestCachedAuthenticator_DoesNotCacheErrors(t *testing.T) {
	cache, err := storage.NewInMemoryLRUCache[any](storage.WithMaxCacheSize[any](100))
	require.NoError(t, err)
	defer cache.Stop()

	delegate := &mockAuthenticator{
		err: ErrUnauthenticated,
	}

	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)
	ctx := contextWithBearer("bad-token")

	_, err = cached.Authenticate(ctx)
	require.Error(t, err)
	require.Equal(t, 1, delegate.callCount)

	// Should try again (not cached)
	_, err = cached.Authenticate(ctx)
	require.Error(t, err)
	require.Equal(t, 2, delegate.callCount)
}

func TestCachedAuthenticator_MissingBearerToken(t *testing.T) {
	cache, err := storage.NewInMemoryLRUCache[any](storage.WithMaxCacheSize[any](100))
	require.NoError(t, err)
	defer cache.Stop()

	delegate := &mockAuthenticator{}
	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)

	_, err = cached.Authenticate(context.Background())
	require.ErrorIs(t, err, ErrMissingBearerToken)
	require.Equal(t, 0, delegate.callCount)
}
