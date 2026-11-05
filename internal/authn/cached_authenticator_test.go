package authn

import (
	"context"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
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

func newTestCache(t *testing.T) storage.InMemoryCache[any] {
	t.Helper()
	cache, err := storage.NewInMemoryLRUCache[any](storage.WithMaxCacheSize[any](100))
	require.NoError(t, err)
	t.Cleanup(cache.Stop)
	return cache
}

func TestCachedAuthenticator_CachesSuccessfulAuth(t *testing.T) {
	cache := newTestCache(t)
	delegate := &mockAuthenticator{
		claims: &authclaims.AuthClaims{
			Subject:  "user:alice",
			ClientID: "my-client",
			Scopes:   map[string]bool{"read": true},
		},
	}

	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)
	ctx := contextWithBearer("test-token-123")

	claims, err := cached.Authenticate(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:alice", claims.Subject)
	require.Equal(t, "my-client", claims.ClientID)
	require.Equal(t, 1, delegate.callCount)

	claims, err = cached.Authenticate(ctx)
	require.NoError(t, err)
	require.Equal(t, "user:alice", claims.Subject)
	require.Equal(t, 1, delegate.callCount)
}

func TestCachedAuthenticator_DifferentTokensNotCached(t *testing.T) {
	cache := newTestCache(t)
	delegate := &mockAuthenticator{
		claims: &authclaims.AuthClaims{Subject: "user:alice"},
	}

	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)

	_, err := cached.Authenticate(contextWithBearer("token-a"))
	require.NoError(t, err)
	require.Equal(t, 1, delegate.callCount)

	_, err = cached.Authenticate(contextWithBearer("token-b"))
	require.NoError(t, err)
	require.Equal(t, 2, delegate.callCount)
}

func TestCachedAuthenticator_DoesNotCacheErrors(t *testing.T) {
	cache := newTestCache(t)
	delegate := &mockAuthenticator{err: ErrUnauthenticated}

	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)
	ctx := contextWithBearer("bad-token")

	_, err := cached.Authenticate(ctx)
	require.Error(t, err)
	require.Equal(t, 1, delegate.callCount)

	_, err = cached.Authenticate(ctx)
	require.Error(t, err)
	require.Equal(t, 2, delegate.callCount)
}

func TestCachedAuthenticator_MissingBearerDelegatesToUnderlying(t *testing.T) {
	cache := newTestCache(t)
	delegate := &mockAuthenticator{
		claims: &authclaims.AuthClaims{Subject: ""},
	}

	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)

	claims, err := cached.Authenticate(context.Background())
	require.NoError(t, err)
	require.NotNil(t, claims)
	require.Equal(t, 1, delegate.callCount)
}

func TestCachedAuthenticator_CacheHitReturnsCopy(t *testing.T) {
	cache := newTestCache(t)
	delegate := &mockAuthenticator{
		claims: &authclaims.AuthClaims{
			Subject: "user:alice",
			Scopes:  map[string]bool{"read": true},
		},
	}

	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)
	ctx := contextWithBearer("token-scopes")

	first, err := cached.Authenticate(ctx)
	require.NoError(t, err)

	first.Scopes["write"] = true

	second, err := cached.Authenticate(ctx)
	require.NoError(t, err)
	require.NotContains(t, second.Scopes, "write", "mutating returned claims must not affect cached entry")
	require.Equal(t, 1, delegate.callCount)
}

func TestCachedAuthenticator_NilClaimsFromDelegate(t *testing.T) {
	cache := newTestCache(t)
	delegate := &mockAuthenticator{claims: nil, err: ErrUnauthenticated}

	cached := NewCachedAuthenticator(delegate, cache, 5*time.Minute)

	_, err := cached.Authenticate(contextWithBearer("nil-token"))
	require.Error(t, err)
	require.Equal(t, 1, delegate.callCount)
}

func TestAuthnTokenCacheKey_DifferentTokensDifferentKeys(t *testing.T) {
	keyA := authnTokenCacheKey("token-a")
	keyB := authnTokenCacheKey("token-b")
	require.NotEqual(t, keyA, keyB)
}

func TestAuthnTokenCacheKey_SameTokenSameKey(t *testing.T) {
	key1 := authnTokenCacheKey("same-token")
	key2 := authnTokenCacheKey("same-token")
	require.Equal(t, key1, key2)
}

func TestEffectiveTTL_NonJWTToken(t *testing.T) {
	cached := &CachedAuthenticator{ttl: 5 * time.Minute}
	require.Equal(t, 5*time.Minute, cached.effectiveTTL("not-a-jwt"))
}

func TestEffectiveTTL_JWTWithFarExpiry(t *testing.T) {
	cached := &CachedAuthenticator{ttl: 5 * time.Minute}
	token := buildUnsignedJWT(t, time.Now().Add(1*time.Hour))
	ttl := cached.effectiveTTL(token)
	require.Equal(t, 5*time.Minute, ttl)
}

func TestEffectiveTTL_JWTWithNearExpiry(t *testing.T) {
	cached := &CachedAuthenticator{ttl: 5 * time.Minute}
	token := buildUnsignedJWT(t, time.Now().Add(30*time.Second))
	ttl := cached.effectiveTTL(token)
	require.True(t, ttl > 0 && ttl <= 30*time.Second,
		"expected TTL capped at ~30s, got %v", ttl)
}

func TestEffectiveTTL_JWTAlreadyExpired(t *testing.T) {
	cached := &CachedAuthenticator{ttl: 5 * time.Minute}
	token := buildUnsignedJWT(t, time.Now().Add(-10*time.Second))
	ttl := cached.effectiveTTL(token)
	require.Equal(t, time.Duration(0), ttl)
}

func buildUnsignedJWT(t *testing.T, exp time.Time) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"exp": exp.Unix(),
		"sub": "test",
	})
	s, err := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)
	return s
}
