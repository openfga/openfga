package authn

import (
	"context"
	"crypto/sha256"
	"maps"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"

	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

const prefixAuthnTokenCache = "AT"

var _ storage.CacheItem = (*AuthClaimsCacheEntry)(nil)

type AuthClaimsCacheEntry struct {
	Claims *authclaims.AuthClaims
}

func (a *AuthClaimsCacheEntry) CacheEntityType() string {
	return "authn_token"
}

type CachedAuthenticator struct {
	delegate Authenticator
	cache    storage.InMemoryCache[any]
	ttl      time.Duration
}

func NewCachedAuthenticator(delegate Authenticator, cache storage.InMemoryCache[any], ttl time.Duration) *CachedAuthenticator {
	return &CachedAuthenticator{
		delegate: delegate,
		cache:    cache,
		ttl:      ttl,
	}
}

func (c *CachedAuthenticator) Authenticate(requestContext context.Context) (*authclaims.AuthClaims, error) {
	authHeader, err := grpcauth.AuthFromMD(requestContext, "Bearer")
	if err != nil {
		return c.delegate.Authenticate(requestContext)
	}

	cacheKey := authnTokenCacheKey(authHeader)
	if cached := c.cache.Get(cacheKey); cached != nil {
		if entry, ok := cached.(*AuthClaimsCacheEntry); ok {
			return cloneClaims(entry.Claims), nil
		}
	}

	claims, err := c.delegate.Authenticate(requestContext)
	if err != nil {
		return nil, err
	}

	ttl := c.effectiveTTL(authHeader)
	if ttl > 0 {
		c.cache.Set(cacheKey, &AuthClaimsCacheEntry{Claims: cloneClaims(claims)}, ttl)
	}

	return claims, nil
}

func (c *CachedAuthenticator) Close() {
	c.cache.Stop()
	c.delegate.Close()
}

// effectiveTTL returns min(configured TTL, time until JWT expiry).
// If the token is not a JWT or has no exp claim, the configured TTL is used.
func (c *CachedAuthenticator) effectiveTTL(rawToken string) time.Duration {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(rawToken, jwt.MapClaims{})
	if err != nil {
		return c.ttl
	}

	exp, err := token.Claims.GetExpirationTime()
	if err != nil || exp == nil {
		return c.ttl
	}

	remaining := time.Until(exp.Time)
	if remaining <= 0 {
		return 0
	}

	if remaining < c.ttl {
		return remaining
	}

	return c.ttl
}

func cloneClaims(src *authclaims.AuthClaims) *authclaims.AuthClaims {
	return &authclaims.AuthClaims{
		Subject:  src.Subject,
		ClientID: src.ClientID,
		Scopes:   maps.Clone(src.Scopes),
	}
}

func authnTokenCacheKey(token string) keys.Key {
	hash := sha256.Sum256([]byte(token))

	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(prefixAuthnTokenCache)
	builder.EncodeBytes(hash[:])
	return builder.Key()
}
