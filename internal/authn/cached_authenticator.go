package authn

import (
	"context"
	"time"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"

	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

const PrefixAuthnTokenCache = "AT"

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
		return nil, ErrMissingBearerToken
	}

	cacheKey := authnTokenCacheKey(authHeader)
	if cached := c.cache.Get(cacheKey); cached != nil {
		if entry, ok := cached.(*AuthClaimsCacheEntry); ok {
			return entry.Claims, nil
		}
	}

	claims, err := c.delegate.Authenticate(requestContext)
	if err != nil {
		return nil, err
	}

	c.cache.Set(cacheKey, &AuthClaimsCacheEntry{Claims: claims}, c.ttl)

	return claims, nil
}

func (c *CachedAuthenticator) Close() {
	c.cache.Stop()
	c.delegate.Close()
}

func authnTokenCacheKey(token string) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(PrefixAuthnTokenCache)
	builder.EncodeString(token)
	return builder.Key()
}
