package authcontext

import (
	"context"
)

type ctxKey string

const authClaimsContextKey = ctxKey("auth-claims")
const skipAuthz = ctxKey("skip-authz-key")

// AuthClaims contains claims that are included in OIDC standard claims. https://openid.net/specs/openid-connect-core-1_0.html#IDToken
type AuthClaims struct {
	Subject  string
	Scopes   map[string]bool
	ClientID string
}

// ContextWithAuthClaims injects the provided AuthClaims into the parent context.
func ContextWithAuthClaims(parent context.Context, claims *AuthClaims) context.Context {
	return context.WithValue(parent, authClaimsContextKey, claims)
}

// AuthClaimsFromContext extracts the AuthClaims from the provided ctx (if any).
func AuthClaimsFromContext(ctx context.Context) (*AuthClaims, bool) {
	claims, ok := ctx.Value(authClaimsContextKey).(*AuthClaims)
	if !ok {
		return nil, false
	}

	return claims, true
}

// ContextWithSkipAuthzCheck attaches whether to skip authz check to the parent context.
func ContextWithSkipAuthzCheck(parent context.Context, skipAuthzCheck bool) context.Context {
	return context.WithValue(parent, skipAuthz, skipAuthzCheck)
}

// SkipAuthzCheckFromContext returns whether the authorize check can be skipped.
func SkipAuthzCheckFromContext(ctx context.Context) bool {
	isSkipped, ok := ctx.Value(skipAuthz).(bool)
	return isSkipped && ok
}
