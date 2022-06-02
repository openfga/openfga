package authentication

import (
	"context"
)

type principalContextKey string

const (
	contextPrincipalKey = principalContextKey("openfga:principal")
)

// WithAuthPrincipal adds the provided principal into the provided context
func WithAuthPrincipal(ctx context.Context, principal *AuthClaims) context.Context {
	return context.WithValue(ctx, contextPrincipalKey, principal)
}

// AuthPrincipalFromContext gets the principal from the context
func AuthPrincipalFromContext(ctx context.Context) (*AuthClaims, bool) {
	val := ctx.Value(contextPrincipalKey)
	if val == nil {
		return nil, false
	}

	principal, ok := val.(*AuthClaims)
	return principal, ok
}
