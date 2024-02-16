//go:generate mockgen -source interface.go -destination ./mock_check_resolver.go -package graph CheckResolver

package graph

import "context"

type CheckResolverCloser func()

// CheckResolver represents an interface that can be implemented to provide recursive resolution
// of a Check.
type CheckResolver interface {
	ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error)
	Close()
}
