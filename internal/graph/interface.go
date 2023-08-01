//go:generate mockgen -source interface.go -destination ./mock_resolver.go -package graph CheckResolver

package graph

import "context"

// CheckResolver represents an interface that can be implemented to provide recursive resolution
// of a Check.
type CheckResolver interface {
	ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error)
}
