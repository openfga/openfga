package check

import (
	"context"
	"sync"

	authzGraph "github.com/openfga/language/pkg/go/graph"
)

type CheckResolver interface {
	ResolveCheck(ctx context.Context, req *Request) (*Response, error)
	GetModel() *AuthorizationModelGraph
	ResolveUnion(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode, visited *sync.Map) (*Response, error)
}
