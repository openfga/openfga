package check

import (
	"context"
	"sync"

	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/storage"
)

type CheckResolver interface {
	ResolveCheck(ctx context.Context, req *Request) (*Response, error)
	ResolveUnion(ctx context.Context, req *Request, node *authzGraph.WeightedAuthorizationModelNode, visited *sync.Map) (*Response, error)
}

type Strategy interface {
	Userset(context.Context, *Request, *authzGraph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator, *sync.Map) (*Response, error)
	TTU(context.Context, *Request, *authzGraph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator, *sync.Map) (*Response, error)
}
