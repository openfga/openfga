package check

import (
	"context"
	"sync"

	"github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/pkg/storage"
	"golang.org/x/sync/errgroup"
)

type CheckResolver interface {
	ResolveCheck(context.Context, *Request) (*Response, error)
	ResolveUnion(context.Context, *Request, *graph.WeightedAuthorizationModelNode, *sync.Map) (*Response, error)
	ResolveEdge(context.Context, *Request, *graph.WeightedAuthorizationModelEdge, *sync.Map) (*Response, error)
}

type GroupStrategy interface {
	Resolve(ctx context.Context, req *Request, edges []*graph.WeightedAuthorizationModelEdge, operation string, out chan<- ResponseMsg, pool *errgroup.Group, visited *sync.Map)
}

type EdgeStrategy interface {
	Userset(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, _ *sync.Map) (*Response, error)
	TTU(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, _ *sync.Map) (*Response, error)
}
