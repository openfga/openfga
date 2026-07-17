package check

import (
	"context"
	"sync"

	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/storage"
)

type CheckResolver interface {
	ResolveCheck(context.Context, *Request) (*Response, error)
	ResolveUnion(context.Context, *Request, *graph.WeightedAuthorizationModelNode, *sync.Map) (*Response, error)
	ResolveUnionEdges(context.Context, *Request, []LogicalEdge, *sync.Map) (*Response, error)
	ResolveIntersectionEdges(context.Context, *Request, []LogicalEdge) (*Response, error)
	ResolveExclusionEdges(context.Context, *Request, []LogicalEdge) (*Response, error)
	ResolveEdge(context.Context, *Request, *graph.WeightedAuthorizationModelEdge, *sync.Map) (*Response, error)
}

type GroupStrategy interface {
	Union(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error)
	Intersection(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error)
	Exclusion(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error)
}

type EdgeStrategy interface {
	Userset(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, _ *sync.Map) (*Response, error)
	TTU(ctx context.Context, req *Request, edge *graph.WeightedAuthorizationModelEdge, iter storage.TupleKeyIterator, _ *sync.Map) (*Response, error)
}
