package check

//go:generate mockgen -source interface.go -destination ./mock_resolver.go -package check

import (
	"context"
	"sync"

	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/storage"
)

// CheckResolver resolves the query for a node or multiple edges into an allowed/denied result.
// Strategies can delegate to it to resolve the smaller sub-queries they may break a Check into.
type CheckResolver interface {
	ResolveUnion(context.Context, *Request, *graph.WeightedAuthorizationModelNode, *sync.Map) (*Response, error)
	ResolveUnionEdges(context.Context, *Request, []LogicalEdge, *sync.Map) (*Response, error)
	ResolveIntersectionEdges(context.Context, *Request, []LogicalEdge) (*Response, error)
	ResolveExclusionEdges(context.Context, *Request, []LogicalEdge) (*Response, error)
}

// GroupStrategy resolves a GroupEdge (edges bundled under one operator node) as a set operation.
type GroupStrategy interface {
	Union(context.Context, *Request, *GroupEdge) (*Response, error)
	Intersection(context.Context, *Request, *GroupEdge) (*Response, error)
	Exclusion(context.Context, *Request, *GroupEdge) (*Response, error)
}

// EdgeStrategy resolves a single userset or TTU edge by expanding its tuples.
type EdgeStrategy interface {
	Userset(context.Context, *Request, *graph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator, *sync.Map) (*Response, error)
	TTU(context.Context, *Request, *graph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator, *sync.Map) (*Response, error)
}
