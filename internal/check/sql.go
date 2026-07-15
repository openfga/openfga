package check

import (
	"context"
	"errors"
	"sync"

	"github.com/openfga/language/pkg/go/graph"
	"golang.org/x/sync/errgroup"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
)

var (
	ErrSqlUnsupported = errors.New("datastore does not support SQL algorithms")
)

var _ GroupStrategy = &SqlStrategy{}

type SqlStrategy struct {
	datastore storage.RelationshipTupleReader
	model     *modelgraph.AuthorizationModelGraph
}

func NewSql(model *modelgraph.AuthorizationModelGraph, datastore storage.RelationshipTupleReader) *SqlStrategy {
	return &SqlStrategy{
		model:     model,
		datastore: datastore,
	}
}

func (s *SqlStrategy) Resolve(ctx context.Context, req *Request, edges []*graph.WeightedAuthorizationModelEdge, operation string, out chan<- ResponseMsg, _ *errgroup.Group, _ *sync.Map) {
	builder := s.datastore.Builder(req.GetConsistency())
	if builder == nil {
		out <- ResponseMsg{Edges: edges, Err: ErrSqlUnsupported}
		return
	}

	// For now, we assume all edges passed in are weight-1 edges.
	res, err := s.weight1(ctx, req, edges, operation)
	out <- ResponseMsg{Res: res, Edges: edges, Err: err}
	return
}

func (s *SqlStrategy) weight1(ctx context.Context, req *Request, edges []*graph.WeightedAuthorizationModelEdge, operation string) (*Response, error) {
	return nil, nil
}
