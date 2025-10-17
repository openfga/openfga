package check

import (
	"context"

	authzGraph "github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/pkg/storage"
)

type Strategy interface {
	Userset(context.Context, *Request, *authzGraph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator) (*Response, error)
	TTU(context.Context, *Request, *authzGraph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator) (*Response, error)
}
