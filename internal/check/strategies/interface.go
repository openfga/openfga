package strategies

import (
	"context"

	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/pkg/storage"
)

type defaultStrategyHandler func(context.Context, *check.Request, *authzGraph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator, chan requestMsg)

type weight2Handler func(context.Context, []storage.Iterator[string], chan<- *iterator.Msg)
