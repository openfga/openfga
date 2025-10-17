package strategies

import (
	"context"

	authzGraph "github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/pkg/storage"
)

type strategyHandler func(context.Context, *check.Request, *authzGraph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator, chan requestMsg)
