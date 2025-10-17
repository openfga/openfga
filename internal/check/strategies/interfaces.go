package strategies

import (
	"context"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/pkg/storage"
)

type strategyHandler func(context.Context, *check.Request, storage.TupleKeyIterator, chan requestMsg)
