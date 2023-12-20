package storagewrappers

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/pkg/storage"
)

// ContextTracerWrapper is a wrapper around a datastore that passes a new
// context to the underlying datastore methods.
// This is so that if the original context gets cancelled (e.g by the client), the underlying database connection isn't closed.
// So, we let outstanding queries run their course even if the original context gets cancelled to avoid database connection churning.
//
// ContextTracerWrapper must be the first wrapper around the datastore if traces are to work properly.
type ContextTracerWrapper struct {
	storage.OpenFGADatastore
}

var _ storage.OpenFGADatastore = (*ContextTracerWrapper)(nil)

func NewContextWrapper(inner storage.OpenFGADatastore) *ContextTracerWrapper {
	return &ContextTracerWrapper{inner}
}

// queryContext returns a new context (not a child context) with a timeout and
// the same span data as the supplied context.
func queryContext(ctx context.Context) context.Context {
	span := trace.SpanFromContext(ctx)
	return trace.ContextWithSpan(context.Background(), span)
}

func (c *ContextTracerWrapper) Close() {
	c.OpenFGADatastore.Close()
}

func (c *ContextTracerWrapper) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey) (storage.TupleIterator, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.Read(queryCtx, store, tupleKey)
}

func (c *ContextTracerWrapper) ReadPage(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, opts storage.PaginationOptions) ([]*openfgav1.Tuple, []byte, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.ReadPage(queryCtx, store, tupleKey, opts)
}

func (c *ContextTracerWrapper) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey) (*openfgav1.Tuple, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.ReadUserTuple(queryCtx, store, tupleKey)
}

func (c *ContextTracerWrapper) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.ReadUsersetTuples(queryCtx, store, filter)
}

func (c *ContextTracerWrapper) ReadStartingWithUser(ctx context.Context, store string, opts storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.ReadStartingWithUser(queryCtx, store, opts)
}
