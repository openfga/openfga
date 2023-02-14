package storage

import (
	"context"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
)

// ContextTracerWrapper is a wrapper around a datastore that passes a new
// context to the underlying datastore methods. It must be the first wrapper
// around the datastore if traces are to work properly.
type ContextTracerWrapper struct {
	OpenFGADatastore
}

var _ OpenFGADatastore = (*ContextTracerWrapper)(nil)

func NewContextWrapper(inner OpenFGADatastore) *ContextTracerWrapper {
	return &ContextTracerWrapper{inner}
}

// queryContext returns a new context (not a child context) with a timeout and
// the same span data as the supplied context.
func queryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	span := trace.SpanFromContext(ctx)
	return trace.ContextWithSpan(context.Background(), span), func() {}
}

func (c *ContextTracerWrapper) Close() {
	c.OpenFGADatastore.Close()
}

func (c *ContextTracerWrapper) Read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (TupleIterator, error) {
	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.OpenFGADatastore.Read(queryCtx, store, tupleKey)
}

func (c *ContextTracerWrapper) ReadPage(ctx context.Context, store string, tupleKey *openfgapb.TupleKey, opts PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.OpenFGADatastore.ReadPage(queryCtx, store, tupleKey, opts)
}

func (c *ContextTracerWrapper) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.OpenFGADatastore.ReadUserTuple(queryCtx, store, tupleKey)
}

func (c *ContextTracerWrapper) ReadUsersetTuples(ctx context.Context, store string, filter ReadUsersetTuplesFilter) (TupleIterator, error) {
	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.OpenFGADatastore.ReadUsersetTuples(queryCtx, store, filter)
}

func (c *ContextTracerWrapper) ReadStartingWithUser(ctx context.Context, store string, opts ReadStartingWithUserFilter) (TupleIterator, error) {
	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.OpenFGADatastore.ReadStartingWithUser(queryCtx, store, opts)
}
