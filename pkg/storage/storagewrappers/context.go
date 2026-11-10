package storagewrappers

import (
	"context"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

// ContextTracerWrapper is a wrapper for a datastore that introduces a new context to the underlying datastore methods.
// Its purpose is to prevent the closure of the underlying database connection in case the original context is cancelled,
// such as when a client cancels the context. This ensures that ongoing queries are allowed to complete even if the
// original context is cancelled, helping to avoid unnecessary database connection churn.
type ContextTracerWrapper struct {
	storage.OpenFGADatastore
	queryTimeout time.Duration
}

var _ storage.OpenFGADatastore = (*ContextTracerWrapper)(nil)

type ContextTracerWrapperOpt func(*ContextTracerWrapper)

// WithContextWrapperQueryTimeout sets how long a Querier statement, including reading its rows, may run before
// its context is cancelled. Values <= 0 are ignored, leaving the default of [config.DefaultRequestTimeout].
func WithContextWrapperQueryTimeout(timeout time.Duration) ContextTracerWrapperOpt {
	return func(c *ContextTracerWrapper) {
		if timeout > 0 {
			c.queryTimeout = timeout
		}
	}
}

// NewContextWrapper creates a new instance of [ContextTracerWrapper], wrapping the specified datastore. It is crucial
// for [ContextTracerWrapper] to be the first wrapper around the datastore for traces to function correctly.
func NewContextWrapper(inner storage.OpenFGADatastore, opts ...ContextTracerWrapperOpt) *ContextTracerWrapper {
	c := &ContextTracerWrapper{
		OpenFGADatastore: inner,
		queryTimeout:     config.DefaultRequestTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// queryContext generates a new context that is independent of the provided
// context timeout.
func queryContext(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

// Close ensures proper cleanup and closure of resources associated with the OpenFGADatastore.
func (c *ContextTracerWrapper) Close() {
	c.OpenFGADatastore.Close()
}

// Querier see [storage.RelationshipTupleReader].Querier. Statements ignore the request's cancellation and
// deadline, and are instead bounded by the wrapper's query timeout.
func (c *ContextTracerWrapper) Querier(consistency openfgav1.ConsistencyPreference) adapter.Querier {
	inner := c.OpenFGADatastore.Querier(consistency)
	if inner == nil {
		return nil
	}
	return &contextQuerier{Querier: inner, timeout: c.queryTimeout}
}

type contextQuerier struct {
	adapter.Querier
	timeout time.Duration
}

// Execute holds the timeout's context until the returned Rows are exhausted or closed, since cancelling it
// earlier would abort the rows.
func (q *contextQuerier) Execute(ctx context.Context, stmt *query.Statement) (adapter.Rows, error) {
	queryCtx, cancel := context.WithTimeout(queryContext(ctx), q.timeout)
	rows, err := q.Querier.Execute(queryCtx, stmt)
	if err != nil {
		cancel()
		return nil, err
	}
	return &contextRows{Rows: rows, cancel: cancel}, nil
}

type contextRows struct {
	adapter.Rows
	cancel context.CancelFunc
}

func (r *contextRows) Next() bool {
	ok := r.Rows.Next()
	if !ok {
		r.cancel()
	}
	return ok
}

func (r *contextRows) Close() error {
	defer r.cancel()
	return r.Rows.Close()
}

// Read see [storage.RelationshipTupleReader.ReadUserTuple].
func (c *ContextTracerWrapper) Read(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadOptions) (storage.TupleIterator, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.Read(queryCtx, store, filter, options)
}

// ReadPage see [storage.RelationshipTupleReader.ReadPage].
func (c *ContextTracerWrapper) ReadPage(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.ReadPage(queryCtx, store, filter, options)
}

// ReadUserTuple see [storage.RelationshipTupleReader].ReadUserTuple.
func (c *ContextTracerWrapper) ReadUserTuple(ctx context.Context, store string, filter storage.ReadUserTupleFilter, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.ReadUserTuple(queryCtx, store, filter, options)
}

// ReadUsersetTuples see [storage.RelationshipTupleReader].ReadUsersetTuples.
func (c *ContextTracerWrapper) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.ReadUsersetTuples(queryCtx, store, filter, options)
}

// ReadStartingWithUser see [storage.RelationshipTupleReader].ReadStartingWithUser.
func (c *ContextTracerWrapper) ReadStartingWithUser(ctx context.Context, store string, opts storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	queryCtx := queryContext(ctx)

	return c.OpenFGADatastore.ReadStartingWithUser(queryCtx, store, opts, options)
}
