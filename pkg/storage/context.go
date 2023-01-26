package storage

import (
	"context"
	"time"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var (
	queryTimeout = 500 * time.Millisecond
	tracer       = otel.Tracer("openfga/pkg/storage")
)

// ContextTracerWrapper needs to be the first wrapper around the inner OpenFGADatastore.
type ContextTracerWrapper struct {
	inner OpenFGADatastore
}

var _ OpenFGADatastore = (*ContextTracerWrapper)(nil)

func NewContextTracerWrapper(inner OpenFGADatastore) *ContextTracerWrapper {
	return &ContextTracerWrapper{
		inner: inner,
	}
}

// queryContext returns a new context (not a child context) with a timeout and
// the same span data as the supplied context.
func queryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	spanCtx := trace.SpanContextFromContext(ctx)
	return trace.ContextWithSpanContext(context.Background(), spanCtx), func() {}
	//return context.WithTimeout(trace.ContextWithSpanContext(context.Background(), spanCtx), queryTimeout)
}

func (c *ContextTracerWrapper) Close() {
	c.inner.Close()
}

func (c *ContextTracerWrapper) ListObjectsByType(ctx context.Context, store string, objectType string) (ObjectIterator, error) {
	ctx, span := tracer.Start(ctx, "ListObjectsByType")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ListObjectsByType(queryCtx, store, objectType)
}

func (c *ContextTracerWrapper) Read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "Read")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.Read(queryCtx, store, tupleKey)
}

func (c *ContextTracerWrapper) ReadPage(ctx context.Context, store string, tupleKey *openfgapb.TupleKey, opts PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx, span := tracer.Start(ctx, "ReadPage")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ReadPage(queryCtx, store, tupleKey, opts)
}

func (c *ContextTracerWrapper) Write(ctx context.Context, store string, deletes Deletes, writes Writes) error {
	ctx, span := tracer.Start(ctx, "Write")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.Write(queryCtx, store, deletes, writes)
}

func (c *ContextTracerWrapper) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	ctx, span := tracer.Start(ctx, "ReadUserTuple")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ReadUserTuple(queryCtx, store, tupleKey)
}

func (c *ContextTracerWrapper) ReadUsersetTuples(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "ReadUsersetTuples")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ReadUsersetTuples(queryCtx, store, tupleKey)
}

func (c *ContextTracerWrapper) ReadStartingWithUser(ctx context.Context, store string, opts ReadStartingWithUserFilter) (TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "ReadStartingWithUser")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ReadStartingWithUser(queryCtx, store, opts)
}

func (c *ContextTracerWrapper) MaxTuplesPerWrite() int {
	return c.inner.MaxTuplesPerWrite()
}

func (c *ContextTracerWrapper) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgapb.AuthorizationModel, error) {
	ctx, span := tracer.Start(ctx, "ReadAuthorizationModel")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ReadAuthorizationModel(queryCtx, store, modelID)
}

func (c *ContextTracerWrapper) ReadAuthorizationModels(ctx context.Context, store string, opts PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error) {
	ctx, span := tracer.Start(ctx, "ReadAuthorizationModels")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ReadAuthorizationModels(queryCtx, store, opts)
}

func (c *ContextTracerWrapper) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {
	ctx, span := tracer.Start(ctx, "FindLatestAuthorizationModelID")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.FindLatestAuthorizationModelID(queryCtx, store)
}

func (c *ContextTracerWrapper) ReadTypeDefinition(ctx context.Context, store, modelID, objectType string) (*openfgapb.TypeDefinition, error) {
	ctx, span := tracer.Start(ctx, "ReadTypeDefinition")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ReadTypeDefinition(queryCtx, store, modelID, objectType)
}

func (c *ContextTracerWrapper) MaxTypesPerAuthorizationModel() int {
	return c.inner.MaxTypesPerAuthorizationModel()
}

func (c *ContextTracerWrapper) WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error {
	ctx, span := tracer.Start(ctx, "WriteAuthorizationModel")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.WriteAuthorizationModel(queryCtx, store, model)
}

func (c *ContextTracerWrapper) CreateStore(ctx context.Context, store *openfgapb.Store) (*openfgapb.Store, error) {
	ctx, span := tracer.Start(ctx, "CreateStore")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.CreateStore(queryCtx, store)
}

func (c *ContextTracerWrapper) GetStore(ctx context.Context, id string) (*openfgapb.Store, error) {
	ctx, span := tracer.Start(ctx, "GetStore")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.GetStore(queryCtx, id)
}

func (c *ContextTracerWrapper) ListStores(ctx context.Context, opts PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	ctx, span := tracer.Start(ctx, "ListStores")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ListStores(queryCtx, opts)
}

func (c *ContextTracerWrapper) DeleteStore(ctx context.Context, id string) error {
	ctx, span := tracer.Start(ctx, "DeleteStore")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.DeleteStore(queryCtx, id)
}

func (c *ContextTracerWrapper) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	ctx, span := tracer.Start(ctx, "WriteAssertions")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.WriteAssertions(queryCtx, store, modelID, assertions)
}

func (c *ContextTracerWrapper) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	ctx, span := tracer.Start(ctx, "ReadAssertions")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ReadAssertions(queryCtx, store, modelID)
}

func (c *ContextTracerWrapper) ReadChanges(ctx context.Context, store, objectTypeFilter string, opts PaginationOptions, horizonOffset time.Duration) ([]*openfgapb.TupleChange, []byte, error) {
	ctx, span := tracer.Start(ctx, "ReadChanges")
	defer span.End()

	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.ReadChanges(queryCtx, store, objectTypeFilter, opts, horizonOffset)
}

func (c *ContextTracerWrapper) IsReady(ctx context.Context) (bool, error) {
	queryCtx, cancel := queryContext(ctx)
	defer cancel()

	return c.inner.IsReady(queryCtx)
}
