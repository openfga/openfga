package storage

import (
	"context"
	"sync/atomic"
	"time"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ctxKey string

const (
	DBCounterCtxKey ctxKey = "db_counter"
)

type DBCounter struct {
	Reads  atomic.Int32
	Writes atomic.Int32
}

// StorageWrapper is a wrapper around any OpenFGADatastore and is used to record metadata around how the underlying
// implementation is being used.
//
// Currently, we are using it to record the number of reads and writes that are performed. Note that the values for the
// reads and writes may not be accurate as we are only measuring calls that flow through the interface, and, such a call
// may involve any number of database accesses under the hood.
type StorageWrapper struct {
	storage OpenFGADatastore
}

func NewStorageWrapper(storage OpenFGADatastore) *StorageWrapper {
	return &StorageWrapper{storage: storage}
}

func (w *StorageWrapper) Read(ctx context.Context, store string, tuple *openfgapb.TupleKey) (TupleIterator, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.Read(ctx, store, tuple)
}

func (w *StorageWrapper) ListObjectsByType(ctx context.Context, store, objectType string) (ObjectIterator, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ListObjectsByType(ctx, store, objectType)
}

func (w *StorageWrapper) ReadPage(ctx context.Context, store string, tuple *openfgapb.TupleKey, opts PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadPage(ctx, store, tuple, opts)
}

func (w *StorageWrapper) Write(ctx context.Context, store string, deletes Deletes, writes Writes) error {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Writes.Add(1)

	return w.storage.Write(ctx, store, deletes, writes)
}

func (w *StorageWrapper) ReadUserTuple(ctx context.Context, store string, tuple *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadUserTuple(ctx, store, tuple)
}

func (w *StorageWrapper) ReadUsersetTuples(ctx context.Context, store string, tuple *openfgapb.TupleKey) (TupleIterator, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadUsersetTuples(ctx, store, tuple)
}

func (w *StorageWrapper) ReadStartingWithUser(ctx context.Context, store string, filter ReadStartingWithUserFilter) (TupleIterator, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadStartingWithUser(ctx, store, filter)
}

func (w *StorageWrapper) ReadByStore(ctx context.Context, store string, opts PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadByStore(ctx, store, opts)
}

func (w *StorageWrapper) MaxTuplesInWriteOperation() int {
	return w.storage.MaxTuplesInWriteOperation()
}

func (w *StorageWrapper) ReadAuthorizationModel(ctx context.Context, store, id string) (*openfgapb.AuthorizationModel, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadAuthorizationModel(ctx, store, id)
}

func (w *StorageWrapper) ReadAuthorizationModels(ctx context.Context, store string, options PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadAuthorizationModels(ctx, store, options)
}

func (w *StorageWrapper) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.FindLatestAuthorizationModelID(ctx, store)
}

func (w *StorageWrapper) ReadTypeDefinition(ctx context.Context, store, id string, objectType string) (*openfgapb.TypeDefinition, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadTypeDefinition(ctx, store, id, objectType)
}

func (w *StorageWrapper) MaxTypesInTypeDefinition() int {
	return w.storage.MaxTypesInTypeDefinition()
}

func (w *StorageWrapper) WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Writes.Add(1)

	return w.storage.WriteAuthorizationModel(ctx, store, model)
}

func (w *StorageWrapper) CreateStore(ctx context.Context, store *openfgapb.Store) (*openfgapb.Store, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Writes.Add(1)

	return w.storage.CreateStore(ctx, store)
}

func (w *StorageWrapper) DeleteStore(ctx context.Context, id string) error {
	return w.storage.DeleteStore(ctx, id)
}

func (w *StorageWrapper) GetStore(ctx context.Context, id string) (*openfgapb.Store, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.GetStore(ctx, id)
}

func (w *StorageWrapper) ListStores(ctx context.Context, opts PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ListStores(ctx, opts)
}

func (w *StorageWrapper) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Writes.Add(1)

	return w.storage.WriteAssertions(ctx, store, modelID, assertions)
}

func (w *StorageWrapper) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadAssertions(ctx, store, modelID)
}

func (w *StorageWrapper) ReadChanges(ctx context.Context, store, objectType string, opts PaginationOptions, horizonOffset time.Duration) ([]*openfgapb.TupleChange, []byte, error) {
	ctx.Value(DBCounterCtxKey).(*DBCounter).Reads.Add(1)

	return w.storage.ReadChanges(ctx, store, objectType, opts, horizonOffset)
}

func (w *StorageWrapper) IsReady(ctx context.Context) (bool, error) {
	return w.storage.IsReady(ctx)
}

func (w *StorageWrapper) Close(ctx context.Context) error {
	return w.storage.Close(ctx)
}
