package mocks

import (
	"context"
	"time"

	"github.com/openfga/openfga/pkg/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// slowDataStorage is a proxy to the actual ds except the Reads are slow time by the readTuplesDelay
// This allows simulating list objection condition that times out
type slowDataStorage struct {
	readTuplesDelay time.Duration
	ds              storage.OpenFGADatastore
}

// NewMockSlowDataStorage returns a wrapper of a datastore that adds artificial delays into the reads of tuples
func NewMockSlowDataStorage(ds storage.OpenFGADatastore, readTuplesDelay time.Duration) storage.OpenFGADatastore {
	return &slowDataStorage{
		readTuplesDelay: readTuplesDelay,
		ds:              ds,
	}
}

func (m *slowDataStorage) Close() {}

func (m *slowDataStorage) ListObjectsByType(ctx context.Context, store string, objectType string) (storage.ObjectIterator, error) {
	time.Sleep(m.readTuplesDelay)
	return m.ds.ListObjectsByType(ctx, store, objectType)
}

func (m *slowDataStorage) Read(ctx context.Context, store string, key *openfgapb.TupleKey) (storage.TupleIterator, error) {
	time.Sleep(m.readTuplesDelay)
	return m.ds.Read(ctx, store, key)
}

func (m *slowDataStorage) ReadPage(ctx context.Context, store string, key *openfgapb.TupleKey, paginationOptions storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	time.Sleep(m.readTuplesDelay)
	return m.ds.ReadPage(ctx, store, key, paginationOptions)
}

func (m *slowDataStorage) ReadChanges(ctx context.Context, store, objectType string, paginationOptions storage.PaginationOptions, horizonOffset time.Duration) ([]*openfgapb.TupleChange, []byte, error) {
	return m.ds.ReadChanges(ctx, store, objectType, paginationOptions, horizonOffset)
}

func (m *slowDataStorage) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	return m.ds.Write(ctx, store, deletes, writes)
}

func (m *slowDataStorage) ReadUserTuple(ctx context.Context, store string, key *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	time.Sleep(m.readTuplesDelay)
	return m.ds.ReadUserTuple(ctx, store, key)
}

func (m *slowDataStorage) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	time.Sleep(m.readTuplesDelay)
	return m.ds.ReadUsersetTuples(ctx, store, filter)
}

func (m *slowDataStorage) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {
	time.Sleep(m.readTuplesDelay)
	return m.ds.ReadStartingWithUser(ctx, store, filter)
}

func (m *slowDataStorage) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgapb.AuthorizationModel, error) {
	return m.ds.ReadAuthorizationModel(ctx, store, id)
}

func (m *slowDataStorage) ReadAuthorizationModels(ctx context.Context, store string, options storage.PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error) {
	return m.ds.ReadAuthorizationModels(ctx, store, options)
}

func (m *slowDataStorage) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {
	return m.ds.FindLatestAuthorizationModelID(ctx, store)
}

func (m *slowDataStorage) WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error {
	return m.ds.WriteAuthorizationModel(ctx, store, model)
}

func (m *slowDataStorage) CreateStore(ctx context.Context, newStore *openfgapb.Store) (*openfgapb.Store, error) {
	return m.ds.CreateStore(ctx, newStore)
}

func (m *slowDataStorage) DeleteStore(ctx context.Context, id string) error {
	return m.ds.DeleteStore(ctx, id)
}

func (m *slowDataStorage) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	return m.ds.WriteAssertions(ctx, store, modelID, assertions)
}

func (m *slowDataStorage) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	return m.ds.ReadAssertions(ctx, store, modelID)
}

func (m *slowDataStorage) MaxTuplesPerWrite() int {
	return m.ds.MaxTuplesPerWrite()
}

func (m *slowDataStorage) MaxTypesPerAuthorizationModel() int {
	return m.ds.MaxTypesPerAuthorizationModel()
}

func (m *slowDataStorage) GetStore(ctx context.Context, storeID string) (*openfgapb.Store, error) {
	return m.ds.GetStore(ctx, storeID)
}

func (m *slowDataStorage) ListStores(ctx context.Context, paginationOptions storage.PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	return m.ds.ListStores(ctx, paginationOptions)
}

func (m *slowDataStorage) IsReady(ctx context.Context) (bool, error) {
	return m.ds.IsReady(ctx)
}
