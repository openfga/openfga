package mocks

import (
	"context"
	"time"

	"github.com/openfga/openfga/pkg/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// SlowDataStorage is a proxy to the actual ds except the Reads are slow time by the sleepTime
// This allows simulating list objection condition that times out
type SlowDataStorage struct {
	sleepTime time.Duration
	ds        storage.OpenFGADatastore
}

// NewMockSlowDataStorage returns SlowDataStorage which is a proxy to the actual ds except the Reads are slow time by the sleepTime
// This allows simulating list objection condition that times out
func NewMockSlowDataStorage(ds storage.OpenFGADatastore, sleepTime time.Duration) storage.OpenFGADatastore {
	return &SlowDataStorage{
		sleepTime: sleepTime,
		ds:        ds,
	}
}

func (m *SlowDataStorage) Close() {}

func (m *SlowDataStorage) ListObjectsByType(ctx context.Context, store string, objectType string) (storage.ObjectIterator, error) {
	return m.ds.ListObjectsByType(ctx, store, objectType)
}

func (m *SlowDataStorage) Read(ctx context.Context, store string, key *openfgapb.TupleKey) (storage.TupleIterator, error) {
	time.Sleep(m.sleepTime)
	return m.ds.Read(ctx, store, key)
}

func (m *SlowDataStorage) ReadPage(ctx context.Context, store string, key *openfgapb.TupleKey, paginationOptions storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	time.Sleep(m.sleepTime)
	return m.ds.ReadPage(ctx, store, key, paginationOptions)
}

func (m *SlowDataStorage) ReadChanges(ctx context.Context, store, objectType string, paginationOptions storage.PaginationOptions, horizonOffset time.Duration) ([]*openfgapb.TupleChange, []byte, error) {
	return m.ds.ReadChanges(ctx, store, objectType, paginationOptions, horizonOffset)
}

func (m *SlowDataStorage) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	return m.ds.Write(ctx, store, deletes, writes)
}

func (m *SlowDataStorage) ReadUserTuple(ctx context.Context, store string, key *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	time.Sleep(m.sleepTime)
	return m.ds.ReadUserTuple(ctx, store, key)
}

func (m *SlowDataStorage) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	time.Sleep(m.sleepTime)
	return m.ds.ReadUsersetTuples(ctx, store, filter)
}

func (m *SlowDataStorage) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {
	time.Sleep(m.sleepTime)
	return m.ds.ReadStartingWithUser(ctx, store, filter)
}

func (m *SlowDataStorage) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgapb.AuthorizationModel, error) {
	return m.ds.ReadAuthorizationModel(ctx, store, id)
}

func (m *SlowDataStorage) ReadAuthorizationModels(ctx context.Context, store string, options storage.PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error) {
	return m.ds.ReadAuthorizationModels(ctx, store, options)
}

func (m *SlowDataStorage) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {
	return m.ds.FindLatestAuthorizationModelID(ctx, store)
}

func (m *SlowDataStorage) ReadTypeDefinition(ctx context.Context, store, id, objectType string) (*openfgapb.TypeDefinition, error) {
	return m.ds.ReadTypeDefinition(ctx, store, id, objectType)
}

func (m *SlowDataStorage) WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error {
	return m.ds.WriteAuthorizationModel(ctx, store, model)
}

func (m *SlowDataStorage) CreateStore(ctx context.Context, newStore *openfgapb.Store) (*openfgapb.Store, error) {
	return m.ds.CreateStore(ctx, newStore)
}

func (m *SlowDataStorage) DeleteStore(ctx context.Context, id string) error {
	return m.ds.DeleteStore(ctx, id)
}

func (m *SlowDataStorage) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	return m.ds.WriteAssertions(ctx, store, modelID, assertions)
}

func (m *SlowDataStorage) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	return m.ds.ReadAssertions(ctx, store, modelID)
}

func (m *SlowDataStorage) MaxTuplesPerWrite() int {
	return m.ds.MaxTuplesPerWrite()
}

func (m *SlowDataStorage) MaxTypesPerAuthorizationModel() int {
	return m.ds.MaxTypesPerAuthorizationModel()
}

func (m *SlowDataStorage) GetStore(ctx context.Context, storeID string) (*openfgapb.Store, error) {
	return m.ds.GetStore(ctx, storeID)
}

func (m *SlowDataStorage) ListStores(ctx context.Context, paginationOptions storage.PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	return m.ds.ListStores(ctx, paginationOptions)
}

func (m *SlowDataStorage) IsReady(ctx context.Context) (bool, error) {
	return m.ds.IsReady(ctx)
}
