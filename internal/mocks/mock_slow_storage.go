package mocks

import (
	"context"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

// slowDataStorage is a proxy to the actual ds except the Reads are slow time by the readTuplesDelay
// This allows simulating list objection condition that times out.
type slowDataStorage struct {
	readTuplesDelay time.Duration
	storage.OpenFGADatastore
}

// NewMockSlowDataStorage returns a wrapper of a datastore that adds artificial delays into the reads of tuples.
func NewMockSlowDataStorage(ds storage.OpenFGADatastore, readTuplesDelay time.Duration) storage.OpenFGADatastore {
	return &slowDataStorage{
		readTuplesDelay:  readTuplesDelay,
		OpenFGADatastore: ds,
	}
}

func (m *slowDataStorage) Close() {}

func (m *slowDataStorage) Read(ctx context.Context, store string, key *openfgav1.TupleKey) (storage.TupleIterator, error) {
	time.Sleep(m.readTuplesDelay)
	return m.OpenFGADatastore.Read(ctx, store, key)
}

func (m *slowDataStorage) ReadPage(ctx context.Context, store string, key *openfgav1.TupleKey, paginationOptions storage.PaginationOptions) ([]*openfgav1.Tuple, []byte, error) {
	time.Sleep(m.readTuplesDelay)
	return m.OpenFGADatastore.ReadPage(ctx, store, key, paginationOptions)
}

func (m *slowDataStorage) ReadUserTuple(ctx context.Context, store string, key *openfgav1.TupleKey) (*openfgav1.Tuple, error) {
	time.Sleep(m.readTuplesDelay)
	return m.OpenFGADatastore.ReadUserTuple(ctx, store, key)
}

func (m *slowDataStorage) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	time.Sleep(m.readTuplesDelay)
	return m.OpenFGADatastore.ReadUsersetTuples(ctx, store, filter)
}

func (m *slowDataStorage) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {
	time.Sleep(m.readTuplesDelay)
	return m.OpenFGADatastore.ReadStartingWithUser(ctx, store, filter)
}
