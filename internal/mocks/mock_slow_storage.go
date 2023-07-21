package mocks

import (
	"context"
	"fmt"
	"time"

	"github.com/openfga/openfga/pkg/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const defaultDelay = 0 * time.Second

type slowDataStorage struct {
	readDelay                 time.Duration
	readPageDelay             time.Duration
	readUserTupleDelay        time.Duration
	readUsersetTuplesDelay    time.Duration
	readStartingWithUserDelay time.Duration
	storage.OpenFGADatastore
}

type MockSlowDataStorageOption func(*slowDataStorage)

func WithReadDelay(d time.Duration) MockSlowDataStorageOption {
	return func(ds *slowDataStorage) {
		ds.readDelay = d
	}
}

func WithReadPageDelay(d time.Duration) MockSlowDataStorageOption {
	return func(ds *slowDataStorage) {
		ds.readPageDelay = d
	}
}

func WithReadUserTupleDelay(d time.Duration) MockSlowDataStorageOption {
	return func(ds *slowDataStorage) {
		ds.readUserTupleDelay = d
	}
}

func WithReadUsersetTuplesDelay(d time.Duration) MockSlowDataStorageOption {
	return func(ds *slowDataStorage) {
		ds.readUsersetTuplesDelay = d
	}
}

func WithReadStartingWithUserDelay(d time.Duration) MockSlowDataStorageOption {
	return func(ds *slowDataStorage) {
		ds.readStartingWithUserDelay = d
	}
}

// NewMockSlowDataStorage returns a wrapper of a datastore that adds artificial delays into the reads of tuples
// By default, it adds no delay.
func NewMockSlowDataStorage(ds storage.OpenFGADatastore, opts ...MockSlowDataStorageOption) storage.OpenFGADatastore {
	s := &slowDataStorage{
		readDelay:                 defaultDelay,
		readPageDelay:             defaultDelay,
		readUserTupleDelay:        defaultDelay,
		readUsersetTuplesDelay:    defaultDelay,
		readStartingWithUserDelay: defaultDelay,
		OpenFGADatastore:          ds,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (m *slowDataStorage) Close() {}

func (m *slowDataStorage) Read(ctx context.Context, store string, key *openfgapb.TupleKey) (storage.TupleIterator, error) {
	time.Sleep(m.readDelay)
	return m.OpenFGADatastore.Read(ctx, store, key)
}

func (m *slowDataStorage) ReadPage(ctx context.Context, store string, key *openfgapb.TupleKey, paginationOptions storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	time.Sleep(m.readPageDelay)
	return m.OpenFGADatastore.ReadPage(ctx, store, key, paginationOptions)
}

func (m *slowDataStorage) ReadUserTuple(ctx context.Context, store string, key *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	time.Sleep(m.readUserTupleDelay)
	fmt.Printf("%s ReadUserTuple\n", time.Now())
	return m.OpenFGADatastore.ReadUserTuple(ctx, store, key)
}

func (m *slowDataStorage) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	time.Sleep(m.readUsersetTuplesDelay)
	fmt.Printf("%s ReadUsersetTuples\n", time.Now())
	return m.OpenFGADatastore.ReadUsersetTuples(ctx, store, filter)
}

func (m *slowDataStorage) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {
	time.Sleep(m.readStartingWithUserDelay)
	return m.OpenFGADatastore.ReadStartingWithUser(ctx, store, filter)
}
