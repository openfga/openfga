package storagewrappers

import (
	"context"
	"sync/atomic"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

var _ storage.RelationshipTupleReader = (*InstrumentedOpenFGAStorage)(nil)

type InstrumentedOpenFGAStorage struct {
	storage.RelationshipTupleReader
	countReads atomic.Uint32
}

// NewInstrumentedOpenFGAStorage creates a new instance of InstrumentedOpenFGAStorage that wraps the specified datastore and maintains metrics per request.
// InstrumentedOpenFGAStorage is thread-safe but should not be shared across multiple requests.
// It is crucial that the wrapped object does NOT return results from an in-memory cache for this object to return accurate metrics.
func NewInstrumentedOpenFGAStorage(wrapped storage.RelationshipTupleReader) *InstrumentedOpenFGAStorage {
	return &InstrumentedOpenFGAStorage{
		RelationshipTupleReader: wrapped,
		countReads:              atomic.Uint32{},
	}
}

type Metrics struct {
	DatastoreQueryCount uint32
}

func (m *InstrumentedOpenFGAStorage) GetMetrics() Metrics {
	return Metrics{
		DatastoreQueryCount: m.countReads.Load(),
	}
}

func (m *InstrumentedOpenFGAStorage) increaseReads() {
	m.countReads.Add(1)
}

// Read see [storage.RelationshipTupleReader.Read].
func (m *InstrumentedOpenFGAStorage) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadOptions) (storage.TupleIterator, error) {
	m.increaseReads()

	return m.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
}

// ReadPage see [storage.RelationshipTupleReader.ReadPage].
func (m *InstrumentedOpenFGAStorage) ReadPage(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	m.increaseReads()

	return m.RelationshipTupleReader.ReadPage(ctx, store, tupleKey, options)
}

// ReadUserTuple see [storage.RelationshipTupleReader.ReadUserTuple].
func (m *InstrumentedOpenFGAStorage) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	m.increaseReads()

	return m.RelationshipTupleReader.ReadUserTuple(ctx, store, tupleKey, options)
}

// ReadUsersetTuples see [storage.RelationshipTupleReader.ReadUsersetTuples].
func (m *InstrumentedOpenFGAStorage) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	m.increaseReads()

	return m.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
}

// ReadStartingWithUser see [storage.RelationshipTupleReader.ReadStartingWithUser].
func (m *InstrumentedOpenFGAStorage) ReadStartingWithUser(ctx context.Context, store string, opts storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	m.increaseReads()

	return m.RelationshipTupleReader.ReadStartingWithUser(ctx, store, opts, options)
}
