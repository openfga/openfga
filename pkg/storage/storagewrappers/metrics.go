package storagewrappers

import (
	"context"
	"sync/atomic"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

var _ storage.RelationshipTupleReader = (*MetricsOpenFGAStorage)(nil)

type MetricsOpenFGAStorage struct {
	storage.RelationshipTupleReader
	counter atomic.Uint32
}

func NewMetricsOpenFGAStorage(wrapped storage.RelationshipTupleReader) *MetricsOpenFGAStorage {
	return &MetricsOpenFGAStorage{
		RelationshipTupleReader: wrapped,
		counter:                 atomic.Uint32{},
	}
}

type Metrics struct {
	DatastoreQueryCount int
}

func (m *MetricsOpenFGAStorage) GetMetrics() Metrics {
	return Metrics{
		DatastoreQueryCount: int(m.counter.Load()),
	}
}

func (m *MetricsOpenFGAStorage) increase() {
	m.counter.Add(1)
}

// Read see [storage.RelationshipTupleReader.ReadUserTuple].
func (m *MetricsOpenFGAStorage) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadOptions) (storage.TupleIterator, error) {
	defer m.increase()

	return m.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
}

// ReadPage see [storage.RelationshipTupleReader.ReadPage].
func (m *MetricsOpenFGAStorage) ReadPage(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadPageOptions) ([]*openfgav1.Tuple, []byte, error) {
	defer m.increase()

	return m.RelationshipTupleReader.ReadPage(ctx, store, tupleKey, options)
}

// ReadUserTuple see [storage.RelationshipTupleReader].ReadUserTuple.
func (m *MetricsOpenFGAStorage) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	defer m.increase()

	return m.RelationshipTupleReader.ReadUserTuple(ctx, store, tupleKey, options)
}

// ReadUsersetTuples see [storage.RelationshipTupleReader].ReadUsersetTuples.
func (m *MetricsOpenFGAStorage) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	defer m.increase()

	return m.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
}

// ReadStartingWithUser see [storage.RelationshipTupleReader].ReadStartingWithUser.
func (m *MetricsOpenFGAStorage) ReadStartingWithUser(ctx context.Context, store string, opts storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	defer m.increase()

	return m.RelationshipTupleReader.ReadStartingWithUser(ctx, store, opts, options)
}
