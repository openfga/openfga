package storagewrappers

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
)

type Operation int

const (
	Check Operation = iota
	ListObjects
)

// RequestStorageWrapper uses the decorator pattern to wrap a RelationshipTupleReader with various functionalities,
// which includes exposing metrics.
type RequestStorageWrapper struct {
	storage.RelationshipTupleReader
	InstrumentedStorage
}

var _ InstrumentedStorage = (*RequestStorageWrapper)(nil)

// NewRequestStorageWrapperWithCache wraps the existing datstore to enable caching of iterators.
func NewRequestStorageWrapperWithCache(
	ds storage.RelationshipTupleReader,
	requestContextualTuples []*openfgav1.TupleKey,
	maxConcurrentReads uint32,
	resources *shared.SharedDatastoreResources,
	cacheSettings config.CacheSettings,
	logger logger.Logger,
	operation Operation,
) *RequestStorageWrapper {
	var tupleReader storage.RelationshipTupleReader
	tupleReader = NewBoundedConcurrencyTupleReader(ds, maxConcurrentReads) // to rate-limit reads
	if operation == Check && cacheSettings.ShouldCacheCheckIterators() {
		// Reads tuples from cache where possible
		tupleReader = NewCachedDatastore(
			resources.ServerCtx,
			tupleReader,
			resources.CheckCache,
			int(cacheSettings.CheckIteratorCacheMaxResults),
			cacheSettings.CheckIteratorCacheTTL,
			resources.SingleflightGroup,
			resources.WaitGroup,
			WithCachedDatastoreLogger(logger),
		)
	} else if operation == ListObjects && cacheSettings.ShouldCacheListObjectsIterators() {
		tupleReader = NewCachedDatastore(
			resources.ServerCtx,
			tupleReader,
			resources.CheckCache,
			int(cacheSettings.ListObjectsIteratorCacheMaxResults),
			cacheSettings.ListObjectsIteratorCacheTTL,
			resources.SingleflightGroup,
			resources.WaitGroup,
			WithCachedDatastoreLogger(logger),
		)
	}
	instrumentedStorage := NewInstrumentedOpenFGAStorage(tupleReader)                           // to capture metrics
	combinedTupleReader := NewCombinedTupleReader(instrumentedStorage, requestContextualTuples) // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: combinedTupleReader,
		InstrumentedStorage:     instrumentedStorage,
	}
}

// NewRequestStorageWrapper can be used for ListObjects or ListUsers.
func NewRequestStorageWrapper(ds storage.RelationshipTupleReader, requestContextualTuples []*openfgav1.TupleKey, maxConcurrentReads uint32) *RequestStorageWrapper {
	a := NewBoundedConcurrencyTupleReader(ds, maxConcurrentReads) // to rate-limit reads
	b := NewInstrumentedOpenFGAStorage(a)                         // to capture metrics
	c := NewCombinedTupleReader(b, requestContextualTuples)       // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: c,
		InstrumentedStorage:     b,
	}
}

func (s *RequestStorageWrapper) GetMetrics() Metrics {
	return s.InstrumentedStorage.GetMetrics()
}
