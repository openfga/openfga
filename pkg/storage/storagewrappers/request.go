package storagewrappers

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
)

// RequestStorageWrapper uses the decorator pattern to wrap a RelationshipTupleReader with various functionalities,
// which includes exposing metrics.
type RequestStorageWrapper struct {
	storage.RelationshipTupleReader
	InstrumentedStorage
}

var _ InstrumentedStorage = (*RequestStorageWrapper)(nil)

func NewRequestStorageWrapperForCheckAPI(ds storage.RelationshipTupleReader, requestContextualTuples []*openfgav1.TupleKey, maxConcurrentReads uint32,
	resources *shared.SharedCheckResources,
	cacheSettings config.CacheSettings,
	logger logger.Logger,
) *RequestStorageWrapper {
	var tupleReader storage.RelationshipTupleReader
	tupleReader = NewBoundedConcurrencyTupleReader(ds, maxConcurrentReads) // to rate-limit reads
	if cacheSettings.ShouldCacheIterators() {
		// TODO(miparnisari): pass cacheSettings and resources directly, i can't now because i would create a package import cycle
		tupleReader = NewCachedDatastore(
			resources.ServerCtx,
			tupleReader,
			resources.CheckCache,
			int(cacheSettings.CheckIteratorCacheMaxResults),
			cacheSettings.CheckIteratorCacheTTL,
			resources.SingleflightGroup,
			resources.WaitGroup,
			WithCachedDatastoreLogger(logger),
		) // to read tuples from cache
	}
	instrumentedStorage := NewInstrumentedOpenFGAStorage(tupleReader)                           // to capture metrics
	combinedTupleReader := NewCombinedTupleReader(instrumentedStorage, requestContextualTuples) // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: combinedTupleReader,
		InstrumentedStorage:     instrumentedStorage,
	}
}

// NewRequestStorageWrapperForListAPIs can be used for ListObjects or ListUsers.
func NewRequestStorageWrapperForListAPIs(ds storage.RelationshipTupleReader, requestContextualTuples []*openfgav1.TupleKey, maxConcurrentReads uint32) *RequestStorageWrapper {
	tupleReader := NewBoundedConcurrencyTupleReader(ds, maxConcurrentReads)                     // to rate-limit reads
	instrumentedStorage := NewInstrumentedOpenFGAStorage(tupleReader)                           // to capture metrics
	combinedTupleReader := NewCombinedTupleReader(instrumentedStorage, requestContextualTuples) // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: combinedTupleReader,
		InstrumentedStorage:     instrumentedStorage,
	}
}

func (s *RequestStorageWrapper) GetMetrics() Metrics {
	return s.InstrumentedStorage.GetMetrics()
}
