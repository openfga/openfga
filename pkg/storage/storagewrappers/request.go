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
	cacheSettings config.CacheSettings, logger logger.Logger) *RequestStorageWrapper {
	var a storage.RelationshipTupleReader
	a = NewBoundedConcurrencyTupleReader(ds, maxConcurrentReads) // to rate-limit reads
	if cacheSettings.ShouldCacheIterators() {
		// TODO(miparnisari): pass cacheSettings and resources directly, i can't now because i would create a package import cycle
		a = NewCachedDatastore(resources.ServerCtx, a, resources.CheckCache, int(cacheSettings.CheckIteratorCacheMaxResults), cacheSettings.CheckIteratorCacheTTL, resources.SingleflightGroup, resources.WaitGroup,
			WithCachedDatastoreLogger(logger)) // to read tuples from cache
	}
	b := NewInstrumentedOpenFGAStorage(a)                   // to capture metrics
	c := NewCombinedTupleReader(b, requestContextualTuples) // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: c,
		InstrumentedStorage:     b,
	}
}

// NewRequestStorageWrapperForListAPIs can be used for ListObjects or ListUsers.
func NewRequestStorageWrapperForListAPIs(ds storage.RelationshipTupleReader, requestContextualTuples []*openfgav1.TupleKey, maxConcurrentReads uint32) *RequestStorageWrapper {
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
