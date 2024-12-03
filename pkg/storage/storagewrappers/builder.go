package storagewrappers

import (
	"time"

	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/storage"
)

// RequestStorageWrapper uses the decorator pattern to wrap a RelationshipTupleReader with various functionalities.
type RequestStorageWrapper struct {
	storage.RelationshipTupleReader
	metrics *InstrumentedOpenFGAStorage // keep a reference so the caller can access final metrics
}

var _ InstrumentedStorage = (*RequestStorageWrapper)(nil)

func NewStorageWrapperForCheckAPI(ds storage.RelationshipTupleReader,
	requestContextualTuples []*openfgav1.TupleKey,
	maxConcurrentReads uint32,
	shouldCache bool,
	singleflightGroup *singleflight.Group,
	checkCache storage.InMemoryCache[any],
	maxCheckCacheSize uint32,
	checkCacheTTL time.Duration) *RequestStorageWrapper {
	var br storage.RelationshipTupleReader
	br = NewBoundedConcurrencyTupleReader(ds, maxConcurrentReads) // to rate-limit reads
	if shouldCache {
		br = graph.NewCachedDatastore(br, checkCache, int(maxCheckCacheSize), checkCacheTTL, singleflightGroup) // to read tuples from cache
	}
	iw := NewInstrumentedOpenFGAStorage(br)                   // to capture metrics
	cr := NewCombinedTupleReader(iw, requestContextualTuples) // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: cr,
		metrics:                 iw,
	}
}

// NewStorageWrapperForListAPIs can be used for ListObjects or ListUsers.
func NewStorageWrapperForListAPIs(ds storage.RelationshipTupleReader, requestContextualTuples []*openfgav1.TupleKey, maxConcurrentReads uint32) *RequestStorageWrapper {
	br := NewBoundedConcurrencyTupleReader(ds, maxConcurrentReads) // to rate-limit reads
	iw := NewInstrumentedOpenFGAStorage(br)                        // to capture metrics
	cr := NewCombinedTupleReader(iw, requestContextualTuples)      // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: cr,
		metrics:                 iw,
	}
}

func (s *RequestStorageWrapper) GetMetrics() Metrics {
	return s.metrics.GetMetrics()
}
