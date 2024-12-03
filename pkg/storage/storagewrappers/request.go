package storagewrappers

import (
	"time"

	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/storage"
)

// RequestStorageWrapper uses the decorator pattern to wrap a RelationshipTupleReader with various functionalities,
// which includes exposing metrics.
type RequestStorageWrapper struct {
	storage.RelationshipTupleReader
	metrics *InstrumentedOpenFGAStorage // keep a reference so the caller can access final metrics
}

var _ InstrumentedStorage = (*RequestStorageWrapper)(nil)

func NewRequestStorageWrapperForCheckAPI(ds storage.RelationshipTupleReader,
	requestContextualTuples []*openfgav1.TupleKey,
	maxConcurrentReads uint32,
	shouldCacheIterators bool,
	singleflightGroup *singleflight.Group,
	checkCache storage.InMemoryCache[any],
	maxCheckCacheSize uint32,
	checkCacheTTL time.Duration) *RequestStorageWrapper {
	var a storage.RelationshipTupleReader
	a = NewBoundedConcurrencyTupleReader(ds, maxConcurrentReads) // to rate-limit reads
	if shouldCacheIterators {
		a = graph.NewCachedDatastore(a, checkCache, int(maxCheckCacheSize), checkCacheTTL, singleflightGroup) // to read tuples from cache
	}
	b := NewInstrumentedOpenFGAStorage(a)                   // to capture metrics
	c := NewCombinedTupleReader(b, requestContextualTuples) // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: c,
		metrics:                 b,
	}
}

// NewRequestStorageWrapperForListAPIs can be used for ListObjects or ListUsers.
func NewRequestStorageWrapperForListAPIs(ds storage.RelationshipTupleReader, requestContextualTuples []*openfgav1.TupleKey, maxConcurrentReads uint32) *RequestStorageWrapper {
	a := NewBoundedConcurrencyTupleReader(ds, maxConcurrentReads) // to rate-limit reads
	b := NewInstrumentedOpenFGAStorage(a)                         // to capture metrics
	c := NewCombinedTupleReader(b, requestContextualTuples)       // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: c,
		metrics:                 b,
	}
}

func (s *RequestStorageWrapper) GetMetrics() Metrics {
	return s.metrics.GetMetrics()
}
