package storagewrappers

import (
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers/sharediterator"
)

type OperationType int

type Operation struct {
	Method            apimethod.APIMethod
	Concurrency       uint32
	ThrottleThreshold int
	ThrottleDuration  time.Duration
}

// RequestStorageWrapper uses the decorator pattern to wrap a RelationshipTupleReader with various functionalities,
// which includes exposing metrics.
type RequestStorageWrapper struct {
	storage.RelationshipTupleReader
	StorageInstrumentation
}

var _ StorageInstrumentation = (*RequestStorageWrapper)(nil)

// NewRequestStorageWrapperWithCache wraps the existing datastore to enable caching of iterators.
func NewRequestStorageWrapperWithCache(
	ds storage.RelationshipTupleReader,
	requestContextualTuples []*openfgav1.TupleKey,
	op *Operation,
	resources *shared.SharedDatastoreResources,
	cacheSettings config.CacheSettings,
) *RequestStorageWrapper {
	instrumented := NewBoundedTupleReader(ds, op) // to rate-limit reads
	var tupleReader storage.RelationshipTupleReader
	tupleReader = instrumented
	if op.Method == apimethod.Check && cacheSettings.ShouldCacheCheckIterators() {
		// Reads tuples from cache where possible
		tupleReader = NewCachedDatastore(
			resources.ServerCtx,
			tupleReader,
			resources.CheckCache,
			int(cacheSettings.CheckIteratorCacheMaxResults),
			cacheSettings.CheckIteratorCacheTTL,
			resources.SingleflightGroup,
			resources.WaitGroup,
			WithCachedDatastoreLogger(resources.Logger),
			WithCachedDatastoreMethodName(string(op.Method)),
		)
	} else if op.Method == apimethod.ListObjects && cacheSettings.ShouldCacheListObjectsIterators() {
		tupleReader = NewCachedDatastore(
			resources.ServerCtx,
			tupleReader,
			resources.CheckCache,
			int(cacheSettings.ListObjectsIteratorCacheMaxResults),
			cacheSettings.ListObjectsIteratorCacheTTL,
			resources.SingleflightGroup,
			resources.WaitGroup,
			WithCachedDatastoreLogger(resources.Logger),
			WithCachedDatastoreMethodName(string(op.Method)),
		)
	}
	if cacheSettings.SharedIteratorEnabled {
		tupleReader = sharediterator.NewSharedIteratorDatastore(tupleReader, resources.SharedIteratorStorage, sharediterator.WithSharedIteratorDatastoreLogger(resources.Logger))
	}
	combinedTupleReader := NewCombinedTupleReader(tupleReader, requestContextualTuples) // to read the contextual tuples

	return &RequestStorageWrapper{
		RelationshipTupleReader: combinedTupleReader,
		StorageInstrumentation:  instrumented,
	}
}

// NewRequestStorageWrapper is used for ListUsers.
func NewRequestStorageWrapper(ds storage.RelationshipTupleReader, requestContextualTuples []*openfgav1.TupleKey, op *Operation) *RequestStorageWrapper {
	instrumented := NewBoundedTupleReader(ds, op)
	return &RequestStorageWrapper{
		RelationshipTupleReader: NewCombinedTupleReader(instrumented, requestContextualTuples),
		StorageInstrumentation:  instrumented,
	}
}

func (s *RequestStorageWrapper) GetMetadata() Metadata {
	return s.StorageInstrumentation.GetMetadata()
}
