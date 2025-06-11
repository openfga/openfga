package sharediterator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers/storagewrappersutil"
	"github.com/openfga/openfga/pkg/telemetry"
)

var (
	tracer = otel.Tracer("openfga/pkg/storagewrappers/sharediterator")

	_ storage.RelationshipTupleReader = (*IteratorDatastore)(nil)
	_ storage.TupleIterator           = (*sharedIterator)(nil)

	sharedIteratorQueryHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "shared_iterator_query_ms",
		Help:                            "The duration (in ms) of a shared iterator query labeled by operation and shared.",
		Buckets:                         []float64{1, 5, 10, 25, 50, 100, 200, 300, 1000},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"operation", "method", "shared"})

	sharedIteratorBypassed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "shared_iterator_bypassed",
		Help:      "Total number of iterators bypassed by the shared iterator layer because the internal map size exceed specified limit OR max admission time has passed.",
	}, []string{"operation"})

	sharedIteratorCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: build.ProjectName,
		Name:      "shared_iterator_count",
		Help:      "The current number of items of shared iterator.",
	})
)

const (
	defaultSharedIteratorLimit = 1000000
	defaultIteratorTargetSize  = 1000
)

// Storage is a simple in-memory storage for shared iterators.
// It uses a sync.Map to store iterators and an atomic counter to keep track of the number of items.
// The limit is set to defaultSharedIteratorLimit, which can be overridden by the user.
// The storage is used to share iterators across multiple requests, allowing for efficient reuse of iterators.
type Storage struct {
	// iters is a map of iterators that are shared across requests.
	// The key is a string that uniquely identifies the iterator, and the value is a storageItem that contains
	// the iterator and its producer function.
	iters sync.Map

	// limit is the maximum number of items that can be stored in the storage.
	// If the number of items exceeds this limit, new iterators will not be created and the request will be bypassed.
	// This is to prevent memory exhaustion and ensure that the storage does not grow indefinitely.
	limit int64

	// ctr is an atomic counter that keeps track of the number of items in the storage.
	// It is incremented when a new iterator is created and decremented when an iterator is removed.
	// This counter is used to determine if the storage is full and if new iterators should be bypassed.
	// It is also used to monitor the number of active iterators in the system.
	ctr atomic.Int64
}

// storageItem is a wrapper around a shared iterator that provides thread-safe access to the iterator.
// The unwrap method is used to get a clone of the iterator, and if the iterator is not yet created,
// it will call the producer function to create a new iterator.
type storageItem struct {
	// mu is a mutex to ensure that only one goroutine can access the iterator at a time.
	mu sync.Mutex

	// iter is the shared iterator that is being wrapped.
	// It is set to nil when the iterator is not yet created, and will be created by the producer function.
	iter *sharedIterator

	// producer is a function that creates a new shared iterator.
	// It is called when the iterator is not yet created, and it should return a new shared iterator or an error.
	// This allows the storageItem to lazily create the iterator when it is first accessed.
	// This is useful for cases where the iterator creation is expensive or requires context.
	producer func() (*sharedIterator, error)
}

// unwrap returns a clone of the shared iterator.
// If the iterator is not yet created, it will call the producer function to create a new iterator.
// If the iterator is already created, it will return a clone of the existing iterator.
// If there is an error while creating the iterator, it will return nil and the error.
func (s *storageItem) unwrap() (*sharedIterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.iter != nil {
		return s.iter.clone(), nil
	}

	// If the iterator is not yet created, call the producer function to create it.
	it, err := s.producer()
	if err != nil {
		return nil, err
	}
	s.iter = it
	clone := it.clone()

	// Stop the original iterator since we are returning a clone.
	// This is important so that the original iterator does not continue to hold a reference count.
	// By cloning the iterator, the reference count is incremented to 2. After stopping the original iterator,
	// the reference count will be decremented to 1, allowing the clone to clean up properly when it is stopped.
	it.Stop()
	return clone, nil
}

type DatastoreStorageOpt func(*Storage)

// WithSharedIteratorDatastoreStorageLimit sets the limit on the number of items in SF iterator iters.
func WithSharedIteratorDatastoreStorageLimit(limit int) DatastoreStorageOpt {
	return func(b *Storage) {
		b.limit = int64(limit)
	}
}

func NewSharedIteratorDatastoreStorage(opts ...DatastoreStorageOpt) *Storage {
	newStorage := &Storage{
		limit: defaultSharedIteratorLimit,
	}
	for _, opt := range opts {
		opt(newStorage)
	}
	return newStorage
}

type IteratorDatastoreOpt func(*IteratorDatastore)

// WithSharedIteratorDatastoreLogger sets the logger for the IteratorDatastore.
func WithSharedIteratorDatastoreLogger(logger logger.Logger) IteratorDatastoreOpt {
	return func(b *IteratorDatastore) {
		b.logger = logger
	}
}

// WithMaxAdmissionTime sets the maximum duration for which shared iterator allows clone.
// After this period, clone will fail and fall back to non-shared iterator. This is done
// to prevent stale data if there are very long-running requests.
func WithMaxAdmissionTime(maxAdmissionTime time.Duration) IteratorDatastoreOpt {
	return func(b *IteratorDatastore) {
		b.maxAdmissionTime = maxAdmissionTime
	}
}

// WithMethod specifies whether the shared iterator is for check or list objects for metrics.
func WithMethod(method string) IteratorDatastoreOpt {
	return func(b *IteratorDatastore) {
		b.method = method
	}
}

// IteratorDatastore is a wrapper around a storage.RelationshipTupleReader that provides shared iterators.
// It uses an internal storage to manage shared iterators and provides methods to read tuples starting with a user,
// read userset tuples, and read tuples by key.
// The shared iterators are created lazily and are shared across multiple requests to improve performance.
type IteratorDatastore struct {
	// RelationshipTupleReader is the inner datastore that provides the actual implementation of reading tuples.
	storage.RelationshipTupleReader

	// logger is used for logging messages related to the shared iterator operations.
	logger logger.Logger

	// method is used to specify the type of operation being performed (e.g., "check" or "list objects").
	method string

	// internalStorage is used to store shared iterators and manage their lifecycle.
	internalStorage *Storage

	// maxAdmissionTime is the maximum duration for which shared iterator allows clone.
	maxAdmissionTime time.Duration
}

// NewSharedIteratorDatastore creates a new IteratorDatastore with the given inner RelationshipTupleReader and internal storage.
// It also accepts optional configuration options to customize the behavior of the datastore.
// The datastore will use shared iterators to improve performance by reusing iterators across multiple requests.
// If the number of active iterators exceeds the specified limit, new requests will be bypassed and handled by the inner reader.
func NewSharedIteratorDatastore(inner storage.RelationshipTupleReader, internalStorage *Storage, opts ...IteratorDatastoreOpt) *IteratorDatastore {
	sf := &IteratorDatastore{
		RelationshipTupleReader: inner,
		logger:                  logger.NewNoopLogger(),
		internalStorage:         internalStorage,
		method:                  "",
		maxAdmissionTime:        config.DefaultSharedIteratorMaxAdmissionTime,
	}

	for _, opt := range opts {
		opt(sf)
	}

	return sf
}

// ReadStartingWithUser reads tuples starting with a user using shared iterators.
// If the request is for higher consistency, it will fall back to the inner RelationshipTupleReader.
func (sf *IteratorDatastore) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	options storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.ReadStartingWithUser",
	)
	defer span.End()
	span.SetAttributes(attribute.String("consistency_preference", options.Consistency.Preference.String()))

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		// for now, we will skip shared iterator since there is a possibility that the request
		// may be slightly stale. In the future, consider whether we should have shared iterator
		// for higher consistency request. This may mean having separate cache.
		return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	}
	start := time.Now()

	cacheKey, err := storagewrappersutil.ReadStartingWithUserKey(store, filter)
	if err != nil {
		// should never happen
		telemetry.TraceError(span, err)
		return nil, err
	}
	span.SetAttributes(attribute.String("cache_key", cacheKey))

	// Check if the internal storage has reached its limit.
	// If it has, we will bypass the shared iterator and use the inner reader directly.
	// This is to prevent memory exhaustion and ensure that the storage does not grow indefinitely.
	var count int

	// If the limit is zero, we will not use the shared iterator.
	full := sf.internalStorage.limit == 0

	// Iterate over the internal storage to count the number of items.
	// This call will short-circuit if the count exceeds the limit.
	// The outcome of this operation is not guaranteed to be accurate, but it is sufficient for our use case.
	// The number of items admitted may be able to exceed the limit.
	sf.internalStorage.iters.Range(func(_, _ any) bool {
		count++
		full = count >= int(sf.internalStorage.limit)
		return !full
	})

	if full {
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadStartingWithUser).Inc()
		return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	}

	// Create a new storage item to hold the shared iterator.
	// This item will be stored in the internal storage map and will be used to share the iterator across requests.
	newStorageItem := new(storageItem)

	// The producer function is called to create a new shared iterator when it is first accessed.
	newStorageItem.producer = func() (*sharedIterator, error) {
		it, err := sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
		if err != nil {
			return nil, err
		}

		// Set a timer to remove the item from the internal storage if it is not used within the max admission time.
		// This is to prevent clones from being created indefinitely and to ensure that the storage does not grow indefinitely.
		timer := time.AfterFunc(sf.maxAdmissionTime, func() {
			if sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem) {
				sf.internalStorage.ctr.Add(-1)
			}
		})

		// Create a new shared iterator from the original iterator.
		// This allows the iterator to be shared across multiple requests.
		// The cleanup function will be called when the shared iterator is stopped, which will remove it from the internal storage.
		// This ensures that the internal storage does not grow indefinitely and that the iterator is cleaned up properly.
		// The cleanup function will also stop the timer, ensuring that the item is removed from the internal storage immediately.
		newIterator := newSharedIterator(it, func() {
			timer.Stop()
			if sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem) {
				sf.internalStorage.ctr.Add(-1)
			}
		})

		sharedIteratorCount.Inc()

		span.SetAttributes(attribute.Bool("found", false))

		sharedIteratorQueryHistogram.WithLabelValues(
			storagewrappersutil.OperationReadStartingWithUser, sf.method, "false",
		).Observe(float64(time.Since(start).Milliseconds()))

		return newIterator, err
	}

	// Load or store the new storage item in the internal storage map.
	// If the item is not already present, it will be added to the map and the counter will be incremented.
	value, loaded := sf.internalStorage.iters.LoadOrStore(cacheKey, newStorageItem)
	if !loaded {
		sf.internalStorage.ctr.Add(1)
	}
	item, _ := value.(*storageItem)

	// Unwrap the storage item to get the shared iterator.
	// If there is an error while unwrapping, we will remove the item from the internal storage and return the error.
	// If this is the first time the iterator is accessed, it will call the producer function to create a new iterator.
	it, err := item.unwrap()
	if err != nil {
		sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem)
		return nil, err
	}

	// If the iterator is nil, we will fall back to the inner RelationshipTupleReader.
	// This can happen if the cloned shared iterator is already stopped and all references have been cleaned up.
	if it == nil {
		return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	}

	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadStartingWithUser, sf.method, "true",
	).Observe(float64(time.Since(start).Milliseconds()))

	return it, nil
}

// ReadUsersetTuples reads userset tuples using shared iterators.
// If the request is for higher consistency, it will fall back to the inner RelationshipTupleReader.
func (sf *IteratorDatastore) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
	options storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.ReadUsersetTuples",
	)
	defer span.End()
	span.SetAttributes(attribute.String("consistency_preference", options.Consistency.Preference.String()))

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	}
	start := time.Now()

	cacheKey := storagewrappersutil.ReadUsersetTuplesKey(store, filter)
	span.SetAttributes(attribute.String("cache_key", cacheKey))

	// Check if the internal storage has reached its limit.
	// If it has, we will bypass the shared iterator and use the inner reader directly.
	// This is to prevent memory exhaustion and ensure that the storage does not grow indefinitely.
	var count int

	// If the limit is zero, we will not use the shared iterator.
	full := sf.internalStorage.limit == 0

	// Iterate over the internal storage to count the number of items.
	// This call will short-circuit if the count exceeds the limit.
	// The outcome of this operation is not guaranteed to be accurate, but it is sufficient for our use case.
	// The number of items admitted may be able to exceed the limit.
	sf.internalStorage.iters.Range(func(_, _ any) bool {
		count++
		full = count >= int(sf.internalStorage.limit)
		return !full
	})

	if full {
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadStartingWithUser).Inc()
		return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	}

	// Create a new storage item to hold the shared iterator.
	// This item will be stored in the internal storage map and will be used to share the iterator across requests.
	newStorageItem := new(storageItem)

	// The producer function is called to create a new shared iterator when it is first accessed.
	newStorageItem.producer = func() (*sharedIterator, error) {
		it, err := sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
		if err != nil {
			return nil, err
		}

		// Set a timer to remove the item from the internal storage if it is not used within the max admission time.
		// This is to prevent clones from being created indefinitely and to ensure that the storage does not grow indefinitely.
		timer := time.AfterFunc(sf.maxAdmissionTime, func() {
			if sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem) {
				sf.internalStorage.ctr.Add(-1)
			}
		})

		// Create a new shared iterator from the original iterator.
		// This allows the iterator to be shared across multiple requests.
		// The cleanup function will be called when the shared iterator is stopped, which will remove it from the internal storage.
		// This ensures that the internal storage does not grow indefinitely and that the iterator is cleaned up properly.
		// The cleanup function will also stop the timer, ensuring that the item is removed from the internal storage.
		newIterator := newSharedIterator(it, func() {
			timer.Stop()
			if sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem) {
				sf.internalStorage.ctr.Add(-1)
			}
		})

		sharedIteratorCount.Inc()

		span.SetAttributes(attribute.Bool("found", false))

		sharedIteratorQueryHistogram.WithLabelValues(
			storagewrappersutil.OperationReadUsersetTuples, sf.method, "false",
		).Observe(float64(time.Since(start).Milliseconds()))

		return newIterator, err
	}

	// Load or store the new storage item in the internal storage map.
	// If the item is not already present, it will be added to the map and the counter will be incremented.
	value, loaded := sf.internalStorage.iters.LoadOrStore(cacheKey, newStorageItem)
	if !loaded {
		sf.internalStorage.ctr.Add(1)
	}
	item, _ := value.(*storageItem)

	// Unwrap the storage item to get the shared iterator.
	// If there is an error while unwrapping, we will remove the item from the internal storage and return the error.
	// If this is the first time the iterator is accessed, it will call the producer function to create a new iterator.
	it, err := item.unwrap()
	if err != nil {
		sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem)
		return nil, err
	}

	// If the iterator is nil, we will fall back to the inner RelationshipTupleReader.
	// This can happen if the cloned shared iterator is already stopped and all references have been cleaned up.
	if it == nil {
		return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	}

	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadUsersetTuples, sf.method, "true",
	).Observe(float64(time.Since(start).Milliseconds()))

	return it, nil
}

// Read reads tuples by key using shared iterators.
// If the request is for higher consistency, it will fall back to the inner RelationshipTupleReader.
func (sf *IteratorDatastore) Read(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
	options storage.ReadOptions) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.Read",
	)
	defer span.End()
	span.SetAttributes(attribute.String("consistency_preference", options.Consistency.Preference.String()))

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	}
	start := time.Now()

	cacheKey := storagewrappersutil.ReadKey(store, tupleKey)
	span.SetAttributes(attribute.String("cache_key", cacheKey))

	// Check if the internal storage has reached its limit.
	// If it has, we will bypass the shared iterator and use the inner reader directly.
	// This is to prevent memory exhaustion and ensure that the storage does not grow indefinitely.
	var count int

	// If the limit is zero, we will not use the shared iterator.
	full := sf.internalStorage.limit == 0

	// Iterate over the internal storage to count the number of items.
	// This call will short-circuit if the count exceeds the limit.
	// The outcome of this operation is not guaranteed to be accurate, but it is sufficient for our use case.
	// The number of items admitted may be able to exceed the limit.
	sf.internalStorage.iters.Range(func(_, _ any) bool {
		count++
		full = count >= int(sf.internalStorage.limit)
		return !full
	})

	if full {
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationRead).Inc()
		return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	}

	// Create a new storage item to hold the shared iterator.
	// This item will be stored in the internal storage map and will be used to share the iterator across requests.
	newStorageItem := new(storageItem)

	// The producer function is called to create a new shared iterator when it is first accessed.
	newStorageItem.producer = func() (*sharedIterator, error) {
		it, err := sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
		if err != nil {
			return nil, err
		}

		// Set a timer to remove the item from the internal storage if it is not used within the max admission time.
		// This is to prevent clones from being created indefinitely and to ensure that the storage does not grow indefinitely.
		timer := time.AfterFunc(sf.maxAdmissionTime, func() {
			if sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem) {
				sf.internalStorage.ctr.Add(-1)
			}
		})

		// Create a new shared iterator from the original iterator.
		// This allows the iterator to be shared across multiple requests.
		// The cleanup function will be called when the shared iterator is stopped, which will remove it from the internal storage.
		// This ensures that the internal storage does not grow indefinitely and that the iterator is cleaned up properly.
		// The cleanup function will also stop the timer, ensuring that the item is removed from the internal storage.
		newIterator := newSharedIterator(it, func() {
			timer.Stop()
			if sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem) {
				sf.internalStorage.ctr.Add(-1)
			}
		})

		sharedIteratorCount.Inc()

		span.SetAttributes(attribute.Bool("found", false))

		sharedIteratorQueryHistogram.WithLabelValues(
			storagewrappersutil.OperationRead, sf.method, "false",
		).Observe(float64(time.Since(start).Milliseconds()))

		return newIterator, err
	}

	// Load or store the new storage item in the internal storage map.
	// If the item is not already present, it will be added to the map and the counter will be incremented.
	value, loaded := sf.internalStorage.iters.LoadOrStore(cacheKey, newStorageItem)
	if !loaded {
		sf.internalStorage.ctr.Add(1)
	}
	item, _ := value.(*storageItem)

	// Unwrap the storage item to get the shared iterator.
	// If there is an error while unwrapping, we will remove the item from the internal storage and return the error.
	// If this is the first time the iterator is accessed, it will call the producer function to create a new iterator.
	it, err := item.unwrap()
	if err != nil {
		sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem)
		return nil, err
	}

	// If the iterator is nil, we will fall back to the inner RelationshipTupleReader.
	// This can happen if the cloned shared iterator is already stopped and all references have been cleaned up.
	if it == nil {
		return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	}

	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationRead, sf.method, "true",
	).Observe(float64(time.Since(start).Milliseconds()))

	return it, nil
}

// BufferSize is the number of items to fetch at a time when reading from the shared iterator.
// This is primarily adjusted at runtime for testing purposes, but can be set to a different value if needed.
var BufferSize = 100

// reader is an interface that defines a method to read items from a source iterator.
type reader[T any] interface {
	// Read reads items from the source iterator into the provided buffer.
	Read(context.Context, []T) (int, error)
}

// stopper is an interface that defines a method to stop the iterator.
type stopper interface {
	// Stop stops the iterator and releases any resources associated with it.
	Stop()
}

// readStopper is an interface that combines the reader and stopper interfaces.
type readStopper[T any] interface {
	reader[T]
	stopper
}

// iteratorReader is a wrapper around a storage.Iterator that implements the reader interface.
type iteratorReader[T any] struct {
	storage.Iterator[T]
}

// Read reads items from the iterator into the provided buffer.
// The method will read up to the length of the buffer, and if there are fewer items available,
// it will return the number of items read and an error if any occurred.
func (ir *iteratorReader[T]) Read(ctx context.Context, buf []T) (int, error) {
	var i int
	var e error

	buflen := len(buf)

	for ; i < buflen; i++ {
		t, ierr := ir.Next(ctx)
		if ierr != nil {
			e = ierr
			break
		}
		buf[i] = t
	}
	return i, e
}

// sharedIterator is a thread-safe iterator that allows multiple goroutines to share the same iterator.
// It uses a mutex to ensure that only one goroutine can access an individual iterator instance at a time.
// Atomic variables are used to manage data shared between clones.
type sharedIterator struct {
	// mu is a mutex to ensure that only one goroutine can access the current iterator instance at a time.
	// mu is a value type because it is not shared between iterator instances.
	mu sync.Mutex

	// cleanup is a function that will be called when all shared iterator instances have stopped.
	cleanup func()

	// head is the index of the next item to be returned by the current iterator instance.
	// It is incremented each time an item is returned by the iterator in a call to Next.
	// head is a value type because it is not shared between iterator instances.
	head int

	// stopped is an atomic boolean that indicates whether the current iterator instance has been stopped.
	// stopped is a value type because it is not shared between iterator instances.
	stopped atomic.Bool

	// ctx is a shared context between all clones and manages the lifetime of a call to fetch.
	ctx context.Context

	// cancel is a function that cancels the shared context when all shared iterator instances have stopped.
	cancel context.CancelFunc

	// ir is the underlying iterator reader that provides the actual implementation of reading tuples.
	ir readStopper[*openfgav1.Tuple]

	// fetching is a shared atomic boolean that indicates whether any shared iterator instance is currently fetching items.
	fetching *atomic.Bool

	// err is a shared atomic pointer to an error that indicates if there was an error during the iteration.
	err *atomic.Pointer[error]

	// items is a shared atomic pointer to a slice of tuples that contains the items fetched by the iterator.
	items *atomic.Pointer[[]*openfgav1.Tuple]

	// refs is a shared atomic counter that keeps track of the number of shared instances of the iterator.
	refs *atomic.Int64

	// ch is a shared atomic pointer to a channel that is used to signal when new items are available.
	ch *atomic.Pointer[chan struct{}]
}

// newSharedIterator creates a new shared iterator from the given storage.TupleIterator.
// It initializes the shared context, cancellation function, and other necessary fields.
func newSharedIterator(it storage.TupleIterator, cleanup func()) *sharedIterator {
	ctx, cancel := context.WithCancel(context.Background())

	var fetching atomic.Bool

	// Initialize the channel that will be used to signal when new items are available.
	ch := make(chan struct{})
	var pch atomic.Pointer[chan struct{}]
	pch.Store(&ch)

	// Initialize the reference counter to 1, indicating that there is one active instance of the iterator.
	var refs atomic.Int64
	refs.Store(1)

	// Initialize the error pointer to nil, indicating that there are no errors at the start.
	// This will be used to store any errors that occur during the iteration.
	var err atomic.Pointer[error]
	terr := new(error)
	err.Store(terr)

	// Initialize the items pointer to an empty slice of tuples.
	// This will be used to store the items fetched by the iterator.
	var items atomic.Pointer[[]*openfgav1.Tuple]
	titems := make([]*openfgav1.Tuple, 0)
	items.Store(&titems)

	ir := &iteratorReader[*openfgav1.Tuple]{
		Iterator: it,
	}

	newIter := sharedIterator{
		ctx:      ctx,
		cancel:   cancel,
		cleanup:  cleanup,
		ir:       ir,
		fetching: &fetching,
		items:    &items,
		err:      &err,
		refs:     &refs,
		ch:       &pch,
	}

	return &newIter
}

// clone creates a new shared iterator that shares the same context, cancellation function, and other fields.
// It increments the reference count and returns a new instance of the shared iterator.
// If the reference count reaches zero, it returns nil, indicating that the iterator has been stopped.
// This allows multiple goroutines to share the same iterator instance without interfering with each other.
// The clone method is thread-safe and ensures that the reference count is incremented atomically.
func (s *sharedIterator) clone() *sharedIterator {
	for {
		remaining := s.refs.Load()

		// If the reference count is zero, it means that the iterator has been stopped and cleaned up.
		if remaining == 0 {
			return nil
		}

		if s.refs.CompareAndSwap(remaining, remaining+1) {
			return &sharedIterator{
				ctx:      s.ctx,
				cancel:   s.cancel,
				cleanup:  s.cleanup,
				ir:       s.ir,
				fetching: s.fetching,
				items:    s.items,
				err:      s.err,
				refs:     s.refs,
				ch:       s.ch,
			}
		}
	}
}

// fetchAndWait is a method that fetches items from the underlying storage.TupleIterator and waits for new items to be available.
// It blocks until new items are fetched or an error occurs.
// The items and err pointers are updated with the fetched items and any error encountered.
func (s *sharedIterator) fetchAndWait(ctx context.Context, items *[]*openfgav1.Tuple, err *error) {
	*items = *s.items.Load()
	*err = *s.err.Load()

	// Iterate until we have items available or an error occurs.
	for ; s.head >= len(*items) && *err == nil; *items, *err = *s.items.Load(), *s.err.Load() {
		ch := *s.ch.Load()

		if !s.fetching.Swap(true) {
			// If we are not already fetching, we start a new goroutine to fetch items.
			go func() {
				buf := make([]*openfgav1.Tuple, BufferSize)
				read, e := s.ir.Read(s.ctx, buf)

				// Load the current items from the shared items pointer and append the newly fetched items to it.
				ptrItems := s.items.Load()
				loadedItems := *ptrItems
				loadedItems = append(loadedItems, buf[:read]...)
				s.items.Store(&loadedItems)

				if e != nil {
					if e == context.Canceled || e == context.DeadlineExceeded {
						// If the context is canceled or deadline exceeded, we treat it as an iterator done.
						e = storage.ErrIteratorDone
					}
					s.err.Store(&e)
				}

				// Initialize a new channel to signal that new items are available on the next fetch.
				newCh := make(chan struct{})
				currentCh := s.ch.Swap(&newCh)

				// Important! The fetching state must be set to false before closing the old channel.
				// This avoids a race condition in which a goroutine waiting on the channel close might
				// enter this function again and try to fetch items before the fetching state is reset.
				s.fetching.Store(false)

				// Close the old channel to signal waiting consumers to wake up.
				close(*currentCh)
			}()
		}

		// Wait for new items to be available or for the context to be done.
		// This allows the iterator to block until new items are fetched or the context is canceled.
		// This is important to ensure that we do not return stale data or block indefinitely.
		select {
		case <-ch:
		case <-ctx.Done():
			return
		}
	}
}

// Current returns the current item in the shared iterator without advancing the iterator.
// It is used to peek at the next item without consuming it.
// If the iterator is stopped or there are no items available, it returns an error.
// It also handles fetching new items if the current head is beyond the available items.
func (s *sharedIterator) current(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if s.stopped.Load() {
		return nil, storage.ErrIteratorDone
	}

	var items []*openfgav1.Tuple
	var err error

	s.fetchAndWait(ctx, &items, &err)

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if s.head >= len(items) {
		if err != nil {
			return nil, err
		}
		// This is a guard clause to ensure we do not access out of bounds.
		// If we reach here, it means there is a bug in the underlying iterator.
		return nil, storage.ErrIteratorDone
	}

	return items[s.head], nil
}

// Head returns the first item in the shared iterator without advancing the iterator.
// It is used to peek at the next item without consuming it.
// If the iterator is stopped or there are no items available, it returns an error.
// It also handles fetching new items if the current head is beyond the available items.
func (s *sharedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.Head",
	)
	defer span.End()

	return s.current(ctx)
}

// Next returns the next item in the shared iterator and advances the iterator.
// It is used to consume the next item in the iterator.
// If the iterator is stopped or there are no items available, it returns an error.
// It also handles fetching new items if the current head is beyond the available items.
func (s *sharedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.Next",
	)
	defer span.End()

	result, err := s.current(ctx)
	if err != nil {
		return nil, err
	}
	s.head++
	return result, nil
}

// Stop stops the shared iterator and cleans up resources.
// It decrements the reference count and checks if it should clean up the iterator.
// If the reference count reaches zero, it calls the cleanup function to remove the iterator from the internal storage.
func (s *sharedIterator) Stop() {
	if s.stopped.CompareAndSwap(false, true) && s.refs.Add(-1) == 0 {
		s.cleanup()
		s.cancel()
		s.ir.Stop()
	}
}
