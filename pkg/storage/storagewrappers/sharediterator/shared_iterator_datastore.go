package sharediterator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers/storagewrappersutil"
)

var (
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
	}, []string{"operation", "method"})
)

// call is a structure that holds the state of a single-flight call.
type call struct {
	// bank holds the shared iterators that are allocated for the waiting goroutines.
	bank []sharedIterator

	// n holds the number of active goroutines waiting for the shared iterator.
	n int64

	// wg is a wait group that blocks additional goroutines until the single-flight call is completed.
	wg sync.WaitGroup

	// err holds any error that occurred during the single-flight call.
	err error
}

// group is a structure that manages a group of single-flight calls.
type group struct {
	// mu is a mutex that protects access to the map of calls.
	mu sync.Mutex

	// m is a map that holds the active single-flight calls.
	m map[string]*call
}

// Do executes the provided function fn for the given key, ensuring that only one goroutine can execute it at a time.
// If another goroutine is already executing the function for the same key, it will wait for that goroutine to finish.
// The result of the function execution is cloned to all waiting goroutines.
func (g *group) Do(key string, fn func() (*sharedIterator, error)) (*sharedIterator, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}

	if c, ok := g.m[key]; ok {
		atomic.AddInt64(&c.n, 1)
		g.mu.Unlock()
		c.wg.Wait()
		return &c.bank[int(atomic.AddInt64(&c.n, -1))], c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	v, err := fn()
	c.err = err

	g.mu.Lock()
	delete(g.m, key)
	c.bank = make([]sharedIterator, atomic.LoadInt64(&c.n))
	for i := range c.bank {
		v.clone(&c.bank[i])
	}
	c.wg.Done()
	g.mu.Unlock()

	return v, err
}

// Storage manages shared iterators using a single-flight pattern.
// It contains separate single-flight groups for different iterator operations,
// ensuring that only one goroutine creates an iterator for a given key at a time
// while other goroutines wait and receive cloned instances.
type Storage struct {
	// read is a single-flight group for the iterator datastore read operation.
	read group

	// rswu is a single-flight group for the iterator datastore read starting with user operation.
	rswu group

	// rut is a single-flight group for the iterator datastore read user tuples operation.
	rut group
}

type DatastoreStorageOpt func(*Storage)

type IteratorDatastoreOpt func(*IteratorDatastore)

// WithSharedIteratorDatastoreLogger sets the logger for the IteratorDatastore.
func WithSharedIteratorDatastoreLogger(logger logger.Logger) IteratorDatastoreOpt {
	return func(b *IteratorDatastore) {
		b.logger = logger
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
	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		// for now, we will skip shared iterator since there is a possibility that the request
		// may be slightly stale. In the future, consider whether we should have shared iterator
		// for higher consistency request. This may mean having separate cache.
		return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	}
	start := time.Now()

	cacheKey, err := storagewrappersutil.ReadStartingWithUserKey(store, filter)
	if err != nil {
		return nil, err
	}

	it, err := sf.internalStorage.rswu.Do(cacheKey, func() (*sharedIterator, error) {
		it, err := sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
		if err != nil {
			return nil, err
		}

		return newSharedIterator(it), nil
	})

	if err != nil {
		return nil, err
	}

	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadStartingWithUser, sf.method,
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
	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	}
	start := time.Now()

	cacheKey := storagewrappersutil.ReadUsersetTuplesKey(store, filter)

	it, err := sf.internalStorage.rut.Do(cacheKey, func() (*sharedIterator, error) {
		it, err := sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
		if err != nil {
			return nil, err
		}

		return newSharedIterator(it), nil
	})

	if err != nil {
		return nil, err
	}

	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadUsersetTuples, sf.method,
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
	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	}
	start := time.Now()

	cacheKey := storagewrappersutil.ReadKey(store, tupleKey)

	it, err := sf.internalStorage.read.Do(cacheKey, func() (*sharedIterator, error) {
		it, err := sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
		if err != nil {
			return nil, err
		}

		return newSharedIterator(it), nil
	})

	if err != nil {
		return nil, err
	}

	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationRead, sf.method,
	).Observe(float64(time.Since(start).Milliseconds()))

	return it, nil
}

// bufferSize is the number of items to fetch at a time when reading from the shared iterator.
const bufferSize = 100

// await is an object that executes an action exactly once at a time.
//
// The singleflight.Group type was used as a comparison to the await type, but was found to be ~59% slower than await
// in concurrent stress test benchmarks.
type await struct {
	active bool
	wg     *sync.WaitGroup
	mu     sync.Mutex
}

// Do executes the provided function fn if it is not already being executed.
// The first goroutine to call Do will execute the function, while subsequent calls will block until the function has completed.
// This ensures that only one goroutine can execute the function at a time, preventing concurrent execution of the function.
func (a *await) Do(fn func()) {
	a.mu.Lock()
	if a.active {
		wg := a.wg
		a.mu.Unlock()
		wg.Wait()
		return
	}
	a.active = true
	a.wg = new(sync.WaitGroup)
	a.wg.Add(1)
	a.mu.Unlock()

	fn()
	a.wg.Done()

	a.mu.Lock()
	a.active = false
	a.mu.Unlock()
}

// iteratorReader is a wrapper around a storage.Iterator that implements the reader interface.
type iteratorReader[T any] struct {
	storage.Iterator[T]
}

// Read reads items from the iterator into the provided buffer.
// The method will read up to the length of the buffer, and if there are fewer items available,
// it will return the number of items read and an error if any occurred.
func (ir *iteratorReader[T]) Read(ctx context.Context, buf []T) (int, error) {
	for i := range buf {
		t, err := ir.Next(ctx)
		if err != nil {
			return i, err
		}
		buf[i] = t
	}
	return len(buf), nil
}

// iteratorState holds the state of the shared iterator.
// It contains a slice of items that have been fetched and any error encountered during the iteration.
// This state is shared between all clones of the iterator and is updated atomically to ensure thread-safety.
type iteratorState struct {
	items []*openfgav1.Tuple
	err   error
}

// sharedIterator is a thread-safe iterator that allows multiple goroutines to share the same iterator.
// It uses a mutex to ensure that only one goroutine can access an individual iterator instance at a time.
// Atomic variables are used to manage data shared between clones.
type sharedIterator struct {
	// mu is a mutex to ensure that only one goroutine can access the current iterator instance at a time.
	// mu is a value type because it is not shared between iterator instances.
	mu sync.Mutex

	// head is the index of the next item to be returned by the current iterator instance.
	// It is incremented each time an item is returned by the iterator in a call to Next.
	// head is a value type because it is not shared between iterator instances.
	head int

	// stopped is an atomic boolean that indicates whether the current iterator instance has been stopped.
	// stopped is a value type because it is not shared between iterator instances.
	stopped bool

	// await ensures that only one goroutine can fetch items at a time.
	// It is used to block until new items are available or the context is done.
	await *await

	// ir is the underlying iterator reader that provides the actual implementation of reading tuples.
	ir *iteratorReader[*openfgav1.Tuple]

	// state is a shared atomic pointer to the iterator state, which contains the items and any error encountered during iteration.
	state *atomic.Pointer[iteratorState]

	// refs is a shared atomic counter that keeps track of the number of shared instances of the iterator.
	refs *atomic.Int64
}

// newSharedIterator creates a new shared iterator from the given storage.TupleIterator.
// It initializes the shared context, cancellation function, and other necessary fields.
func newSharedIterator(it storage.TupleIterator) *sharedIterator {
	var aw await

	// Initialize the reference counter to 1, indicating that there is one active instance of the iterator.
	var refs atomic.Int64
	refs.Store(1)

	ir := &iteratorReader[*openfgav1.Tuple]{
		Iterator: it,
	}

	// Initialize the iterator state with an empty slice of items and no error.
	var state iteratorState
	var pstate atomic.Pointer[iteratorState]
	pstate.Store(&state)

	return &sharedIterator{
		await: &aw,
		ir:    ir,
		state: &pstate,
		refs:  &refs,
	}
}

// clone creates a new shared iterator that shares the same context, cancellation function, and other fields.
// It increments the reference count and returns a new instance of the shared iterator.
// If the original iterator has been stopped, it returns false, indicating that the clone could not be created.
func (s *sharedIterator) clone(i *sharedIterator) bool {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false
	}
	s.refs.Add(1)
	s.mu.Unlock()

	i.await = s.await
	i.ir = s.ir
	i.state = s.state
	i.refs = s.refs

	return true
}

// fetchMore is a method that fetches more items from the underlying storage.TupleIterator.
// It reads a fixed number of items (bufferSize) from the iterator and appends them
// to the shared items slice in the iterator state.
// If an error occurs during the read operation, it updates the error in the iterator state.
func (s *sharedIterator) fetchMore() {
	var buf [bufferSize]*openfgav1.Tuple
	read, e := s.ir.Read(context.Background(), buf[:])

	// Load the current items from the shared items pointer and append the newly fetched items to it.
	state := s.state.Load()

	newState := &iteratorState{
		items: make([]*openfgav1.Tuple, len(state.items)+read),
		err:   state.err,
	}

	copy(newState.items, state.items)
	copy(newState.items[len(state.items):], buf[:read])

	if e != nil {
		newState.err = e
	}

	s.state.Store(newState)
}

// fetchAndWait is a method that fetches items from the underlying storage.TupleIterator and waits for new items to be available.
// It blocks until new items are fetched or an error occurs.
// The items and err pointers are updated with the fetched items and any error encountered.
func (s *sharedIterator) fetchAndWait(items *[]*openfgav1.Tuple, err *error) {
	for {
		state := s.state.Load()

		if s.head < len(state.items) || state.err != nil {
			*items = state.items
			*err = state.err
			return
		}

		s.await.Do(s.fetchMore)
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

	if s.stopped {
		return nil, storage.ErrIteratorDone
	}

	var items []*openfgav1.Tuple
	var err error

	s.fetchAndWait(&items, &err)

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

	return s.current(ctx)
}

// Next returns the next item in the shared iterator and advances the iterator.
// It is used to consume the next item in the iterator.
// If the iterator is stopped or there are no items available, it returns an error.
// It also handles fetching new items if the current head is beyond the available items.
func (s *sharedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.stopped && s.refs.Add(-1) == 0 {
		s.ir.Stop()
	}
	s.stopped = true
}
