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

type Storage struct {
	iters sync.Map
	limit int64
	ctr   atomic.Int64
}

type storageItem struct {
	mu       sync.Mutex
	iter     *sharedIterator
	producer func() (*sharedIterator, error)
}

func (s *storageItem) unwrap() (*sharedIterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.iter == nil {
		it, err := s.producer()
		if err != nil {
			return nil, err
		}
		s.iter = it
		clone := it.clone()
		it.Stop()
		return clone, nil
	}
	return s.iter.clone(), nil
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

// WithMaxTTL sets the time for watchdog will kick and clean up.
func WithMaxTTL(ttl time.Duration) IteratorDatastoreOpt {
	return func(b *IteratorDatastore) {
		b.watchdogTTL = ttl
	}
}

// WithIteratorTargetSize sets the pre-allocated size of each shared tuple.
// This allows iterator from having to reallocate buffer space unless we go over that value.
func WithIteratorTargetSize(targetSize uint32) IteratorDatastoreOpt {
	return func(b *IteratorDatastore) {
		b.iteratorTargetSize = targetSize
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

type IteratorDatastore struct {
	storage.RelationshipTupleReader
	logger             logger.Logger
	method             string
	internalStorage    *Storage
	watchdogTTL        time.Duration
	maxAdmissionTime   time.Duration
	iteratorTargetSize uint32
}

func NewSharedIteratorDatastore(inner storage.RelationshipTupleReader, internalStorage *Storage, opts ...IteratorDatastoreOpt) *IteratorDatastore {
	sf := &IteratorDatastore{
		RelationshipTupleReader: inner,
		logger:                  logger.NewNoopLogger(),
		internalStorage:         internalStorage,
		method:                  "",
		watchdogTTL:             config.DefaultSharedIteratorTTL,
		iteratorTargetSize:      defaultIteratorTargetSize,
		maxAdmissionTime:        config.DefaultSharedIteratorMaxAdmissionTime,
	}

	for _, opt := range opts {
		opt(sf)
	}

	return sf
}

func length(m *sync.Map) int {
	var i int
	m.Range(func(_, _ any) bool {
		i++
		return true
	})
	return i
}

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

	if length(&sf.internalStorage.iters) >= int(sf.internalStorage.limit) {
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadStartingWithUser).Inc()
		return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	}

	newStorageItem := new(storageItem)

	timer := time.AfterFunc(sf.maxAdmissionTime, func() {
		if sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem) {
			sf.internalStorage.ctr.Add(-1)
		}
	})

	newStorageItem.producer = func() (*sharedIterator, error) {
		it, err := sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
		if err != nil {
			return nil, err
		}

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

	value, loaded := sf.internalStorage.iters.LoadOrStore(cacheKey, newStorageItem)
	if !loaded {
		sf.internalStorage.ctr.Add(1)
	}
	item, _ := value.(*storageItem)
	it, err := item.unwrap()
	if err != nil {
		sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem)
		return nil, err
	}
	if it == nil {
		return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	}
	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadStartingWithUser, sf.method, "true",
	).Observe(float64(time.Since(start).Milliseconds()))
	return it, nil
}

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

	if length(&sf.internalStorage.iters) >= int(sf.internalStorage.limit) {
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadUsersetTuples).Inc()
		return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	}

	newStorageItem := new(storageItem)

	timer := time.AfterFunc(sf.maxAdmissionTime, func() {
		if sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem) {
			sf.internalStorage.ctr.Add(-1)
		}
	})

	newStorageItem.producer = func() (*sharedIterator, error) {
		it, err := sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
		if err != nil {
			return nil, err
		}

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

	value, loaded := sf.internalStorage.iters.LoadOrStore(cacheKey, newStorageItem)
	if !loaded {
		sf.internalStorage.ctr.Add(1)
	}
	item, _ := value.(*storageItem)
	it, err := item.unwrap()
	if err != nil {
		sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem)
		return nil, err
	}
	if it == nil {
		return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	}
	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadUsersetTuples, sf.method, "true",
	).Observe(float64(time.Since(start).Milliseconds()))
	return it, nil
}

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

	if length(&sf.internalStorage.iters) >= int(sf.internalStorage.limit) {
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationRead).Inc()
		return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	}

	newStorageItem := new(storageItem)

	timer := time.AfterFunc(sf.maxAdmissionTime, func() {
		if sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem) {
			sf.internalStorage.ctr.Add(-1)
		}
	})

	newStorageItem.producer = func() (*sharedIterator, error) {
		it, err := sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
		if err != nil {
			return nil, err
		}

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

	value, loaded := sf.internalStorage.iters.LoadOrStore(cacheKey, newStorageItem)
	if !loaded {
		sf.internalStorage.ctr.Add(1)
	}
	item, _ := value.(*storageItem)
	it, err := item.unwrap()
	if err != nil {
		sf.internalStorage.iters.CompareAndDelete(cacheKey, newStorageItem)
		return nil, err
	}
	if it == nil {
		return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	}
	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationRead, sf.method, "true",
	).Observe(float64(time.Since(start).Milliseconds()))
	return it, nil
}

var BufferSize = 100

// sharedIterator will be shared with multiple consumers.
type sharedIterator struct {
	mu      sync.Mutex
	cleanup func()
	head    int
	stopped atomic.Bool
	cancel  func()

	err   *atomic.Pointer[error]
	items *atomic.Pointer[[]*openfgav1.Tuple]
	refs  *atomic.Int64
	wg    *sync.WaitGroup
	ch    *atomic.Pointer[chan struct{}]
}

func newSharedIterator(it storage.TupleIterator, cleanup func()) *sharedIterator {
	ctx, cancel := context.WithCancel(context.Background())

	var done bool

	ch := make(chan struct{})
	var pch atomic.Pointer[chan struct{}]
	pch.Store(&ch)

	var refs atomic.Int64
	refs.Add(1)

	var wg sync.WaitGroup

	var err atomic.Pointer[error]
	terr := new(error)
	err.Store(terr)

	var items atomic.Pointer[[]*openfgav1.Tuple]
	titems := make([]*openfgav1.Tuple, 0)
	items.Store(&titems)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer it.Stop()

		buf := make([]*openfgav1.Tuple, BufferSize)

		ptrError := err.Load()

		for !done {
			c := pch.Load()
			select {
			case *c <- struct{}{}:
				var i int
				var e error

				for ; i < BufferSize; i++ {
					t, ierr := it.Next(ctx)
					if ierr != nil {
						e = ierr
						done = true
						break
					}
					buf[i] = t
				}
				ptrItems := items.Load()
				loadedItems := *ptrItems
				loadedItems = append(loadedItems, buf[:i]...)
				items.Store(&loadedItems)
				if e != nil {
					if e == context.Canceled || e == context.DeadlineExceeded {
						e = storage.ErrIteratorDone
					}
					err.Store(&e)
				}
				nc := make(chan struct{})
				pch.CompareAndSwap(c, &nc)
				close(*c)
			case <-ctx.Done():
				done = true
			}
		}
		err.CompareAndSwap(ptrError, &storage.ErrIteratorDone)
		close(*pch.Load())
	}()

	newIter := sharedIterator{
		cleanup: cleanup,
		cancel:  cancel,
		items:   &items,
		err:     &err,
		refs:    &refs,
		wg:      &wg,
		ch:      &pch,
	}

	return &newIter
}

func (s *sharedIterator) clone() *sharedIterator {
	for {
		remaining := s.refs.Load()

		if remaining == 0 {
			return nil
		}

		if s.refs.CompareAndSwap(remaining, remaining+1) {
			return &sharedIterator{
				cleanup: s.cleanup,
				cancel:  s.cancel,
				items:   s.items,
				err:     s.err,
				refs:    s.refs,
				wg:      s.wg,
				ch:      s.ch,
			}
		}
	}
}

func (s *sharedIterator) Head(ctx context.Context) (tup *openfgav1.Tuple, e error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.Head",
	)
	defer span.End()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if s.stopped.Load() {
		return nil, storage.ErrIteratorDone
	}

	items := *s.items.Load()
	err := *s.err.Load()

	for ch := *s.ch.Load(); s.head >= len(items) && err == nil; items, err, ch = *s.items.Load(), *s.err.Load(), *s.ch.Load() {
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if s.head >= len(items) && err != nil {
		if err == storage.ErrIteratorDone {
			return nil, storage.ErrIteratorDone
		}
		telemetry.TraceError(span, err)
		return nil, err
	}

	return items[s.head], nil
}

func (s *sharedIterator) Next(ctx context.Context) (tup *openfgav1.Tuple, e error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.Next",
	)
	defer span.End()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if s.stopped.Load() {
		return nil, storage.ErrIteratorDone
	}

	items := *s.items.Load()
	err := *s.err.Load()

	for ch := *s.ch.Load(); s.head >= len(items) && err == nil; items, err, ch = *s.items.Load(), *s.err.Load(), *s.ch.Load() {
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if s.head >= len(items) && err != nil {
		if err == storage.ErrIteratorDone {
			return nil, storage.ErrIteratorDone
		}
		telemetry.TraceError(span, err)
		return nil, err
	}

	defer func() {
		s.head++
	}()

	return items[s.head], nil
}

func (s *sharedIterator) Stop() {
	if s.stopped.CompareAndSwap(false, true) && s.refs.Add(-1) == 0 {
		s.cleanup()
		s.cancel()
		s.wg.Wait()
	}
}
