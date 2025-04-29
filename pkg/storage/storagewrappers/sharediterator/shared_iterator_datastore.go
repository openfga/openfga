package sharediterator

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers/storagewrappersutil"
)

var (
	tracer = otel.Tracer("openfga/pkg/storagewrappers/sharediterator")

	_ storage.RelationshipTupleReader = (*IteratorDatastore)(nil)
	_ storage.TupleIterator           = (*sharedIterator)(nil)

	sharedIteratorQueryHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "shared_iterator_query_ms",
		Help:                            "The duration (in ms) of a shared iterator query labeled by operation and shared.",
		Buckets:                         []float64{1, 2, 5, 10, 25, 50, 80, 100, 150, 200, 300, 1000},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"operation", "shared"})

	sharedIteratorBypassed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "shared_iterator_bypassed",
		Help:      "Total number of iterators bypassed by the shared iterator layer because the internal map size exceed specified limit.",
	}, []string{"operation"})
)

const (
	defaultSharedIteratorLimit = 1000000
)

type Storage struct {
	mu    sync.Mutex
	iters map[string]*internalSharedIterator // protected by mu
	limit int
}

type DatastoreStorageOpt func(*Storage)

// WithSharedIteratorDatastoreStorageLimit sets the limit on the number of items in SF iterator iters.
func WithSharedIteratorDatastoreStorageLimit(limit int) DatastoreStorageOpt {
	return func(b *Storage) {
		b.limit = limit
	}
}

func NewSharedIteratorDatastoreStorage(opts ...DatastoreStorageOpt) *Storage {
	newStorage := &Storage{
		mu:    sync.Mutex{},
		limit: defaultSharedIteratorLimit,
		iters: map[string]*internalSharedIterator{},
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

type IteratorDatastore struct {
	storage.RelationshipTupleReader
	logger          logger.Logger
	internalStorage *Storage
}

type internalSharedIterator struct {
	counter uint64
	// in the future, there will be reference to the shared iterator
}

func NewSharedIteratorDatastore(inner storage.RelationshipTupleReader, internalStorage *Storage, opts ...IteratorDatastoreOpt) *IteratorDatastore {
	sf := &IteratorDatastore{
		RelationshipTupleReader: inner,
		logger:                  logger.NewNoopLogger(),
		internalStorage:         internalStorage,
	}

	for _, opt := range opts {
		opt(sf)
	}

	return sf
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
	start := time.Now()

	cacheKey, err := storagewrappersutil.ReadStartingWithUserKey(store, filter)
	if err != nil {
		// should never happen
		return nil, err
	}
	span.SetAttributes(attribute.String("cache_key", cacheKey))

	sf.internalStorage.mu.Lock()

	keyItem, found := sf.internalStorage.iters[cacheKey]

	if found {
		keyItem.counter++
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", true))

		// For now, this will be identical to the not found path.  When the SF iterator datastore
		// is fully implemented, the found path will look up the shared iterator and create
		// a clone from it.
	} else {
		if len(sf.internalStorage.iters) >= sf.internalStorage.limit {
			sf.internalStorage.mu.Unlock()
			sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadStartingWithUser).Inc()
			// we cannot share this iterator because we have reached the size limit.
			return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
		}

		sf.internalStorage.iters[cacheKey] = &internalSharedIterator{
			counter: 1,
		}
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", false))
	}
	actual, err := sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	if err != nil {
		go func() {
			_, _ = sf.deref(cacheKey)
		}()
		return nil, err
	}
	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadStartingWithUser, strconv.FormatBool(found),
	).Observe(float64(time.Since(start).Milliseconds()))

	return newSharedIterator(sf, cacheKey, actual), nil
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
	start := time.Now()

	cacheKey := storagewrappersutil.ReadUsersetTuplesKey(store, filter)
	span.SetAttributes(attribute.String("cache_key", cacheKey))

	sf.internalStorage.mu.Lock()

	keyItem, found := sf.internalStorage.iters[cacheKey]

	if found {
		keyItem.counter++
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", true))

		// For now, this will be identical to the not found path.  When the SF iterator datastore
		// is fully implemented, the found path will look up the shared iterator and create
		// a clone from it.
	} else {
		if len(sf.internalStorage.iters) >= sf.internalStorage.limit {
			sf.internalStorage.mu.Unlock()
			sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadUsersetTuples).Inc()
			// we cannot share this iterator because we have reached the size limit.
			return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
		}

		sf.internalStorage.iters[cacheKey] = &internalSharedIterator{
			counter: 1,
		}
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", false))
	}
	actual, err := sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	if err != nil {
		go func() {
			_, _ = sf.deref(cacheKey)
		}()
		return nil, err
	}
	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadUsersetTuples, strconv.FormatBool(found),
	).Observe(float64(time.Since(start).Milliseconds()))

	return newSharedIterator(sf, cacheKey, actual), nil
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
	start := time.Now()

	cacheKey := storagewrappersutil.ReadKey(store, tupleKey)
	span.SetAttributes(attribute.String("cache_key", cacheKey))

	sf.internalStorage.mu.Lock()

	keyItem, found := sf.internalStorage.iters[cacheKey]
	if found {
		keyItem.counter++
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", true))

		// For now, this will be identical to the not found path.  When the SF iterator datastore
		// is fully implemented, the found path will look up the shared iterator and create
		// a clone from it.
	} else {
		if len(sf.internalStorage.iters) >= sf.internalStorage.limit {
			sf.internalStorage.mu.Unlock()
			sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationRead).Inc()

			// we cannot share this iterator because we have reached the size limit.
			return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
		}

		sf.internalStorage.iters[cacheKey] = &internalSharedIterator{
			counter: 1,
		}
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", false))
	}
	actual, err := sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	if err != nil {
		go func() {
			_, _ = sf.deref(cacheKey)
		}()
		return nil, err
	}
	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationRead, strconv.FormatBool(found),
	).Observe(float64(time.Since(start).Milliseconds()))

	return newSharedIterator(sf, cacheKey, actual), nil
}

// decrement cacheKey from internal reference count
// If reference count is 0, remove cacheKey.
// Return whether item is removed.
func (sf *IteratorDatastore) deref(cacheKey string) (bool, error) {
	sf.internalStorage.mu.Lock()
	defer sf.internalStorage.mu.Unlock()
	item, ok := sf.internalStorage.iters[cacheKey]
	if !ok {
		sf.logger.Error("failed to dereference cache key", zap.String("cacheKey", cacheKey))
		return false, fmt.Errorf("failed to dereference cache key: %s", cacheKey)
	}
	item.counter--
	if item.counter == 0 {
		delete(sf.internalStorage.iters, cacheKey)
		return true, nil
	}
	return false, nil
}

// sharedIterator will be shared with multiple consumers.
// For now, this is a skeleton and no one is actually sharing the iterator.
type sharedIterator struct {
	manager *IteratorDatastore
	key     string
	inner   storage.TupleIterator
	stopped bool
}

func newSharedIterator(manager *IteratorDatastore, key string, inner storage.TupleIterator) *sharedIterator {
	return &sharedIterator{
		manager: manager,
		key:     key,
		inner:   inner,
		stopped: false,
	}
}

func (s *sharedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return s.inner.Head(ctx)
}

func (s *sharedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	return s.inner.Next(ctx)
}

func (s *sharedIterator) Stop() {
	// For now, we don't check whether this is the last item in the shared iterator
	// as this is just a skeleton.  When the actual shared iterator is implemented,
	// we will stop the inner iterator only if this is the last reference.
	if !s.stopped {
		// It is perfectly possible that iterator calling stop more than once.
		// However, we only want to decrement the count on the first stop.
		_, _ = s.manager.deref(s.key)
	}
	s.stopped = true
	s.inner.Stop()
}
