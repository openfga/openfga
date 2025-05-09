package sharediterator

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
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
		Buckets:                         []float64{1, 5, 10, 25, 50, 100, 200, 300, 1000},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"operation", "shared"})

	sharedIteratorBypassed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "shared_iterator_bypassed",
		Help:      "Total number of iterators bypassed by the shared iterator layer because the internal map size exceed specified limit.",
	}, []string{"operation"})

	errSharedIteratorWatchdog = errors.New("shared iterator watchdog timeout")
)

const (
	defaultSharedIteratorLimit        = 1000000
	defaultSharedIteratorMaxAliveTime = 4 * time.Minute
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

// WithMaxAliveTime sets the time for watchdog will kick and clean up.
func WithMaxAliveTime(maxAliveTime time.Duration) IteratorDatastoreOpt {
	return func(b *IteratorDatastore) {
		b.maxAliveTime = maxAliveTime
	}
}

type IteratorDatastore struct {
	storage.RelationshipTupleReader
	logger          logger.Logger
	internalStorage *Storage
	maxAliveTime    time.Duration
}

type internalSharedIterator struct {
	counter uint64
	iter    *sharedIterator
}

func NewSharedIteratorDatastore(inner storage.RelationshipTupleReader, internalStorage *Storage, opts ...IteratorDatastoreOpt) *IteratorDatastore {
	sf := &IteratorDatastore{
		RelationshipTupleReader: inner,
		logger:                  logger.NewNoopLogger(),
		internalStorage:         internalStorage,
		maxAliveTime:            defaultSharedIteratorMaxAliveTime,
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
		keyItem.iter.mu.Lock()
		defer keyItem.iter.mu.Unlock()
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", true))

		sharedIteratorQueryHistogram.WithLabelValues(
			storagewrappersutil.OperationReadStartingWithUser, strconv.FormatBool(found),
		).Observe(float64(time.Since(start).Milliseconds()))

		item, err := keyItem.iter.clone()
		if err != nil {
			go func() {
				_, _ = sf.deref(cacheKey)
			}()
			return nil, err
		}
		return item, nil
	}
	if len(sf.internalStorage.iters) >= sf.internalStorage.limit {
		span.SetAttributes(attribute.Bool("overlimit", true))

		sf.internalStorage.mu.Unlock()
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadStartingWithUser).Inc()
		// we cannot share this iterator because we have reached the size limit.
		return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	}

	newIterator := newSharedIterator(sf, cacheKey, nil, sf.maxAliveTime)
	newIterator.mu.Lock()
	defer newIterator.mu.Unlock()
	sf.internalStorage.iters[cacheKey] = &internalSharedIterator{
		counter: 1,
		iter:    newIterator,
	}
	sf.internalStorage.mu.Unlock()
	span.SetAttributes(attribute.Bool("found", false))

	actual, err := sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	if err != nil {
		newIterator.queryErr = err
		_, _ = sf.deref(cacheKey)
		return nil, err
	}
	newIterator.inner = actual
	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadStartingWithUser, strconv.FormatBool(found),
	).Observe(float64(time.Since(start).Milliseconds()))

	return newIterator, nil
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
		span.SetAttributes(attribute.Bool("found", true))
		keyItem.counter++
		keyItem.iter.mu.Lock()
		defer keyItem.iter.mu.Unlock()
		sf.internalStorage.mu.Unlock()

		sharedIteratorQueryHistogram.WithLabelValues(
			storagewrappersutil.OperationReadUsersetTuples, strconv.FormatBool(found),
		).Observe(float64(time.Since(start).Milliseconds()))

		item, err := keyItem.iter.clone()
		if err != nil {
			go func() {
				_, _ = sf.deref(cacheKey)
			}()
			return nil, err
		}
		return item, nil
	}

	if len(sf.internalStorage.iters) >= sf.internalStorage.limit {
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("overlimit", true))
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadUsersetTuples).Inc()
		// we cannot share this iterator because we have reached the size limit.
		return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	}

	newIterator := newSharedIterator(sf, cacheKey, nil, sf.maxAliveTime)
	newIterator.mu.Lock()
	defer newIterator.mu.Unlock()

	sf.internalStorage.iters[cacheKey] = &internalSharedIterator{
		counter: 1,
		iter:    newIterator,
	}
	sf.internalStorage.mu.Unlock()
	span.SetAttributes(attribute.Bool("found", false))

	actual, err := sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	if err != nil {
		newIterator.queryErr = err
		_, _ = sf.deref(cacheKey)
		return nil, err
	}
	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadUsersetTuples, strconv.FormatBool(found),
	).Observe(float64(time.Since(start).Milliseconds()))
	newIterator.inner = actual
	return newIterator, nil
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
		keyItem.iter.mu.Lock()
		defer keyItem.iter.mu.Unlock()
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", true))
		sharedIteratorQueryHistogram.WithLabelValues(
			storagewrappersutil.OperationRead, strconv.FormatBool(found),
		).Observe(float64(time.Since(start).Milliseconds()))

		item, err := keyItem.iter.clone()
		if err != nil {
			go func() {
				_, _ = sf.deref(cacheKey)
			}()
			return nil, err
		}
		return item, nil
	}
	if len(sf.internalStorage.iters) >= sf.internalStorage.limit {
		sf.internalStorage.mu.Unlock()
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationRead).Inc()

		// we cannot share this iterator because we have reached the size limit.
		return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	}

	newIterator := newSharedIterator(sf, cacheKey, nil, sf.maxAliveTime)
	newIterator.mu.Lock()
	defer newIterator.mu.Unlock()

	sf.internalStorage.iters[cacheKey] = &internalSharedIterator{
		counter: 1,
		iter:    newIterator,
	}
	sf.internalStorage.mu.Unlock()
	span.SetAttributes(attribute.Bool("found", false))

	actual, err := sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	if err != nil {
		newIterator.queryErr = err
		_, _ = sf.deref(cacheKey)
		return nil, err
	}
	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationRead, strconv.FormatBool(found),
	).Observe(float64(time.Since(start).Milliseconds()))
	newIterator.inner = actual
	return newIterator, nil
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
	manager      *IteratorDatastore // non-changing
	key          string             // non-changing
	stopped      atomic.Bool
	head         int
	maxAliveTime time.Duration
	queryErr     error

	mu                 *sync.Mutex
	items              *[]*openfgav1.Tuple   // shared - protected by mu
	inner              storage.TupleIterator // shared - protected by mu
	sharedErr          *error                // shared - protected by mu
	watchdogTimeoutErr error                 // shared - protected by mu
	watchdogTimer      *time.Timer
}

func newSharedIterator(manager *IteratorDatastore, key string, inner storage.TupleIterator, maxAliveTime time.Duration) *sharedIterator {
	newIter := &sharedIterator{
		manager:      manager,
		key:          key,
		head:         0,
		maxAliveTime: maxAliveTime,

		mu:        &sync.Mutex{},
		items:     new([]*openfgav1.Tuple),
		inner:     inner,
		sharedErr: new(error),
	}
	newIter.watchdogTimer = time.AfterFunc(maxAliveTime, newIter.watchdogTimeout)
	return newIter
}

func (s *sharedIterator) clone() (*sharedIterator, error) {
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	newIter := &sharedIterator{
		manager:      s.manager,
		key:          s.key,
		head:         0,
		maxAliveTime: s.maxAliveTime,

		mu:        s.mu,
		items:     s.items,
		inner:     s.inner,
		sharedErr: s.sharedErr,
	}
	newIter.watchdogTimer = time.AfterFunc(s.maxAliveTime, newIter.watchdogTimeout)
	return newIter, nil
}

// when watchdogTimeout is invoked, it will revoke the current iterator from usage.
// This will mark the iterator as error and stop on its behalf
// All further usage of Head/Next() will return error.
func (s *sharedIterator) watchdogTimeout() {
	s.mu.Lock()
	s.watchdogTimeoutErr = errSharedIteratorWatchdog
	s.mu.Unlock()
	s.Stop()
}

func (s *sharedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.Head",
	)
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.watchdogTimer != nil {
		s.watchdogTimer.Reset(s.maxAliveTime)
	}

	if s.watchdogTimeoutErr != nil {
		return nil, s.watchdogTimeoutErr
	}

	if s.stopped.Load() {
		span.SetAttributes(attribute.Bool("stopped", true))
		return nil, storage.ErrIteratorDone
	}

	if s.head == len(*s.items) {
		// If there is an error, no need to get any more items, and we just return the error
		if s.sharedErr != nil && *s.sharedErr != nil {
			span.SetAttributes(attribute.String("existingError", (*s.sharedErr).Error()))
			return nil, *s.sharedErr
		}

		span.SetAttributes(attribute.Bool("newItem", true))
		item, err := s.inner.Next(ctx)
		if err != nil {
			span.SetAttributes(attribute.String("newError", err.Error()))
			*s.sharedErr = err
			return nil, err
		}
		*s.items = append(*s.items, item)
		return item, nil
	}

	span.SetAttributes(attribute.Bool("newItem", false))
	return (*s.items)[s.head], nil
}

func (s *sharedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.Next",
	)
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.watchdogTimer != nil {
		s.watchdogTimer.Reset(s.maxAliveTime)
	}

	if s.watchdogTimeoutErr != nil {
		return nil, s.watchdogTimeoutErr
	}

	if s.stopped.Load() {
		span.SetAttributes(attribute.Bool("stopped", true))
		return nil, storage.ErrIteratorDone
	}
	if s.head < len(*s.items) {
		currentHead := s.head
		s.head++
		return (*s.items)[currentHead], nil
	}
	if s.sharedErr != nil && *s.sharedErr != nil {
		// we are going to get more items. However, if we see error
		// previously, we will not need to get more items
		// and can simply return the same error.
		span.SetAttributes(attribute.String("existingError", (*s.sharedErr).Error()))

		return nil, *s.sharedErr
	}
	span.SetAttributes(attribute.Bool("newItem", true))

	item, err := s.inner.Next(ctx)
	if err != nil {
		span.SetAttributes(attribute.String("newError", err.Error()))
		*s.sharedErr = err
		return nil, err
	}
	*s.items = append(*s.items, item)
	s.head++
	return item, nil
}

func (s *sharedIterator) Stop() {
	s.mu.Lock()
	if s.watchdogTimer != nil {
		s.watchdogTimer.Stop()
	}
	s.mu.Unlock()

	if s.stopped.Swap(true) {
		// It is perfectly possible that iterator calling stop more than once.
		// However, we only want to decrement the count on the first stop.
		return
	}
	lastItem, _ := s.manager.deref(s.key)
	if lastItem {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.inner.Stop()
	}
}
