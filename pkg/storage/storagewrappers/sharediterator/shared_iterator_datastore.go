package sharediterator

import (
	"context"
	"errors"
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

	sharedIteratorWatchDog = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "shared_iterator_watchdog_timer_triggered",
		Help:      "Total number of times watchdog timer is triggered.",
	})

	sharedIteratorCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: build.ProjectName,
		Name:      "shared_iterator_count",
		Help:      "The current number of items of shared iterator.",
	})

	errSharedIteratorWatchdog               = errors.New("shared iterator watchdog timeout")
	errSharedIteratorAfterLastAdmissionTime = errors.New("shared iterator cloned after last admission time")
)

const (
	defaultSharedIteratorLimit = 1000000
	defaultIteratorTargetSize  = 1000
)

type Storage struct {
	mu sync.Mutex /* There are two locks (the mutex in Storage [aka big lock]
	and the mutex in the sharedIterator [aka small lock]) in this file. To avoid deadlock, you MUST release the small
	lock before obtaining the big lock.
	*/
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
		iters: make(map[string]*internalSharedIterator, 1000),
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

type internalSharedIterator struct {
	counter uint64 // indirectly protected by the surrounding mutex. Therefor, it's usage must be made with caution.
	iter    *sharedIterator
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

	sf.internalStorage.mu.Lock()

	keyItem, found := sf.internalStorage.iters[cacheKey]

	if found {
		keyItem.counter++
		sf.internalStorage.mu.Unlock()

		span.SetAttributes(attribute.Bool("found", true))

		// by the time we have access to keyItem.iter.mu, we know that
		// the inner is already set OR the initializationErr is set (for which clone
		// will fail).
		item, err := keyItem.iter.clone()
		if err != nil {
			sf.deref(cacheKey)
			telemetry.TraceError(span, err)
			return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
		}
		sharedIteratorQueryHistogram.WithLabelValues(
			storagewrappersutil.OperationReadStartingWithUser, sf.method, "true",
		).Observe(float64(time.Since(start).Milliseconds()))
		return item, nil
	}
	if len(sf.internalStorage.iters) >= sf.internalStorage.limit {
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("overlimit", true))

		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadStartingWithUser).Inc()
		// we cannot share this iterator because we have reached the size limit.
		return sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	}

	newIterator := newSharedIterator(sf, cacheKey, sf.watchdogTTL, sf.maxAdmissionTime, sf.iteratorTargetSize)
	newIterator.mu.Lock()

	sf.internalStorage.iters[cacheKey] = &internalSharedIterator{
		counter: 1,
		iter:    newIterator,
	}
	sf.internalStorage.mu.Unlock()
	sharedIteratorCount.Inc()
	span.SetAttributes(attribute.Bool("found", false))

	actual, err := sf.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	if err != nil {
		newIterator.initializationErr = err
		newIterator.mu.Unlock()
		sf.deref(cacheKey)
		telemetry.TraceError(span, err)
		return nil, err
	}

	// note that this is protected by the newIterator.Lock. Therefore, any clone will not
	// have access to inner until it is set.
	newIterator.inner = actual
	newIterator.mu.Unlock()

	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadStartingWithUser, sf.method, "false",
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
	span.SetAttributes(attribute.String("consistency_preference", options.Consistency.Preference.String()))

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	}
	start := time.Now()

	cacheKey := storagewrappersutil.ReadUsersetTuplesKey(store, filter)
	span.SetAttributes(attribute.String("cache_key", cacheKey))

	sf.internalStorage.mu.Lock()

	keyItem, found := sf.internalStorage.iters[cacheKey]

	if found {
		keyItem.counter++
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", true))

		// by the time we have access to keyItem.iter.mu, we know that
		// the inner is already set OR the initializationErr is set (for which clone
		// will fail).
		item, err := keyItem.iter.clone()
		if err != nil {
			sf.deref(cacheKey)
			telemetry.TraceError(span, err)
			return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
		}

		sharedIteratorQueryHistogram.WithLabelValues(
			storagewrappersutil.OperationReadUsersetTuples, sf.method, "true",
		).Observe(float64(time.Since(start).Milliseconds()))

		return item, nil
	}

	if len(sf.internalStorage.iters) >= sf.internalStorage.limit {
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("overlimit", true))
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationReadUsersetTuples).Inc()
		// we cannot share this iterator because we have reached the size limit.
		return sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	}

	newIterator := newSharedIterator(sf, cacheKey, sf.watchdogTTL, sf.maxAdmissionTime, sf.iteratorTargetSize)
	newIterator.mu.Lock()

	sf.internalStorage.iters[cacheKey] = &internalSharedIterator{
		counter: 1,
		iter:    newIterator,
	}
	sf.internalStorage.mu.Unlock()
	sharedIteratorCount.Inc()
	span.SetAttributes(attribute.Bool("found", false))

	actual, err := sf.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	if err != nil {
		newIterator.initializationErr = err
		newIterator.mu.Unlock()

		sf.deref(cacheKey)
		telemetry.TraceError(span, err)
		return nil, err
	}

	newIterator.inner = actual
	newIterator.mu.Unlock()

	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationReadUsersetTuples, sf.method, "false",
	).Observe(float64(time.Since(start).Milliseconds()))
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
	span.SetAttributes(attribute.String("consistency_preference", options.Consistency.Preference.String()))

	if options.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	}
	start := time.Now()

	cacheKey := storagewrappersutil.ReadKey(store, tupleKey)
	span.SetAttributes(attribute.String("cache_key", cacheKey))

	sf.internalStorage.mu.Lock()

	keyItem, found := sf.internalStorage.iters[cacheKey]
	if found {
		keyItem.counter++
		sf.internalStorage.mu.Unlock()
		span.SetAttributes(attribute.Bool("found", true))

		item, err := keyItem.iter.clone()
		if err != nil {
			sf.deref(cacheKey)
			telemetry.TraceError(span, err)
			return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
		}
		sharedIteratorQueryHistogram.WithLabelValues(
			storagewrappersutil.OperationRead, sf.method, "true",
		).Observe(float64(time.Since(start).Milliseconds()))
		return item, nil
	}
	if len(sf.internalStorage.iters) >= sf.internalStorage.limit {
		sf.internalStorage.mu.Unlock()
		sharedIteratorBypassed.WithLabelValues(storagewrappersutil.OperationRead).Inc()

		// we cannot share this iterator because we have reached the size limit.
		return sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	}

	newIterator := newSharedIterator(sf, cacheKey, sf.watchdogTTL, sf.maxAdmissionTime, sf.iteratorTargetSize)
	newIterator.mu.Lock()

	sf.internalStorage.iters[cacheKey] = &internalSharedIterator{
		counter: 1,
		iter:    newIterator,
	}
	sf.internalStorage.mu.Unlock()
	sharedIteratorCount.Inc()
	span.SetAttributes(attribute.Bool("found", false))

	actual, err := sf.RelationshipTupleReader.Read(ctx, store, tupleKey, options)
	if err != nil {
		newIterator.initializationErr = err
		newIterator.mu.Unlock()

		sf.deref(cacheKey)
		telemetry.TraceError(span, err)
		return nil, err
	}
	newIterator.inner = actual
	newIterator.mu.Unlock()

	sharedIteratorQueryHistogram.WithLabelValues(
		storagewrappersutil.OperationRead, sf.method, "false",
	).Observe(float64(time.Since(start).Milliseconds()))
	return newIterator, nil
}

// decrement cacheKey from internal reference count
// If reference count is 0, remove cacheKey.
func (sf *IteratorDatastore) deref(cacheKey string) {
	sf.internalStorage.mu.Lock()
	item, ok := sf.internalStorage.iters[cacheKey]
	if !ok {
		sf.internalStorage.mu.Unlock()
		sf.logger.Error("failed to dereference cache key", zap.String("cacheKey", cacheKey))
		return
	}
	item.counter--
	if item.counter == 0 {
		delete(sf.internalStorage.iters, cacheKey)
		sf.internalStorage.mu.Unlock()
		sharedIteratorCount.Dec()
		item.iter.cleanup() // will grab `mu` internally, thus must be yielded before called.
		return
	}
	sf.internalStorage.mu.Unlock()
}

// watchdogTimeout will be called by the sharedIterator to clean itself up upon watchdog timeout.
// This is needed to avoid leaks when the shared iterator did not stop.
func (sf *IteratorDatastore) watchdogTimeout(cacheKey string, iter *sharedIterator) {
	sf.internalStorage.mu.Lock()
	item, ok := sf.internalStorage.iters[cacheKey]
	if !ok {
		// It is possible that the watchdogTimer runs after the item has been dereferenced.
		// In this case, we do nothing
		sf.internalStorage.mu.Unlock()

		sf.logger.Debug("shared iterator watchdog timeout failed to deref key", zap.String("cacheKey", cacheKey))
		return
	}
	if item.iter != iter {
		// This is the case where the watchdogTimeout runs after the item has been dereferenced and then re-created.
		// In this case, we only want to clean the timed-out shared iterator up - not the newly re-created iterator.
		sf.internalStorage.mu.Unlock()
		sf.logger.Debug("shared iterator watchdog timeout deref key but new shared iter is created in the meantime", zap.String("cacheKey", cacheKey))

		iter.cleanup()
		return
	}
	// no one has deleted / recreate the map's entry. Time to clean myself up.
	delete(sf.internalStorage.iters, cacheKey)
	sf.internalStorage.mu.Unlock()
	sharedIteratorCount.Dec()

	item.iter.cleanup()
}

// sharedIterator will be shared with multiple consumers.
type sharedIterator struct {
	manager          *IteratorDatastore // non-changing
	key              string             // non-changing
	maxAliveTime     time.Duration      // non-changing
	maxAdmissionTime time.Time          // non-changing
	head             int

	mu                *sync.RWMutex         // We expect the contention should be minimal. TODO: minimize mu holding time.
	initializationErr error                 // shared - protected by mu. Although it is shared, it is only inspected by clone().
	items             *[]*openfgav1.Tuple   // shared - protected by mu
	inner             storage.TupleIterator // shared - protected by mu
	sharedErr         *error                // shared - protected by mu
	stopped           bool                  // not shared across clone - but protected by mu
	watchdogTimer     *time.Timer           /* shared - protected by mu. Watchdog timer is used to protect from leak
	when the shared iterator was not stopped. Every time there is an action on the shared iterator, the watchdog
	timer's timeout is postponed by the maxAliveTime. When the timer is trigger, watchdogTimeout() will be invoked
	which will call the manager to clean up this iterator.
	*/
}

func newSharedIterator(manager *IteratorDatastore, key string, maxAliveTime time.Duration, maxAdmissionTime time.Duration, targetSize uint32) *sharedIterator {
	items := make([]*openfgav1.Tuple, 0, targetSize)
	newIter := &sharedIterator{
		manager:          manager,
		key:              key,
		maxAliveTime:     maxAliveTime,
		maxAdmissionTime: time.Now().Add(maxAdmissionTime),

		mu:        &sync.RWMutex{},
		items:     &items,
		inner:     nil,
		sharedErr: new(error),
		//watchdogTimeoutErr: new(error),
	}
	newIter.watchdogTimer = time.AfterFunc(maxAliveTime, newIter.watchdogTimeout)
	return newIter
}

// It is assumed that mu is held by the parent while cloning.
func (s *sharedIterator) clone() (*sharedIterator, error) {
	if s.initializationErr != nil {
		return nil, s.initializationErr
	}
	if time.Now().After(s.maxAdmissionTime) {
		// To avoid stale data, we want to prevent shared iterator from using the clone if it is created after
		// maxAdmissionTime. When we return, the clone will default to skip the shared iterator.
		return nil, errSharedIteratorAfterLastAdmissionTime
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	newIter := &sharedIterator{
		manager:      s.manager,
		key:          s.key,
		maxAliveTime: s.maxAliveTime,
		// the start time and the maxAdmissionTime is only used for checking
		// whether we need to clone. As such, it is not copied into the cloned copy.

		mu:            s.mu,
		items:         s.items,
		inner:         s.inner,
		sharedErr:     s.sharedErr,
		watchdogTimer: s.watchdogTimer,
		//watchdogTimeoutErr: s.watchdogTimeoutErr,
	}
	newIter.watchdogTimer.Reset(s.maxAliveTime)
	return newIter, nil
}

func (s *sharedIterator) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inner != nil {
		s.inner.Stop()
	}
	if s.watchdogTimer != nil {
		s.watchdogTimer.Stop()
	}
	s.inner = nil
	s.watchdogTimer = nil
}

// when watchdogTimeout is invoked, it will revoke the current iterator from usage.
// This will mark the iterator as error and stop on its behalf
// All further usage of Head/Next() will return error.
func (s *sharedIterator) watchdogTimeout() {
	s.mu.Lock()
	// *s.watchdogTimeoutErr = errSharedIteratorWatchdog
	*s.sharedErr = errSharedIteratorWatchdog
	s.mu.Unlock()
	sharedIteratorWatchDog.Inc()
	s.manager.watchdogTimeout(s.key, s)
}

func (s *sharedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.Head",
	)
	defer span.End()

	s.mu.RLock()
	// if we have seen timeout, anything else is error
	if s.sharedErr != nil && errors.Is(*s.sharedErr, errSharedIteratorWatchdog) {
		defer s.mu.RUnlock()
		return nil, *s.sharedErr
	}

	/*
		if s.watchdogTimer != nil {
			s.watchdogTimer.Reset(s.maxAliveTime)
		}

	*/

	if s.stopped {
		s.mu.RUnlock()
		span.SetAttributes(attribute.Bool("stopped", true))
		return nil, storage.ErrIteratorDone
	}

	if s.head < len(*s.items) {
		span.SetAttributes(attribute.Bool("newItem", false))
		defer s.mu.RUnlock()
		return (*s.items)[s.head], nil
	}

	// When we get to here, it means that we need new item
	s.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	span.SetAttributes(attribute.Bool("newItem", true))
	// there is a rare chance that timeout / stopped / has more item in between

	if s.sharedErr != nil && errors.Is(*s.sharedErr, errSharedIteratorWatchdog) {
		return nil, *s.sharedErr
	}

	if s.stopped {
		span.SetAttributes(attribute.Bool("stopped", true))
		return nil, storage.ErrIteratorDone
	}

	if s.head < len(*s.items) {
		span.SetAttributes(attribute.Bool("newItem", false))
		return (*s.items)[s.head], nil
	}

	// If there is an error, no need to get any more items, and we just return the error
	if s.sharedErr != nil && *s.sharedErr != nil {
		span.SetAttributes(attribute.Bool("sharedError", true))
		telemetry.TraceError(span, *s.sharedErr)
		return nil, *s.sharedErr
	}

	item, err := s.inner.Next(ctx)
	if err != nil {
		telemetry.TraceError(span, err)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, err
		}
		if s.sharedErr != nil && !errors.Is(err, errSharedIteratorWatchdog) {
			// watchdog error has higher priority
			*s.sharedErr = err
		}
		return nil, err
	}
	*s.items = append(*s.items, item)
	return item, nil
}

func (s *sharedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	ctx, span := tracer.Start(
		ctx,
		"sharedIterator.Next",
	)
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sharedErr != nil && errors.Is(*s.sharedErr, errSharedIteratorWatchdog) {
		// watchdog timer has higher priority
		return nil, *s.sharedErr
	}

	/*
		if s.watchdogTimer != nil {
			s.watchdogTimer.Reset(s.maxAliveTime)
		}

	*/

	if s.stopped {
		span.SetAttributes(attribute.Bool("stopped", true))
		return nil, storage.ErrIteratorDone
	}

	if s.head < len(*s.items) {
		currentHead := s.head
		s.head++
		return (*s.items)[currentHead], nil
	}

	// at this point, we know that for sure we will need to get more data.

	if s.sharedErr != nil && *s.sharedErr != nil {
		// we are going to get more items. However, if we see error
		// previously, we will not need to get more items
		// and can simply return the same error.
		span.SetAttributes(attribute.Bool("sharedError", true))
		telemetry.TraceError(span, *s.sharedErr)
		// we don't need to decrement the head counter because it is only
		// fot this particular shared iterator.
		return nil, *s.sharedErr
	}

	span.SetAttributes(attribute.Bool("newItem", true))

	// it is entirely possible that some shared client has called Next()
	// and fetch more data. There are no harm in getting more data

	item, err := s.inner.Next(ctx)
	if err != nil {
		telemetry.TraceError(span, err)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, err
		}
		if s.sharedErr != nil && !errors.Is(err, errSharedIteratorWatchdog) {
			// watch dog error has higher priority and we do not want to override
			*s.sharedErr = err
		}
		return nil, err
	}
	*s.items = append(*s.items, item)
	s.head++
	return item, nil
}

func (s *sharedIterator) Stop() {
	s.mu.Lock()
	if s.stopped {
		// It is perfectly possible that iterator calling stop more than once.
		// However, we only want to decrement the count on the first stop.
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()

	/*
		if s.watchdogTimer != nil {
			s.watchdogTimer.Reset(s.maxAliveTime)
		}

	*/
	s.manager.deref(s.key)
}
