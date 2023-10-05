package storagewrappers

import (
	"context"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const timeWaitingSpanAttribute = "time_waiting"

var _ storage.RelationshipTupleReader = (*boundedConcurrencyTupleReader)(nil)

var (
	boundedReadDelayMsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "datastore_bounded_read_delay_ms",
		Help:                            "Time spent waiting for Read, ReadUserTuple and ReadUsersetTuples calls to the datastore",
		Buckets:                         []float64{1, 3, 5, 10, 25, 50, 100, 1000, 5000}, // milliseconds. Upper bound is config.UpstreamTimeout
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"grpc_service", "grpc_method"})
)

type boundedConcurrencyTupleReader struct {
	storage.RelationshipTupleReader
	limiter chan struct{}
}

// NewBoundedConcurrencyTupleReader returns a wrapper over a datastore that makes sure that there are, at most,
// "concurrency" concurrent calls to Read, ReadUserTuple and ReadUsersetTuples.
// Consumers can then rest assured that one client will not hoard all the database connections available.
func NewBoundedConcurrencyTupleReader(wrapped storage.RelationshipTupleReader, concurrency uint32) *boundedConcurrencyTupleReader {
	return &boundedConcurrencyTupleReader{
		RelationshipTupleReader: wrapped,
		limiter:                 make(chan struct{}, concurrency),
	}
}

func (b *boundedConcurrencyTupleReader) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey) (*openfgav1.Tuple, error) {
	b.waitForLimiter(ctx)

	defer func() {
		<-b.limiter
	}()

	return b.RelationshipTupleReader.ReadUserTuple(ctx, store, tupleKey)
}

func (b *boundedConcurrencyTupleReader) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey) (storage.TupleIterator, error) {
	b.waitForLimiter(ctx)

	defer func() {
		<-b.limiter
	}()

	return b.RelationshipTupleReader.Read(ctx, store, tupleKey)
}

func (b *boundedConcurrencyTupleReader) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	b.waitForLimiter(ctx)

	defer func() {
		<-b.limiter
	}()

	return b.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter)
}

func (b *boundedConcurrencyTupleReader) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {
	b.waitForLimiter(ctx)

	defer func() {
		<-b.limiter
	}()

	return b.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter)
}

func (b *boundedConcurrencyTupleReader) waitForLimiter(ctx context.Context) {
	start := time.Now()

	b.limiter <- struct{}{}

	end := time.Now()
	timeWaiting := end.Sub(start).Milliseconds()

	rpcInfo := telemetry.RPCInfoFromContext(ctx)
	boundedReadDelayMsHistogram.WithLabelValues(
		rpcInfo.Service,
		rpcInfo.Method,
	).Observe(float64(timeWaiting))

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Int64(timeWaitingSpanAttribute, timeWaiting))
}
