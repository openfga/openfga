package storagewrappers

import (
	"context"
	"time"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var _ storage.RelationshipTupleReader = (*boundedConcurrencyTupleReader)(nil)

var (
	timeWaitingCounter = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "time_waiting_for_read_queries",
		Help:    "Time (in ms) spent waiting for Read, ReadUserTuple and ReadUsersetTuples calls to the datastore",
		Buckets: []float64{1, 10, 25, 50, 100, 1000, 5000}, // milliseconds
	})
)

type boundedConcurrencyTupleReader struct {
	storage.RelationshipTupleReader
	limiter chan struct{}
}

// NewBoundedConcurrencyTupleReader returns a wrapper over a datastore that makes sure that there are, at most,
// N concurrent calls to Read, ReadUserTuple and ReadUsersetTuples.
// Consumers can then rest assured that one client will not hoard all the database connections available.
func NewBoundedConcurrencyTupleReader(wrapped storage.RelationshipTupleReader, N uint32) *boundedConcurrencyTupleReader {
	return &boundedConcurrencyTupleReader{
		RelationshipTupleReader: wrapped,
		limiter:                 make(chan struct{}, N),
	}
}

func (b *boundedConcurrencyTupleReader) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	start := time.Now()

	b.limiter <- struct{}{}

	end := time.Now()

	timeWaiting := end.Sub(start).Milliseconds()
	timeWaitingCounter.Observe(float64(timeWaiting))
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Int64("time_waiting", timeWaiting))

	defer func() {
		<-b.limiter
	}()

	return b.RelationshipTupleReader.ReadUserTuple(ctx, store, tupleKey)
}

func (b *boundedConcurrencyTupleReader) Read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	start := time.Now()

	b.limiter <- struct{}{}

	end := time.Now()
	timeWaiting := end.Sub(start).Milliseconds()
	timeWaitingCounter.Observe(float64(timeWaiting))
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Int64("time_waiting", timeWaiting))

	defer func() {
		<-b.limiter
	}()

	return b.RelationshipTupleReader.Read(ctx, store, tupleKey)
}

func (b *boundedConcurrencyTupleReader) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	start := time.Now()

	b.limiter <- struct{}{}

	end := time.Now()
	timeWaiting := end.Sub(start).Milliseconds()
	timeWaitingCounter.Observe(float64(timeWaiting))
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Int64("time_waiting", timeWaiting))

	defer func() {
		<-b.limiter
	}()

	return b.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter)
}

func (b *boundedConcurrencyTupleReader) Close() {
	close(b.limiter)
}
