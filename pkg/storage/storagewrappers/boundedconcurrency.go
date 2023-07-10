package storagewrappers

import (
	"context"
	"time"

	"github.com/openfga/openfga/pkg/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("pkg/storage/boundedconcurrency")

var _ storage.RelationshipTupleReader = (*boundedConcurrencyTupleReader)(nil)

type boundedConcurrencyTupleReader struct {
	storage.RelationshipTupleReader
	limiter chan struct{}
}

// NewBoundedConcurrencyTupleReader returns a wrapper over a datastore that makes sure that there are, at most,
// N concurrent calls to Read, ReadUserTuple and ReadUsersetTuples.
// Consumers can then rest assured that one client will not hoard all the database connections available for one Check call.
func NewBoundedConcurrencyTupleReader(wrapped storage.RelationshipTupleReader, N uint32) *boundedConcurrencyTupleReader {
	return &boundedConcurrencyTupleReader{
		RelationshipTupleReader: wrapped,
		limiter:                 make(chan struct{}, N),
	}
}

func (b *boundedConcurrencyTupleReader) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	ctx, span := tracer.Start(ctx, "ReadUserTuple_boundedConcurrency")
	start := time.Now()

	b.limiter <- struct{}{}

	end := time.Now()
	span.SetAttributes(attribute.Int64("time_waiting", end.Sub(start).Milliseconds()))

	defer func() {
		<-b.limiter
		span.End()
	}()

	return b.RelationshipTupleReader.ReadUserTuple(ctx, store, tupleKey)
}

func (b *boundedConcurrencyTupleReader) Read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "Read_boundedConcurrency")
	start := time.Now()

	b.limiter <- struct{}{}

	end := time.Now()
	span.SetAttributes(attribute.Int64("time_waiting", end.Sub(start).Milliseconds()))

	defer func() {
		<-b.limiter
		span.End()
	}()

	return b.RelationshipTupleReader.Read(ctx, store, tupleKey)
}

func (b *boundedConcurrencyTupleReader) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "ReadUsersetTuples_boundedConcurrency")
	start := time.Now()

	b.limiter <- struct{}{}

	end := time.Now()
	span.SetAttributes(attribute.Int64("time_waiting", end.Sub(start).Milliseconds()))

	defer func() {
		<-b.limiter
		span.End()
	}()

	return b.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter)
}

func (b *boundedConcurrencyTupleReader) Close() {
	close(b.limiter)
}
