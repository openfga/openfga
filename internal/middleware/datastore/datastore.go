package datastore

import (
	"context"
	"fmt"

	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ctxKey struct{}

var datastoreCtxKey ctxKey = struct{}{}

// datastoreHandle embeds a handle to an OpenFGA datastore.
type datastoreHandle struct {
	datastore storage.OpenFGADatastore
}

// DatastoreFromContext extracts the OpenFGA datastore out of the provided context. If no
// datastore is included in the provided context then a nil one is returned.
func DatastoreFromContext(ctx context.Context) storage.OpenFGADatastore {
	if c := ctx.Value(datastoreCtxKey); c != nil {
		handle := c.(*datastoreHandle)
		return handle.datastore
	}
	return nil
}

// MustFromContext extracts the OpenFGA datastore out of the provided context and panics if it does not exist.
func MustFromContext(ctx context.Context) storage.OpenFGADatastore {
	datastore := DatastoreFromContext(ctx)
	if datastore == nil {
		panic("datastore middleware did not inject datastore")
	}

	return datastore
}

// ContextWithDatastore injects the provided OpenFGA datastore into the context.
func ContextWithDatastore(ctx context.Context, datastore storage.OpenFGADatastore) context.Context {
	return context.WithValue(ctx, datastoreCtxKey, &datastoreHandle{datastore})
}

var _ storage.OpenFGADatastore = &blah{}

type blah struct {
	storage.OpenFGADatastore
	tuples map[string][]*openfgapb.TupleKey
}

// func NewBlah(ds storage.OpenFGADatastore, contextualTuples []*openfgapb.TupleKey) *blah {

// 	b := &blah{
// 		OpenFGADatastore: ds,
// 		tuples:           make(map[string][]*openfgapb.TupleKey),
// 	}

// 	for _, tuple := range contextualTuples {
// 		or := tuple.ToObjectRelationString(tuple.GetObject(), tuple.GetRelation())
// 		b.tuples[or] = tuple
// 	}
// }

func (b *blah) Read(ctx context.Context, storeID string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *blah) ReadUserTuple(ctx context.Context, storeID string, tupleKey *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *blah) ReadUsersetTuples(ctx context.Context, storeID string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

// MustFromContextWithTuples attempts to extract the OpenFGADatastore from the provided ctx and decorates
// it with the (contextual) tuples provided.
func MustFromContextWithTuples(ctx context.Context, tuples []*openfgapb.TupleKey) storage.OpenFGADatastore {
	datastore := MustFromContext(ctx)

	return &blah{
		OpenFGADatastore: datastore,
		//contextualTuples: tuples,
	}
}
