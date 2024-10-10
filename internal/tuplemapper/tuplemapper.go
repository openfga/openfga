package tuplemapper

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// Mapper is an iterator of tuples that also extracts information from them.
type Mapper interface {

	// Start starts an iterator by reading from the DB.
	Start(ctx context.Context) (storage.TupleIterator, error)

	// Map returns an error if the tuple given by the iterator is invalid.
	// Otherwise, it returns some information from the tuple.
	Map(t *openfgav1.TupleKey) (string, error)

	// Clone creates a copy of the mapper but for a different object.
	// The object will be used to filter the iterator.
	Clone(newObject string) Mapper
}

type Request struct {
	Kind             MapperKind
	StoreID          string
	Consistency      openfgav1.ConsistencyPreference
	Object, Relation string
}

// New is a factory that returns the right mapper based on the request's kind.
func New(ds storage.RelationshipTupleReader, req Request) Mapper {
	switch req.Kind {
	case NoOpKind:
		return newNoOpMapper()
	case NestedUsersetKind:
		return newNestedUsersetTupleMapper(ds, req)
	case NestedTTUKind:
		// TODO
		return nil
	default:
		panic("unsupported mapper kind")
	}
}

type NoOpMapper struct{}

var _ Mapper = (*NoOpMapper)(nil)

func newNoOpMapper() *NoOpMapper {
	return &NoOpMapper{}
}

func (n NoOpMapper) Map(_ *openfgav1.TupleKey) (string, error) {
	return "", nil
}

func (n NoOpMapper) Start(_ context.Context) (storage.TupleIterator, error) {
	return storage.NewStaticTupleIterator(nil), nil
}

func (n NoOpMapper) Clone(_ string) Mapper {
	return &NoOpMapper{}
}

type NestedUsersetMapper struct {
	Datastore storage.RelationshipTupleReader
	StoreID   string
	Filter    storage.ReadUsersetTuplesFilter
	Options   storage.ReadUsersetTuplesOptions
}

type MapperKind int64

const (
	NestedUsersetKind MapperKind = 0
	NestedTTUKind     MapperKind = 1
	// For tests only.
	NoOpKind MapperKind = 2
)

var _ Mapper = (*NestedUsersetMapper)(nil)

func newNestedUsersetTupleMapper(ds storage.RelationshipTupleReader, req Request) *NestedUsersetMapper {
	objectType := tuple.GetType(req.Object)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   req.Object,
		Relation: req.Relation,
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			typesystem.DirectRelationReference(objectType, req.Relation),
		},
	}
	opts := storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{
		Preference: req.Consistency,
	}}

	return &NestedUsersetMapper{Datastore: ds, StoreID: req.StoreID, Filter: filter, Options: opts}
}

func (n NestedUsersetMapper) Start(ctx context.Context) (storage.TupleIterator, error) {
	return n.Datastore.ReadUsersetTuples(ctx, n.StoreID, n.Filter, n.Options)
}

func (n NestedUsersetMapper) Clone(newObject string) Mapper {
	n.Filter.Object = newObject
	return &NestedUsersetMapper{Datastore: n.Datastore, StoreID: n.StoreID, Filter: n.Filter, Options: n.Options}
}

func (n NestedUsersetMapper) Map(t *openfgav1.TupleKey) (string, error) {
	usersetName, relation := tuple.SplitObjectRelation(t.GetUser())
	if relation == "" {
		// This should never happen because ReadUsersetTuples only returns usersets as users.
		return "", fmt.Errorf("unexpected userset %s with no relation", t.GetUser())
	}
	return usersetName, nil
}
