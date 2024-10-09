package tupleevaluator

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// TupleIteratorEvaluator is to evaluate and extract information from tuples.
type TupleIteratorEvaluator interface {
	// Evaluate returns an error if the tuple is invalid according to the evaluator.
	// Otherwise, it returns the information from the tuple.
	Evaluate(t *openfgav1.TupleKey) (string, error)
	// Start starts the evaluator by reading from the DB.
	Start(ctx context.Context) (storage.TupleIterator, error)
	// Clone creates a copy of the evaluator but for a different object and relation pair.
	Clone(newObject, newRelation string) TupleIteratorEvaluator
}

// NewTupleEvaluator is a factory that returns the right evaluator based on the request's kind.
func NewTupleEvaluator(ds storage.RelationshipTupleReader, req EvaluationRequest) TupleIteratorEvaluator {
	switch req.Kind {
	case NestedUsersetKind:
		return newNestedUsersetEvaluator(ds, req)
	case NestedTTUKind:
		// TODO
		return nil
	default:
		panic("unsupported evaluator kind")
	}
}

type NestedUsersetEvaluator struct {
	storage.TupleIterator
	Datastore storage.RelationshipTupleReader
	StoreID   string
	Filter    storage.ReadUsersetTuplesFilter
	Options   storage.ReadUsersetTuplesOptions
}

type EvaluationRequest struct {
	Kind             EvaluatorKind
	StoreID          string
	Consistency      openfgav1.ConsistencyPreference
	Object, Relation string
}

type EvaluatorKind int64

const (
	NestedUsersetKind EvaluatorKind = 0
	NestedTTUKind     EvaluatorKind = 1
)

var _ TupleIteratorEvaluator = (*NestedUsersetEvaluator)(nil)

func newNestedUsersetEvaluator(ds storage.RelationshipTupleReader, req EvaluationRequest) *NestedUsersetEvaluator {
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

	return &NestedUsersetEvaluator{Datastore: ds, StoreID: req.StoreID, Filter: filter, Options: opts}
}

func (n NestedUsersetEvaluator) Start(ctx context.Context) (storage.TupleIterator, error) {
	return n.Datastore.ReadUsersetTuples(ctx, n.StoreID, n.Filter, n.Options)
}

func (n NestedUsersetEvaluator) Clone(newObject, newRelation string) TupleIteratorEvaluator {
	n.Filter.Relation = newRelation
	n.Filter.Object = newObject
	return &NestedUsersetEvaluator{Datastore: n.Datastore, StoreID: n.StoreID, Filter: n.Filter, Options: n.Options}
}

func (n NestedUsersetEvaluator) Evaluate(t *openfgav1.TupleKey) (string, error) {
	usersetName, relation := tuple.SplitObjectRelation(t.GetUser())
	if relation == "" {
		// This should never happen because the filter should only allow userset with relation.
		return "", fmt.Errorf("unexpected userset %s with no relation", t.GetUser())
	}
	return usersetName, nil
}
