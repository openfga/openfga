package tupleevaluator

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type TupleIteratorEvaluator interface {
	storage.TupleIterator
	// Evaluate returns an error if the tuple is invalid according to the evaluator.
	Evaluate(t *openfgav1.TupleKey) (string, error)
	// Clone creates a copy of the evaluator but for a different object and relation pair.
	Clone(ctx context.Context, object, relation string) (TupleIteratorEvaluator, error)
}

type EvaluatorKind int64

const (
	NestedUsersetKind EvaluatorKind = 0
	NestedTTUKind     EvaluatorKind = 1
)

func NewTupleEvaluator(ctx context.Context, ds storage.RelationshipTupleReader, req EvaluationRequest, kind EvaluatorKind) (TupleIteratorEvaluator, error) {
	switch kind {
	case NestedUsersetKind:
		return NewNestedUsersetEvaluator(ctx, ds, req)
	case NestedTTUKind:
		// TODO
		return nil, fmt.Errorf("not implemented")
	default:
		return nil, fmt.Errorf("not supported: %v", kind)
	}
}

type NestedUsersetEvaluator struct {
	storage.TupleIterator
	ds     storage.RelationshipTupleReader
	inputs EvaluationRequest
	filter storage.ReadUsersetTuplesFilter
}

type EvaluationRequest struct {
	StoreID          string
	Consistency      openfgav1.ConsistencyPreference
	Object, Relation string
}

var _ TupleIteratorEvaluator = (*NestedUsersetEvaluator)(nil)

func NewNestedUsersetEvaluator(ctx context.Context, ds storage.RelationshipTupleReader, req EvaluationRequest) (*NestedUsersetEvaluator, error) {
	objectID := req.Object
	objectType := tuple.GetType(objectID)
	relation := req.Relation

	filter := storage.ReadUsersetTuplesFilter{
		Object:   objectID,
		Relation: relation,
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			typesystem.DirectRelationReference(objectType, relation),
		},
	}
	iter, err := ds.ReadUsersetTuples(ctx, req.StoreID, filter, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{
		Preference: req.Consistency,
	}})
	if err != nil {
		return nil, err
	}
	return &NestedUsersetEvaluator{iter, ds, req, filter}, nil
}

func (n NestedUsersetEvaluator) Clone(ctx context.Context, newObject, newRelation string) (TupleIteratorEvaluator, error) {
	n.filter.Relation = newRelation
	n.filter.Object = newObject
	iter, err := n.ds.ReadUsersetTuples(ctx, n.inputs.StoreID, n.filter, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{
		Preference: n.inputs.Consistency,
	}})
	if err != nil {
		return nil, err
	}
	return &NestedUsersetEvaluator{iter, n.ds, n.inputs, n.filter}, nil
}

func (n NestedUsersetEvaluator) Evaluate(t *openfgav1.TupleKey) (string, error) {
	usersetName, relation := tuple.SplitObjectRelation(t.GetUser())
	if relation == "" {
		// This should never happen because the filter should only allow userset with relation.
		return "", fmt.Errorf("unexpected userset %s with no relation", t.GetUser())
	}
	return usersetName, nil
}

func (n NestedUsersetEvaluator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	return n.TupleIterator.Next(ctx)
}
