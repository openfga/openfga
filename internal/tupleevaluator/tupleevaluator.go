package tupleevaluator

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type TupleIteratorEvaluator interface {
	storage.TupleIterator
	Evaluate(t *openfgav1.TupleKey) (string, error)
}

type EvaluatorKind int64

const (
	NestedUsersetKind EvaluatorKind = 0
	NestedTTUKind     EvaluatorKind = 1
)

func NewTupleEvaluator(ctx context.Context, ds storage.RelationshipTupleReader, req checkutil.ResolveCheckRequest, kind EvaluatorKind) (TupleIteratorEvaluator, error) {
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
}

var _ TupleIteratorEvaluator = (*NestedUsersetEvaluator)(nil)

func NewNestedUsersetEvaluator(ctx context.Context, ds storage.RelationshipTupleReader, req checkutil.ResolveCheckRequest) (*NestedUsersetEvaluator, error) {
	objectID := req.GetTupleKey().GetObject()
	objectType := tuple.GetType(objectID)
	relation := req.GetTupleKey().GetRelation()

	iter, err := ds.ReadUsersetTuples(ctx, req.GetStoreID(), storage.ReadUsersetTuplesFilter{
		Object:   objectID,
		Relation: relation,
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			typesystem.DirectRelationReference(objectType, relation),
		},
	}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{
		Preference: req.GetConsistency(),
	}})
	if err != nil {
		return nil, err
	}
	return &NestedUsersetEvaluator{iter}, nil
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
