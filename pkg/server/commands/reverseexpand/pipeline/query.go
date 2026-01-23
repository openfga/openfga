package pipeline

import (
	"context"
	"errors"
	"iter"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

type queryInput struct {
	objectType     string
	objectRelation string
	userFilter     []*openfgav1.ObjectRelation
	conditions     []string
}

type queryEngine struct {
	datastore   storage.RelationshipTupleReader
	storeID     string
	consistency openfgav1.ConsistencyPreference
	validator   falibleValidator[*openfgav1.TupleKey]
}

func (qp *queryEngine) Execute(ctx context.Context, input queryInput) iter.Seq[Item] {
	iterator := qp.createIterator(ctx, input)
	filtered := qp.applyValidator(iterator)
	return qp.toSequence(ctx, filtered)
}

func (qp *queryEngine) createIterator(ctx context.Context, input queryInput) storage.TupleIterator {
	it, err := qp.datastore.ReadStartingWithUser(
		ctx,
		qp.storeID,
		storage.ReadStartingWithUserFilter{
			ObjectType: input.objectType,
			Relation:   input.objectRelation,
			UserFilter: input.userFilter,
			Conditions: input.conditions,
		},
		storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: qp.consistency,
			},
		},
	)

	if err != nil {
		return &errorIterator{err: err}
	}
	return it
}

func (qp *queryEngine) applyValidator(it storage.TupleIterator) storage.TupleKeyIterator {
	base := storage.NewTupleKeyIteratorFromTupleIterator(it)
	base = newValidatingIterator(base, qp.validator)
	return base
}

func (qp *queryEngine) toSequence(ctx context.Context, itr storage.TupleKeyIterator) iter.Seq[Item] {
	ctx, cancel := context.WithCancel(ctx)

	return func(yield func(Item) bool) {
		defer cancel()
		defer itr.Stop()

		for ctx.Err() == nil {
			t, err := itr.Next(ctx)

			var item Item

			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}

				item.Err = err

				yield(item)
				break
			}

			if t == nil {
				continue
			}

			item.Value = t.GetObject()

			if !yield(item) {
				break
			}
		}
	}
}
