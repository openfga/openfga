package pipeline

import (
	"context"
	"errors"
	"iter"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

// queryInput contains the parameters for a single tuple query.
type queryInput struct {
	objectType     string
	objectRelation string
	userFilter     []*openfgav1.ObjectRelation
	conditions     []string
}

// queryEngine abstracts storage operations for edge handlers.
// Wraps the datastore with validation to ensure only valid tuples flow through the pipeline.
type queryEngine struct {
	datastore   storage.RelationshipTupleReader
	storeID     string
	consistency openfgav1.ConsistencyPreference
	validator   fallibleValidator[*openfgav1.TupleKey]
}

// Execute runs the query and returns results as a sequence.
// Applies validation to filter out invalid tuples before they enter the pipeline.
func (qp *queryEngine) Execute(ctx context.Context, input queryInput) iter.Seq[Item] {
	iterator := qp.createIterator(ctx, input)
	filtered := qp.applyValidator(iterator)
	return qp.toSequence(ctx, filtered)
}

// createIterator queries the datastore for tuples matching the input parameters.
// Returns an error iterator if the query fails to preserve error information
// through the iterator interface.
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

// applyValidator wraps the iterator with validation to filter invalid tuples.
// Invalid tuples (wrong type, failing conditions, etc.) are skipped; this prevents
// them from corrupting pipeline results or causing downstream errors.
func (qp *queryEngine) applyValidator(it storage.TupleIterator) storage.TupleKeyIterator {
	base := storage.NewTupleKeyIteratorFromTupleIterator(it)
	base = newValidatingIterator(base, qp.validator)
	return base
}

// toSequence converts the iterator to an iter.Seq for pipeline consumption.
// Manages iterator lifecycle and ensures cleanup on early termination.
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
