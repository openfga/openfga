package obj

import (
	"context"
	"errors"
	"iter"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline"
	"github.com/openfga/openfga/pkg/storage"
)

type Option func(o *Reader)

func NewReader(
	store storage.RelationshipTupleReader,
	storeID string,
	opts ...Option,
) *Reader {
	r := &Reader{store: store, storeID: storeID}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func WithConsistency(pref openfgav1.ConsistencyPreference) Option {
	return func(r *Reader) {
		r.consistency = pref
	}
}

func WithValidator(fn func(*openfgav1.TupleKey) (bool, error)) Option {
	return func(r *Reader) {
		r.validator = fn
	}
}

type Reader struct {
	store       storage.RelationshipTupleReader
	storeID     string
	consistency openfgav1.ConsistencyPreference
	validator   func(*openfgav1.TupleKey) (bool, error)
}

// createIterator queries the datastore for tuples matching the input parameters.
// Returns an error iterator if the query fails to preserve error information
// through the iterator interface.
func (r *Reader) createIterator(
	ctx context.Context,
	q pipeline.ObjectQuery,
) storage.TupleIterator {
	userFilter := make([]*openfgav1.ObjectRelation, len(q.Users))

	for i, user := range q.Users {
		object, relation, _ := strings.Cut(user, "#")

		userFilter[i] = &openfgav1.ObjectRelation{
			Object:   object,
			Relation: relation,
		}
	}

	it, err := r.store.ReadStartingWithUser(
		ctx,
		r.storeID,
		storage.ReadStartingWithUserFilter{
			ObjectType: q.ObjectType,
			Relation:   q.Relation,
			UserFilter: userFilter,
			Conditions: q.Conditions,
		},
		storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: r.consistency,
			},
		},
	)

	if err != nil {
		return iterator.Error[*openfgav1.Tuple](err)
	}
	return it
}

// applyValidator wraps the iterator with validation to filter invalid tuples.
// Invalid tuples (wrong type, failing conditions, etc.) are skipped; this prevents
// them from corrupting pipeline results or causing downstream errors.
func (r *Reader) applyValidator(it storage.TupleIterator) storage.TupleKeyIterator {
	base := storage.NewTupleKeyIteratorFromTupleIterator(it)
	base = iterator.Validate(base, r.validator)
	return base
}

// toSequence converts the iterator to an iter.Seq for pipeline consumption.
// Manages iterator lifecycle and ensures cleanup on early termination.
func (r *Reader) toSequence(
	ctx context.Context,
	itr storage.TupleKeyIterator,
) iter.Seq[pipeline.Item] {
	ctx, cancel := context.WithCancel(ctx)

	return func(yield func(pipeline.Item) bool) {
		defer cancel()
		defer itr.Stop()

		for ctx.Err() == nil {
			t, err := itr.Next(ctx)

			var item pipeline.Item

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

func (r *Reader) Read(
	ctx context.Context,
	q pipeline.ObjectQuery,
) iter.Seq[pipeline.Item] {
	iterator := r.createIterator(ctx, q)
	filtered := r.applyValidator(iterator)
	return r.toSequence(ctx, filtered)
}
