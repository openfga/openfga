package pipeline

import (
	"context"
	"errors"
	"iter"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

// NewValidator returns a validator that combines condition evaluation
// and type-system filtering for use with [WithReaderValidator].
func NewValidator(
	ctx context.Context,
	ts *typesystem.TypeSystem,
	obj *structpb.Struct,
) validation.Validator[*openfgav1.TupleKey] {
	return validation.CombineValidators(
		validation.ValidatorFunc(checkutil.BuildTupleKeyConditionFilter(
			ctx,
			obj,
			ts,
		)),
		validation.MakeFallible(
			validation.FilterInvalidTuples(ts),
		),
	)
}

// ReaderOption configures a [Reader].
type ReaderOption func(o *Reader)

// NewReader returns a Reader that queries store for relationship tuples.
func NewReader(
	store storage.RelationshipTupleReader,
	storeID string,
	opts ...ReaderOption,
) *Reader {
	r := &Reader{store: store, storeID: storeID}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// WithReaderConsistency sets the consistency preference for storage reads.
func WithReaderConsistency(pref openfgav1.ConsistencyPreference) ReaderOption {
	return func(r *Reader) {
		r.consistency = pref
	}
}

// WithReaderValidator sets a function that filters tuples during iteration.
// Tuples for which fn returns false are silently skipped.
func WithReaderValidator(fn func(*openfgav1.TupleKey) (bool, error)) ReaderOption {
	return func(r *Reader) {
		r.validator = fn
	}
}

// Reader implements [ObjectReader] by querying a [storage.RelationshipTupleReader]
// and optionally filtering results through a validator.
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
	q ObjectQuery,
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
) iter.Seq[Item] {
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

// Read queries storage for tuples matching q and returns the matching
// object identifiers as a streaming sequence.
func (r *Reader) Read(
	ctx context.Context,
	q ObjectQuery,
) iter.Seq[Item] {
	iterator := r.createIterator(ctx, q)
	filtered := r.applyValidator(iterator)
	return r.toSequence(ctx, filtered)
}
