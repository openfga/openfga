package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

// NewValidator returns a validator that combines condition evaluation
// and type-system filtering for use with [WithStoreValidator].
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

// StoreOption configures a [ValidatingStore].
type StoreOption func(o *ValidatingStore)

// NewValidatingStore returns a ValidatingStore that queries store for relationship tuples.
func NewValidatingStore(
	store storage.RelationshipTupleReader,
	storeID string,
	opts ...StoreOption,
) *ValidatingStore {
	r := &ValidatingStore{store: store, storeID: storeID}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// WithStoreConsistency sets the consistency preference for storage reads.
func WithStoreConsistency(pref openfgav1.ConsistencyPreference) StoreOption {
	return func(r *ValidatingStore) {
		r.consistency = pref
	}
}

// WithStoreValidator sets a function that filters tuples during iteration.
// Tuples for which fn returns false are silently skipped.
func WithStoreValidator(fn func(*openfgav1.TupleKey) (bool, error)) StoreOption {
	return func(r *ValidatingStore) {
		r.validator = fn
	}
}

// ValidatingStore implements [ObjectStore] by querying a [storage.RelationshipTupleReader]
// and optionally filtering results through a validator.
type ValidatingStore struct {
	store       storage.RelationshipTupleReader
	storeID     string
	consistency openfgav1.ConsistencyPreference
	validator   func(*openfgav1.TupleKey) (bool, error)
}

// createIterator queries the datastore for tuples matching the input parameters.
// Returns an error iterator if the query fails to preserve error information
// through the iterator interface.
func (r *ValidatingStore) createIterator(
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
		return iterator.Error[*openfgav1.Tuple](fmt.Errorf("datastore: %w", err))
	}
	return it
}

// applyValidator wraps the iterator with validation to filter invalid tuples.
// Invalid tuples (wrong type, failing conditions, etc.) are skipped; this prevents
// them from corrupting pipeline results or causing downstream errors.
func (r *ValidatingStore) applyValidator(it storage.TupleIterator) storage.TupleKeyIterator {
	base := storage.NewTupleKeyIteratorFromTupleIterator(it)
	if r.validator != nil {
		base = iterator.Validate(base, r.validator)
	}
	return base
}

// TupleKeyItemReceiver adapts a [storage.TupleKeyIterator] into a
// [Receiver] of [Item] values, extracting the object identifier from
// each tuple key.
type TupleKeyItemReceiver struct {
	itr    storage.TupleKeyIterator
	closed atomic.Bool
}

// Recv returns the next object identifier from the underlying iterator.
// It returns false when the iterator is exhausted, closed, or the context
// is cancelled.
func (r *TupleKeyItemReceiver) Recv(ctx context.Context) (Item, bool) {
	var item Item

	for {
		if r.closed.Load() || ctx.Err() != nil {
			return item, false
		}
		t, err := r.itr.Next(ctx)

		if err != nil {
			defer r.Close()
			if errors.Is(err, storage.ErrIteratorDone) {
				return item, false
			}
			item.Err = fmt.Errorf("iterator: %w", err)
			return item, true
		}

		if t == nil {
			continue
		}

		item.Value = t.GetObject()
		return item, true
	}
}

// Close stops the underlying iterator. It is safe to call multiple times.
func (r *TupleKeyItemReceiver) Close() {
	if !r.closed.Swap(true) {
		r.itr.Stop()
	}
}

// Read queries storage for tuples matching q and returns the matching
// object identifiers as a streaming sequence.
func (r *ValidatingStore) Read(
	ctx context.Context,
	q ObjectQuery,
) Receiver[Item] {
	iterator := r.createIterator(ctx, q)
	filtered := r.applyValidator(iterator)
	return &TupleKeyItemReceiver{
		itr: filtered,
	}
}
