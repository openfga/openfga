package storagewrappers

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

// NewCombinedTupleReader returns a [storage.RelationshipTupleReader] that reads from
// a persistent datastore and from the contextual tuples specified in the request.
func NewCombinedTupleReader(
	ds storage.RelationshipTupleReader,
	contextualTuples []*openfgav1.TupleKey,
) storage.RelationshipTupleReader {
	return &CombinedTupleReader{
		RelationshipTupleReader: ds,
		contextualTuples:        contextualTuples,
	}
}

type CombinedTupleReader struct {
	storage.RelationshipTupleReader
	contextualTuples []*openfgav1.TupleKey
}

var _ storage.RelationshipTupleReader = (*CombinedTupleReader)(nil)

// filterTuples filters out the tuples in the provided slice by removing any tuples in the slice
// that don't match the object and relation provided in the filterKey.
func filterTuples(tuples []*openfgav1.TupleKey, targetObject, targetRelation string) []*openfgav1.Tuple {
	var filtered []*openfgav1.Tuple
	for _, tk := range tuples {
		if tk.GetObject() == targetObject && tk.GetRelation() == targetRelation {
			filtered = append(filtered, &openfgav1.Tuple{
				Key: tk,
			})
		}
	}

	return filtered
}

// Read see [storage.RelationshipTupleReader.ReadUserTuple].
func (c *CombinedTupleReader) Read(
	ctx context.Context,
	storeID string,
	tk *openfgav1.TupleKey,
	options storage.ReadOptions,
) (storage.TupleIterator, error) {
	iter1 := storage.NewStaticTupleIterator(filterTuples(c.contextualTuples, tk.GetObject(), tk.GetRelation()))

	iter2, err := c.RelationshipTupleReader.Read(ctx, storeID, tk, options)
	if err != nil {
		return nil, err
	}

	return storage.NewCombinedIterator(iter1, iter2), nil
}

// ReadPage see [storage.RelationshipTupleReader.ReadPage].
func (c *CombinedTupleReader) ReadPage(ctx context.Context, store string, tk *openfgav1.TupleKey, options storage.ReadPageOptions) ([]*openfgav1.Tuple, []byte, error) {
	// No reading from contextual tuples.
	return c.RelationshipTupleReader.ReadPage(ctx, store, tk, options)
}

// ReadUserTuple see [storage.RelationshipTupleReader.ReadUserTuple].
func (c *CombinedTupleReader) ReadUserTuple(
	ctx context.Context,
	store string,
	tk *openfgav1.TupleKey,
	options storage.ReadUserTupleOptions,
) (*openfgav1.Tuple, error) {
	filteredContextualTuples := filterTuples(c.contextualTuples, tk.GetObject(), tk.GetRelation())

	for _, t := range filteredContextualTuples {
		if t.GetKey().GetUser() == tk.GetUser() {
			return t, nil
		}
	}

	return c.RelationshipTupleReader.ReadUserTuple(ctx, store, tk, options)
}

// ReadUsersetTuples see [storage.RelationshipTupleReader].ReadUsersetTuples.
func (c *CombinedTupleReader) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
	options storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	var usersetTuples []*openfgav1.Tuple

	for _, t := range filterTuples(c.contextualTuples, filter.Object, filter.Relation) {
		if tuple.GetUserTypeFromUser(t.GetKey().GetUser()) == tuple.UserSet {
			usersetTuples = append(usersetTuples, t)
		}
	}

	iter1 := storage.NewStaticTupleIterator(usersetTuples)

	iter2, err := c.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	if err != nil {
		return nil, err
	}

	return storage.NewCombinedIterator(iter1, iter2), nil
}

// ReadStartingWithUser see [storage.RelationshipTupleReader].ReadStartingWithUser.
func (c *CombinedTupleReader) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	options storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	var filteredTuples []*openfgav1.Tuple
	for _, t := range c.contextualTuples {
		if tuple.GetType(t.GetObject()) != filter.ObjectType {
			continue
		}

		if t.GetRelation() != filter.Relation {
			continue
		}

		for _, u := range filter.UserFilter {
			targetUser := u.GetObject()
			if u.GetRelation() != "" {
				targetUser = tuple.ToObjectRelationString(targetUser, u.GetRelation())
			}

			if t.GetUser() == targetUser {
				filteredTuples = append(filteredTuples, &openfgav1.Tuple{
					Key: t,
				})
			}
		}
	}

	iter1 := storage.NewStaticTupleIterator(filteredTuples)

	iter2, err := c.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	if err != nil {
		return nil, err
	}

	return storage.NewCombinedIterator(iter1, iter2), nil
}
