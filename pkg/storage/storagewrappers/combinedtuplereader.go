package storagewrappers

import (
	"context"
	"slices"

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
// that don't match the object, relation or user provided in the filterKey.
//
//nolint:unparam
func filterTuples(tuples []*openfgav1.TupleKey, targetObject, targetRelation string, targetUsers []string) []*openfgav1.Tuple {
	var filtered []*openfgav1.Tuple
	for _, tk := range tuples {
		if (targetObject == "" || tk.GetObject() == targetObject) &&
			(targetRelation == "" || tk.GetRelation() == targetRelation) &&
			(len(targetUsers) == 0 || slices.Contains(targetUsers, tk.GetUser())) {
			filtered = append(filtered, &openfgav1.Tuple{
				Key: tk,
			})
		}
	}

	return filtered
}

// Read see [storage.RelationshipTupleReader.Read].
func (c *CombinedTupleReader) Read(
	ctx context.Context,
	storeID string,
	tk *openfgav1.TupleKey,
	options storage.ReadOptions,
) (storage.TupleIterator, error) {
	iter1 := storage.NewStaticTupleIterator(filterTuples(c.contextualTuples, tk.GetObject(), tk.GetRelation(), []string{}))

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
	targetUsers := []string{tk.GetUser()}
	filteredContextualTuples := filterTuples(c.contextualTuples, tk.GetObject(), tk.GetRelation(), targetUsers)

	for _, t := range filteredContextualTuples {
		if t.GetKey().GetUser() == tk.GetUser() {
			return t, nil
		}
	}

	return c.RelationshipTupleReader.ReadUserTuple(ctx, store, tk, options)
}

// ReadUsersetTuples see [storage.RelationshipTupleReader.ReadUsersetTuples].
func (c *CombinedTupleReader) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
	options storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	var usersetTuples []*openfgav1.Tuple

	for _, t := range filterTuples(c.contextualTuples, filter.Object, filter.Relation, []string{}) {
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

// ReadStartingWithUser see [storage.RelationshipTupleReader.ReadStartingWithUser].
func (c *CombinedTupleReader) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	options storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	var userFilters []string
	for _, u := range filter.UserFilter {
		uf := u.GetObject()
		if u.GetRelation() != "" {
			uf = tuple.ToObjectRelationString(uf, u.GetRelation())
		}
		userFilters = append(userFilters, uf)
	}

	filteredTuples := make([]*openfgav1.Tuple, 0, len(c.contextualTuples))
	for _, t := range filterTuples(c.contextualTuples, "", filter.Relation, userFilters) {
		if tuple.GetType(t.GetKey().GetObject()) != filter.ObjectType {
			continue
		}
		filteredTuples = append(filteredTuples, t)
	}

	iter1 := storage.NewStaticTupleIterator(filteredTuples)

	iter2, err := c.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter, options)
	if err != nil {
		return nil, err
	}

	return storage.NewCombinedIterator(iter1, iter2), nil
}
