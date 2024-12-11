package storagewrappers

import (
	"context"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"slices"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

// NewCombinedTupleReader returns a [storage.TupleEvaluator] that reads from
// a persistent datastore and from the contextual tuples specified in the request.
func NewCombinedTupleReader(
	ds storage.RelationshipTupleReader,
	contextualTuples []*openfgav1.TupleKey,
) *CombinedTupleReader {
	ctr := &CombinedTupleReader{
		RelationshipTupleReader:         ds,
		contextualTuplesOrderedByUser:   contextualTuples,
		contextualTuplesOrderedByObject: contextualTuples,
	}

	slices.SortFunc(ctr.contextualTuplesOrderedByUser, storage.AscendingUserFunc())

	slices.SortFunc(ctr.contextualTuplesOrderedByObject, storage.AscendingObjectFunc())

	return ctr
}

type CombinedTupleReader struct {
	RelationshipTupleReader         storage.RelationshipTupleReader
	contextualTuplesOrderedByUser   []*openfgav1.TupleKey
	contextualTuplesOrderedByObject []*openfgav1.TupleKey
}

var _ storage.TupleEvaluator = (*CombinedTupleReader)(nil)

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

func (c *CombinedTupleReader) Read(ctx context.Context, store string, tk *openfgav1.TupleKey, options storage.ReadOptions, tupleOrder ...storage.TupleOrderFunc) (storage.TupleIterator, error) {
	filteredTuples := filterTuples(c.contextualTuplesOrderedByUser, tk.GetObject(), tk.GetRelation(), []string{})
	iter1 := storage.NewStaticTupleIterator(filteredTuples)

	iter2, err := c.RelationshipTupleReader.Read(ctx, store, tk, options)
	if err != nil {
		return nil, err
	}

	if len(tupleOrder) == 0 {
		return storage.NewCombinedIterator(iter1, iter2), nil
	}

	return storage.NewOrderedCombinedIterator(tupleOrder[0], iter1, iter2), nil
}

func (c *CombinedTupleReader) ReadUserTuple(ctx context.Context, store string, tk *openfgav1.TupleKey, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	targetUsers := []string{tk.GetUser()}
	filteredContextualTuples := filterTuples(c.contextualTuplesOrderedByUser, tk.GetObject(), tk.GetRelation(), targetUsers)

	for _, t := range filteredContextualTuples {
		if t.GetKey().GetUser() == tk.GetUser() {
			return t, nil
		}
	}

	return c.RelationshipTupleReader.ReadUserTuple(ctx, store, tk, options)
}

func (c *CombinedTupleReader) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions, tupleOrder ...storage.TupleOrderFunc) (storage.TupleIterator, error) {
	var usersetTuples []*openfgav1.Tuple

	for _, t := range filterTuples(c.contextualTuplesOrderedByUser, filter.Object, filter.Relation, []string{}) {
		if tuple.GetUserTypeFromUser(t.GetKey().GetUser()) == tuple.UserSet {
			usersetTuples = append(usersetTuples, t)
		}
	}

	iter1 := storage.NewStaticTupleIterator(usersetTuples)

	iter2, err := c.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter, options)
	if err != nil {
		return nil, err
	}

	if len(tupleOrder) == 0 {
		return storage.NewCombinedIterator(iter1, iter2), nil
	}

	return storage.NewOrderedCombinedIterator(tupleOrder[0], iter1, iter2), nil
}

func (c *CombinedTupleReader) ReadStartingWithUser(ctx context.Context, store string, filter storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions, tupleOrder ...storage.TupleOrderFunc) (storage.TupleIterator, error) {
	var userFilters []string
	for _, u := range filter.UserFilter {
		uf := u.GetObject()
		if u.GetRelation() != "" {
			uf = tuple.ToObjectRelationString(uf, u.GetRelation())
		}
		userFilters = append(userFilters, uf)
	}

	filteredTuples := make([]*openfgav1.Tuple, 0, len(c.contextualTuplesOrderedByObject))
	for _, t := range filterTuples(c.contextualTuplesOrderedByObject, "", filter.Relation, userFilters) {
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

	if len(tupleOrder) == 0 {
		return storage.NewCombinedIterator(iter1, iter2), nil
	}

	return storage.NewOrderedCombinedIterator(tupleOrder[0], iter1, iter2), nil
}
