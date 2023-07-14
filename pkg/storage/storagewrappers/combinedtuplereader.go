package storagewrappers

import (
	"context"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// NewCombinedTupleReader returns a TupleReader that reads from a persistent datastore and from the contextual
// tuples specified in the request
func NewCombinedTupleReader(ds storage.RelationshipTupleReader, contextualTuples []*openfgapb.TupleKey) storage.RelationshipTupleReader {
	return &combinedTupleReader{RelationshipTupleReader: ds, contextualTuples: contextualTuples}
}

type combinedTupleReader struct {
	storage.RelationshipTupleReader
	contextualTuples []*openfgapb.TupleKey
}

var _ storage.RelationshipTupleReader = (*combinedTupleReader)(nil)

// filterTuples filters out the tuples in the provided slice by removing any tuples in the slice
// that don't match the object and relation provided in the filterKey.
func filterTuples(tuples []*openfgapb.TupleKey, targetObject, targetRelation string) []*openfgapb.Tuple {
	var filtered []*openfgapb.Tuple
	for _, tk := range tuples {
		if tk.GetObject() == targetObject && tk.GetRelation() == targetRelation {
			filtered = append(filtered, &openfgapb.Tuple{
				Key: tk,
			})
		}
	}

	return filtered
}

func (c *combinedTupleReader) Read(
	ctx context.Context,
	storeID string,
	tk *openfgapb.TupleKey,
) (storage.TupleIterator, error) {

	iter1 := storage.NewStaticTupleIterator(filterTuples(c.contextualTuples, tk.Object, tk.Relation))

	iter2, err := c.RelationshipTupleReader.Read(ctx, storeID, tk)
	if err != nil {
		return nil, err
	}

	return storage.NewCombinedIterator(iter1, iter2), nil
}

func (c *combinedTupleReader) ReadPage(
	ctx context.Context,
	store string,
	tk *openfgapb.TupleKey,
	opts storage.PaginationOptions,
) ([]*openfgapb.Tuple, []byte, error) {

	// no reading from contextual tuples

	return c.RelationshipTupleReader.ReadPage(ctx, store, tk, opts)
}

func (c *combinedTupleReader) ReadUserTuple(
	ctx context.Context,
	store string,
	tk *openfgapb.TupleKey,
) (*openfgapb.Tuple, error) {

	filteredContextualTuples := filterTuples(c.contextualTuples, tk.Object, tk.Relation)

	for _, t := range filteredContextualTuples {
		if t.GetKey().GetUser() == tk.GetUser() {
			return t, nil
		}
	}

	return c.RelationshipTupleReader.ReadUserTuple(ctx, store, tk)
}

func (c *combinedTupleReader) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
) (storage.TupleIterator, error) {

	var usersetTuples []*openfgapb.Tuple

	for _, t := range filterTuples(c.contextualTuples, filter.Object, filter.Relation) {
		if tuple.GetUserTypeFromUser(t.GetKey().GetUser()) == tuple.UserSet {
			usersetTuples = append(usersetTuples, t)
		}
	}

	iter1 := storage.NewStaticTupleIterator(usersetTuples)

	iter2, err := c.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter)
	if err != nil {
		return nil, err
	}

	return storage.NewCombinedIterator(iter1, iter2), nil
}

func (c *combinedTupleReader) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {

	var filteredTuples []*openfgapb.Tuple
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
				filteredTuples = append(filteredTuples, &openfgapb.Tuple{
					Key: t,
				})
			}
		}
	}

	iter1 := storage.NewStaticTupleIterator(filteredTuples)

	iter2, err := c.RelationshipTupleReader.ReadStartingWithUser(ctx, store, filter)
	if err != nil {
		return nil, err
	}

	return storage.NewCombinedIterator(iter1, iter2), nil
}
