package checkutil

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// BuildTupleKeyConditionFilter returns the TupleKeyConditionFilterFunc for which, together with the tuple key,
// evaluates whether condition is met.
func BuildTupleKeyConditionFilter(ctx context.Context, reqCtx *structpb.Struct, typesys *typesystem.TypeSystem) storage.TupleKeyConditionFilterFunc {
	return func(t *openfgav1.TupleKey) (bool, error) {
		condEvalResult, err := eval.EvaluateTupleCondition(ctx, t, typesys, reqCtx)
		if err != nil {
			return false, err
		}

		if len(condEvalResult.MissingParameters) > 0 {
			return false, condition.NewEvaluationError(
				t.GetCondition().GetName(),
				fmt.Errorf("tuple '%s' is missing context parameters '%v'",
					tuple.TupleKeyToString(t),
					condEvalResult.MissingParameters),
			)
		}

		return condEvalResult.ConditionMet, nil
	}
}

// ObjectIDInSortedSet returns whether any of the object IDs in the tuples given by the iterator is in the input set of objectIDs.
func ObjectIDInSortedSet(ctx context.Context, iter storage.TupleKeyIterator, objectIDs storage.SortedSet) (bool, error) {
	for {
		t, err := iter.Next(ctx)
		if errors.Is(err, storage.ErrIteratorDone) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		_, objectID := tuple.SplitObject(t.GetObject())
		if objectIDs.Exists(objectID) {
			return true, nil
		}
	}
}

// userFilter returns the ObjectRelation where the object is the specified user.
// If the specified type is publicly assigned type, the object will also include
// publicly wildcard.
func userFilter(hasPubliclyAssignedType bool,
	user,
	userType string) []*openfgav1.ObjectRelation {
	if !hasPubliclyAssignedType || user == tuple.TypedPublicWildcard(userType) {
		return []*openfgav1.ObjectRelation{{
			Object: user,
		}}
	}

	return []*openfgav1.ObjectRelation{
		{Object: user},
		{Object: tuple.TypedPublicWildcard(userType)},
	}
}

// TODO: These (graph.ResolveCheckRequest, graph.ResolveCheckResponse) should be moved to a shared package to avoid having
// to duplicate across, and have better composition.
type resolveCheckRequest interface {
	GetStoreID() string
	GetTupleKey() *openfgav1.TupleKey
	GetConsistency() openfgav1.ConsistencyPreference
	GetContext() *structpb.Struct
}

// IteratorReadStartingFromUser returns storage iterator for
// user with request's type and relation with specified objectIDs as
// filter.
func IteratorReadStartingFromUser(ctx context.Context,
	typesys *typesystem.TypeSystem,
	ds storage.RelationshipTupleReader,
	req resolveCheckRequest,
	objectRel string,
	objectIDs storage.SortedSet,
	sortContextualTuples bool) (storage.TupleKeyIterator, error) {
	storeID := req.GetStoreID()
	reqTupleKey := req.GetTupleKey()

	opts := storage.ReadStartingWithUserOptions{
		WithResultsSortedAscending: sortContextualTuples,
		Consistency: storage.ConsistencyOptions{
			Preference: req.GetConsistency(),
		},
	}

	user := reqTupleKey.GetUser()
	userType := tuple.GetType(user)
	objectType, relation := tuple.SplitObjectRelation(objectRel)
	// TODO: add in optimization to filter out user not matching the type

	relationReference := typesystem.DirectRelationReference(objectType, relation)
	hasPubliclyAssignedType, _ := typesys.IsPubliclyAssignable(relationReference, userType)

	iter, err := ds.ReadStartingWithUser(ctx, storeID,
		storage.ReadStartingWithUserFilter{
			ObjectType: objectType,
			Relation:   relation,
			UserFilter: userFilter(hasPubliclyAssignedType, user, userType),
			ObjectIDs:  objectIDs,
		}, opts)
	if err != nil {
		return nil, err
	}

	return storage.NewConditionsFilteredTupleKeyIterator(
		storage.NewFilteredTupleKeyIterator(
			storage.NewTupleKeyIteratorFromTupleIterator(iter),
			validation.FilterInvalidTuples(typesys),
		),
		BuildTupleKeyConditionFilter(ctx, req.GetContext(), typesys),
	), nil
}

func buildUsersetDetails(typesys *typesystem.TypeSystem, objectType, relation string) (string, error) {
	cr, err := typesys.ResolveComputedRelation(objectType, relation)
	if err != nil {
		return "", err
	}
	return tuple.ToObjectRelationString(objectType, cr), nil
}

type V2RelationFunc func(*openfgav1.RelationReference) string

// BuildUsersetV2RelationFunc returns the reference's relation.
func BuildUsersetV2RelationFunc() V2RelationFunc {
	return func(ref *openfgav1.RelationReference) string {
		return ref.GetRelation()
	}
}

// BuildTTUV2RelationFunc will always return the computedRelation regardless of the reference.
func BuildTTUV2RelationFunc(computedRelation string) V2RelationFunc {
	return func(_ *openfgav1.RelationReference) string {
		return computedRelation
	}
}

type UsersetDetailsFunc func(*openfgav1.TupleKey) (string, string, error)

// BuildUsersetDetailsUserset given tuple doc:1#viewer@group:2#member will return group#member, 2, nil.
func BuildUsersetDetailsUserset(typesys *typesystem.TypeSystem) UsersetDetailsFunc {
	return func(t *openfgav1.TupleKey) (string, string, error) {
		// the relation is from the tuple
		object, relation := tuple.SplitObjectRelation(t.GetUser())
		objectType, objectID := tuple.SplitObject(object)
		rel, err := buildUsersetDetails(typesys, objectType, relation)
		if err != nil {
			return "", "", err
		}
		return rel, objectID, nil
	}
}

// BuildUsersetDetailsTTU given (tuple doc:1#viewer@group:2, member) will return group#member, 2, nil.
// This util takes into account computed relationships, otherwise it will resolve it from the target UserType.
// nolint:unused
func BuildUsersetDetailsTTU(typesys *typesystem.TypeSystem, computedRelation string) UsersetDetailsFunc {
	return func(t *openfgav1.TupleKey) (string, string, error) {
		object, _ := tuple.SplitObjectRelation(t.GetUser())
		objectType, objectID := tuple.SplitObject(object)
		rel, err := buildUsersetDetails(typesys, objectType, computedRelation)
		if err != nil {
			return "", "", err
		}
		return rel, objectID, nil
	}
}
