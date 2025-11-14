package grpc

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
)

// Tuple conversions

func toStorageTuple(tuple *openfgav1.Tuple) *storagev1.Tuple {
	if tuple == nil {
		return nil
	}
	return &storagev1.Tuple{
		Key:       toStorageTupleKey(tuple.Key),
		Timestamp: tuple.Timestamp,
	}
}

func fromStorageTuple(tuple *storagev1.Tuple) *openfgav1.Tuple {
	if tuple == nil {
		return nil
	}
	return &openfgav1.Tuple{
		Key:       fromStorageTupleKey(tuple.Key),
		Timestamp: tuple.Timestamp,
	}
}

func fromStorageTuples(tuples []*storagev1.Tuple) []*openfgav1.Tuple {
	if tuples == nil {
		return nil
	}
	result := make([]*openfgav1.Tuple, len(tuples))
	for i, t := range tuples {
		result[i] = fromStorageTuple(t)
	}
	return result
}

// TupleKey conversions

func toStorageTupleKey(key *openfgav1.TupleKey) *storagev1.TupleKey {
	if key == nil {
		return nil
	}
	return &storagev1.TupleKey{
		User:      key.User,
		Relation:  key.Relation,
		Object:    key.Object,
		Condition: toStorageRelationshipCondition(key.Condition),
	}
}

func fromStorageTupleKey(key *storagev1.TupleKey) *openfgav1.TupleKey {
	if key == nil {
		return nil
	}
	return &openfgav1.TupleKey{
		User:      key.User,
		Relation:  key.Relation,
		Object:    key.Object,
		Condition: fromStorageRelationshipCondition(key.Condition),
	}
}

// RelationshipCondition conversions

func toStorageRelationshipCondition(cond *openfgav1.RelationshipCondition) *storagev1.RelationshipCondition {
	if cond == nil {
		return nil
	}
	return &storagev1.RelationshipCondition{
		Name:    cond.Name,
		Context: cond.Context,
	}
}

func fromStorageRelationshipCondition(cond *storagev1.RelationshipCondition) *openfgav1.RelationshipCondition {
	if cond == nil {
		return nil
	}
	return &openfgav1.RelationshipCondition{
		Name:    cond.Name,
		Context: cond.Context,
	}
}

// ObjectRelation conversions

func toStorageObjectRelation(obj *openfgav1.ObjectRelation) *storagev1.ObjectRelation {
	if obj == nil {
		return nil
	}
	return &storagev1.ObjectRelation{
		Object:   obj.Object,
		Relation: obj.Relation,
	}
}

func fromStorageObjectRelation(obj *storagev1.ObjectRelation) *openfgav1.ObjectRelation {
	if obj == nil {
		return nil
	}
	return &openfgav1.ObjectRelation{
		Object:   obj.Object,
		Relation: obj.Relation,
	}
}

func toStorageObjectRelations(objs []*openfgav1.ObjectRelation) []*storagev1.ObjectRelation {
	if objs == nil {
		return nil
	}
	result := make([]*storagev1.ObjectRelation, len(objs))
	for i, obj := range objs {
		result[i] = toStorageObjectRelation(obj)
	}
	return result
}

// RelationReference conversions

func toStorageRelationReference(ref *openfgav1.RelationReference) *storagev1.RelationReference {
	if ref == nil {
		return nil
	}

	result := &storagev1.RelationReference{
		Type:      ref.Type,
		Condition: ref.Condition,
	}

	switch r := ref.RelationOrWildcard.(type) {
	case *openfgav1.RelationReference_Relation:
		result.RelationOrWildcard = &storagev1.RelationReference_Relation{
			Relation: r.Relation,
		}
	case *openfgav1.RelationReference_Wildcard:
		result.RelationOrWildcard = &storagev1.RelationReference_Wildcard{
			Wildcard: &storagev1.Wildcard{},
		}
	}

	return result
}

func fromStorageRelationReference(ref *storagev1.RelationReference) *openfgav1.RelationReference {
	if ref == nil {
		return nil
	}

	result := &openfgav1.RelationReference{
		Type:      ref.Type,
		Condition: ref.Condition,
	}

	switch r := ref.RelationOrWildcard.(type) {
	case *storagev1.RelationReference_Relation:
		result.RelationOrWildcard = &openfgav1.RelationReference_Relation{
			Relation: r.Relation,
		}
	case *storagev1.RelationReference_Wildcard:
		result.RelationOrWildcard = &openfgav1.RelationReference_Wildcard{
			Wildcard: &openfgav1.Wildcard{},
		}
	}

	return result
}

func toStorageRelationReferences(refs []*openfgav1.RelationReference) []*storagev1.RelationReference {
	if refs == nil {
		return nil
	}
	result := make([]*storagev1.RelationReference, len(refs))
	for i, ref := range refs {
		result[i] = toStorageRelationReference(ref)
	}
	return result
}

