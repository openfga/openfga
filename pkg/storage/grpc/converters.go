package grpc

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
)

// Tuple conversions

func toStorageTuple(tuple *openfgav1.Tuple) *storagev1.Tuple {
	if tuple == nil {
		return nil
	}
	return &storagev1.Tuple{
		Key:       toStorageTupleKey(tuple.GetKey()),
		Timestamp: tuple.GetTimestamp(),
	}
}

func fromStorageTuple(tuple *storagev1.Tuple) *openfgav1.Tuple {
	if tuple == nil {
		return nil
	}
	return &openfgav1.Tuple{
		Key:       fromStorageTupleKey(tuple.GetKey()),
		Timestamp: tuple.GetTimestamp(),
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
		User:      key.GetUser(),
		Relation:  key.GetRelation(),
		Object:    key.GetObject(),
		Condition: toStorageRelationshipCondition(key.GetCondition()),
	}
}

func fromStorageTupleKey(key *storagev1.TupleKey) *openfgav1.TupleKey {
	if key == nil {
		return nil
	}
	return &openfgav1.TupleKey{
		User:      key.GetUser(),
		Relation:  key.GetRelation(),
		Object:    key.GetObject(),
		Condition: fromStorageRelationshipCondition(key.GetCondition()),
	}
}

func toStorageTupleKeys(keys []*openfgav1.TupleKey) []*storagev1.TupleKey {
	if keys == nil {
		return nil
	}
	result := make([]*storagev1.TupleKey, len(keys))
	for i, k := range keys {
		result[i] = toStorageTupleKey(k)
	}
	return result
}

// toStorageTupleKeysFromDeletes converts TupleKeyWithoutCondition to TupleKey.
// Note: The condition field will be nil, as expected for delete operations.
func toStorageTupleKeysFromDeletes(keys []*openfgav1.TupleKeyWithoutCondition) []*storagev1.TupleKey {
	if keys == nil {
		return nil
	}
	result := make([]*storagev1.TupleKey, len(keys))
	for i, k := range keys {
		result[i] = &storagev1.TupleKey{
			User:     k.GetUser(),
			Relation: k.GetRelation(),
			Object:   k.GetObject(),
			// condition is intentionally nil for deletes
		}
	}
	return result
}

// RelationshipCondition conversions

func toStorageRelationshipCondition(cond *openfgav1.RelationshipCondition) *storagev1.RelationshipCondition {
	if cond == nil {
		return nil
	}
	return &storagev1.RelationshipCondition{
		Name:    cond.GetName(),
		Context: cond.GetContext(),
	}
}

func fromStorageRelationshipCondition(cond *storagev1.RelationshipCondition) *openfgav1.RelationshipCondition {
	if cond == nil {
		return nil
	}
	return &openfgav1.RelationshipCondition{
		Name:    cond.GetName(),
		Context: cond.GetContext(),
	}
}

// ObjectRelation conversions

func toStorageObjectRelation(obj *openfgav1.ObjectRelation) *storagev1.ObjectRelation {
	if obj == nil {
		return nil
	}
	return &storagev1.ObjectRelation{
		Object:   obj.GetObject(),
		Relation: obj.GetRelation(),
	}
}

func fromStorageObjectRelation(obj *storagev1.ObjectRelation) *openfgav1.ObjectRelation {
	if obj == nil {
		return nil
	}
	return &openfgav1.ObjectRelation{
		Object:   obj.GetObject(),
		Relation: obj.GetRelation(),
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
		Type:      ref.GetType(),
		Condition: ref.GetCondition(),
	}

	switch r := ref.GetRelationOrWildcard().(type) {
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
		Type:      ref.GetType(),
		Condition: ref.GetCondition(),
	}

	switch r := ref.GetRelationOrWildcard().(type) {
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

// TupleWriteOptions conversions

func fromStorageTupleWriteOptions(opts *storagev1.TupleWriteOptions) []storage.TupleWriteOption {
	if opts == nil {
		return nil
	}

	result := []storage.TupleWriteOption{}

	switch opts.GetOnMissingDelete() {
	case storagev1.OnMissingDelete_ON_MISSING_DELETE_IGNORE:
		result = append(result, storage.WithOnMissingDelete(storage.OnMissingDeleteIgnore))
	case storagev1.OnMissingDelete_ON_MISSING_DELETE_ERROR:
		result = append(result, storage.WithOnMissingDelete(storage.OnMissingDeleteError))
	default:
		// Explicitly handle unspecified or unknown values - default to ERROR
		result = append(result, storage.WithOnMissingDelete(storage.OnMissingDeleteError))
	}

	switch opts.GetOnDuplicateInsert() {
	case storagev1.OnDuplicateInsert_ON_DUPLICATE_INSERT_IGNORE:
		result = append(result, storage.WithOnDuplicateInsert(storage.OnDuplicateInsertIgnore))
	case storagev1.OnDuplicateInsert_ON_DUPLICATE_INSERT_ERROR:
		result = append(result, storage.WithOnDuplicateInsert(storage.OnDuplicateInsertError))
	default:
		// Explicitly handle unspecified or unknown values - default to ERROR
		result = append(result, storage.WithOnDuplicateInsert(storage.OnDuplicateInsertError))
	}

	return result
}
