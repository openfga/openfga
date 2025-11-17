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

// AuthorizationModel conversions

func toStorageAuthorizationModel(model *openfgav1.AuthorizationModel) *storagev1.AuthorizationModel {
	if model == nil {
		return nil
	}

	return &storagev1.AuthorizationModel{
		Id:              model.GetId(),
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: toStorageTypeDefinitions(model.GetTypeDefinitions()),
		Conditions:      toStorageConditions(model.GetConditions()),
	}
}

func fromStorageAuthorizationModel(model *storagev1.AuthorizationModel) *openfgav1.AuthorizationModel {
	if model == nil {
		return nil
	}

	return &openfgav1.AuthorizationModel{
		Id:              model.GetId(),
		SchemaVersion:   model.GetSchemaVersion(),
		TypeDefinitions: fromStorageTypeDefinitions(model.GetTypeDefinitions()),
		Conditions:      fromStorageConditions(model.GetConditions()),
	}
}

func toStorageAuthorizationModels(models []*openfgav1.AuthorizationModel) []*storagev1.AuthorizationModel {
	if models == nil {
		return nil
	}

	result := make([]*storagev1.AuthorizationModel, len(models))
	for i, m := range models {
		result[i] = toStorageAuthorizationModel(m)
	}
	return result
}

func fromStorageAuthorizationModels(models []*storagev1.AuthorizationModel) []*openfgav1.AuthorizationModel {
	if models == nil {
		return nil
	}

	result := make([]*openfgav1.AuthorizationModel, len(models))
	for i, m := range models {
		result[i] = fromStorageAuthorizationModel(m)
	}
	return result
}

// TypeDefinition conversions

func toStorageTypeDefinition(td *openfgav1.TypeDefinition) *storagev1.TypeDefinition {
	if td == nil {
		return nil
	}

	return &storagev1.TypeDefinition{
		Type:      td.GetType(),
		Relations: toStorageUsersetMap(td.GetRelations()),
		Metadata:  toStorageMetadata(td.GetMetadata()),
	}
}

func fromStorageTypeDefinition(td *storagev1.TypeDefinition) *openfgav1.TypeDefinition {
	if td == nil {
		return nil
	}

	return &openfgav1.TypeDefinition{
		Type:      td.GetType(),
		Relations: fromStorageUsersetMap(td.GetRelations()),
		Metadata:  fromStorageMetadata(td.GetMetadata()),
	}
}

func toStorageTypeDefinitions(tds []*openfgav1.TypeDefinition) []*storagev1.TypeDefinition {
	if tds == nil {
		return nil
	}

	result := make([]*storagev1.TypeDefinition, len(tds))
	for i, td := range tds {
		result[i] = toStorageTypeDefinition(td)
	}
	return result
}

func fromStorageTypeDefinitions(tds []*storagev1.TypeDefinition) []*openfgav1.TypeDefinition {
	if tds == nil {
		return nil
	}

	result := make([]*openfgav1.TypeDefinition, len(tds))
	for i, td := range tds {
		result[i] = fromStorageTypeDefinition(td)
	}
	return result
}

// Userset conversions

func toStorageUserset(us *openfgav1.Userset) *storagev1.Userset {
	if us == nil {
		return nil
	}

	result := &storagev1.Userset{}

	switch u := us.GetUserset().(type) {
	case *openfgav1.Userset_This:
		result.Userset = &storagev1.Userset_This{
			This: &storagev1.DirectUserset{},
		}
	case *openfgav1.Userset_ComputedUserset:
		result.Userset = &storagev1.Userset_ComputedUserset{
			ComputedUserset: toStorageObjectRelation(u.ComputedUserset),
		}
	case *openfgav1.Userset_TupleToUserset:
		result.Userset = &storagev1.Userset_TupleToUserset{
			TupleToUserset: toStorageTupleToUserset(u.TupleToUserset),
		}
	case *openfgav1.Userset_Union:
		result.Userset = &storagev1.Userset_Union{
			Union: toStorageUsersets(u.Union),
		}
	case *openfgav1.Userset_Intersection:
		result.Userset = &storagev1.Userset_Intersection{
			Intersection: toStorageUsersets(u.Intersection),
		}
	case *openfgav1.Userset_Difference:
		result.Userset = &storagev1.Userset_Difference{
			Difference: toStorageDifference(u.Difference),
		}
	}

	return result
}

func fromStorageUserset(us *storagev1.Userset) *openfgav1.Userset {
	if us == nil {
		return nil
	}

	result := &openfgav1.Userset{}

	switch u := us.GetUserset().(type) {
	case *storagev1.Userset_This:
		result.Userset = &openfgav1.Userset_This{
			This: &openfgav1.DirectUserset{},
		}
	case *storagev1.Userset_ComputedUserset:
		result.Userset = &openfgav1.Userset_ComputedUserset{
			ComputedUserset: fromStorageObjectRelation(u.ComputedUserset),
		}
	case *storagev1.Userset_TupleToUserset:
		result.Userset = &openfgav1.Userset_TupleToUserset{
			TupleToUserset: fromStorageTupleToUserset(u.TupleToUserset),
		}
	case *storagev1.Userset_Union:
		result.Userset = &openfgav1.Userset_Union{
			Union: fromStorageUsersets(u.Union),
		}
	case *storagev1.Userset_Intersection:
		result.Userset = &openfgav1.Userset_Intersection{
			Intersection: fromStorageUsersets(u.Intersection),
		}
	case *storagev1.Userset_Difference:
		result.Userset = &openfgav1.Userset_Difference{
			Difference: fromStorageDifference(u.Difference),
		}
	}

	return result
}

func toStorageUsersetMap(usersets map[string]*openfgav1.Userset) map[string]*storagev1.Userset {
	if usersets == nil {
		return nil
	}

	result := make(map[string]*storagev1.Userset, len(usersets))
	for k, v := range usersets {
		result[k] = toStorageUserset(v)
	}
	return result
}

func fromStorageUsersetMap(usersets map[string]*storagev1.Userset) map[string]*openfgav1.Userset {
	if usersets == nil {
		return nil
	}

	result := make(map[string]*openfgav1.Userset, len(usersets))
	for k, v := range usersets {
		result[k] = fromStorageUserset(v)
	}
	return result
}

// Usersets conversions

func toStorageUsersets(usersets *openfgav1.Usersets) *storagev1.Usersets {
	if usersets == nil {
		return nil
	}

	children := make([]*storagev1.Userset, len(usersets.GetChild()))
	for i, child := range usersets.GetChild() {
		children[i] = toStorageUserset(child)
	}

	return &storagev1.Usersets{
		Child: children,
	}
}

func fromStorageUsersets(usersets *storagev1.Usersets) *openfgav1.Usersets {
	if usersets == nil {
		return nil
	}

	children := make([]*openfgav1.Userset, len(usersets.GetChild()))
	for i, child := range usersets.GetChild() {
		children[i] = fromStorageUserset(child)
	}

	return &openfgav1.Usersets{
		Child: children,
	}
}

// Difference conversions

func toStorageDifference(diff *openfgav1.Difference) *storagev1.Difference {
	if diff == nil {
		return nil
	}

	return &storagev1.Difference{
		Base:     toStorageUserset(diff.GetBase()),
		Subtract: toStorageUserset(diff.GetSubtract()),
	}
}

func fromStorageDifference(diff *storagev1.Difference) *openfgav1.Difference {
	if diff == nil {
		return nil
	}

	return &openfgav1.Difference{
		Base:     fromStorageUserset(diff.GetBase()),
		Subtract: fromStorageUserset(diff.GetSubtract()),
	}
}

// TupleToUserset conversions

func toStorageTupleToUserset(ttu *openfgav1.TupleToUserset) *storagev1.TupleToUserset {
	if ttu == nil {
		return nil
	}

	return &storagev1.TupleToUserset{
		Tupleset:        toStorageObjectRelation(ttu.GetTupleset()),
		ComputedUserset: toStorageObjectRelation(ttu.GetComputedUserset()),
	}
}

func fromStorageTupleToUserset(ttu *storagev1.TupleToUserset) *openfgav1.TupleToUserset {
	if ttu == nil {
		return nil
	}

	return &openfgav1.TupleToUserset{
		Tupleset:        fromStorageObjectRelation(ttu.GetTupleset()),
		ComputedUserset: fromStorageObjectRelation(ttu.GetComputedUserset()),
	}
}

// Metadata conversions

func toStorageMetadata(meta *openfgav1.Metadata) *storagev1.Metadata {
	if meta == nil {
		return nil
	}

	return &storagev1.Metadata{
		Relations:  toStorageRelationMetadataMap(meta.GetRelations()),
		Module:     meta.GetModule(),
		SourceInfo: toStorageSourceInfo(meta.GetSourceInfo()),
	}
}

func fromStorageMetadata(meta *storagev1.Metadata) *openfgav1.Metadata {
	if meta == nil {
		return nil
	}

	return &openfgav1.Metadata{
		Relations:  fromStorageRelationMetadataMap(meta.GetRelations()),
		Module:     meta.GetModule(),
		SourceInfo: fromStorageSourceInfo(meta.GetSourceInfo()),
	}
}

// RelationMetadata conversions

func toStorageRelationMetadata(rm *openfgav1.RelationMetadata) *storagev1.RelationMetadata {
	if rm == nil {
		return nil
	}

	return &storagev1.RelationMetadata{
		DirectlyRelatedUserTypes: toStorageRelationReferences(rm.GetDirectlyRelatedUserTypes()),
		Module:                   rm.GetModule(),
		SourceInfo:               toStorageSourceInfo(rm.GetSourceInfo()),
	}
}

func fromStorageRelationMetadata(rm *storagev1.RelationMetadata) *openfgav1.RelationMetadata {
	if rm == nil {
		return nil
	}

	return &openfgav1.RelationMetadata{
		DirectlyRelatedUserTypes: fromStorageRelationReferences(rm.GetDirectlyRelatedUserTypes()),
		Module:                   rm.GetModule(),
		SourceInfo:               fromStorageSourceInfo(rm.GetSourceInfo()),
	}
}

func toStorageRelationMetadataMap(relations map[string]*openfgav1.RelationMetadata) map[string]*storagev1.RelationMetadata {
	if relations == nil {
		return nil
	}

	result := make(map[string]*storagev1.RelationMetadata, len(relations))
	for k, v := range relations {
		result[k] = toStorageRelationMetadata(v)
	}
	return result
}

func fromStorageRelationMetadataMap(relations map[string]*storagev1.RelationMetadata) map[string]*openfgav1.RelationMetadata {
	if relations == nil {
		return nil
	}

	result := make(map[string]*openfgav1.RelationMetadata, len(relations))
	for k, v := range relations {
		result[k] = fromStorageRelationMetadata(v)
	}
	return result
}

func fromStorageRelationReferences(refs []*storagev1.RelationReference) []*openfgav1.RelationReference {
	if refs == nil {
		return nil
	}

	result := make([]*openfgav1.RelationReference, len(refs))
	for i, ref := range refs {
		result[i] = fromStorageRelationReference(ref)
	}
	return result
}

// SourceInfo conversions

func toStorageSourceInfo(si *openfgav1.SourceInfo) *storagev1.SourceInfo {
	if si == nil {
		return nil
	}

	return &storagev1.SourceInfo{
		File: si.GetFile(),
	}
}

func fromStorageSourceInfo(si *storagev1.SourceInfo) *openfgav1.SourceInfo {
	if si == nil {
		return nil
	}

	return &openfgav1.SourceInfo{
		File: si.GetFile(),
	}
}

// Condition conversions

func toStorageCondition(cond *openfgav1.Condition) *storagev1.Condition {
	if cond == nil {
		return nil
	}

	return &storagev1.Condition{
		Name:       cond.GetName(),
		Expression: cond.GetExpression(),
		Parameters: toStorageConditionParamTypeRefMap(cond.GetParameters()),
	}
}

func fromStorageCondition(cond *storagev1.Condition) *openfgav1.Condition {
	if cond == nil {
		return nil
	}

	return &openfgav1.Condition{
		Name:       cond.GetName(),
		Expression: cond.GetExpression(),
		Parameters: fromStorageConditionParamTypeRefMap(cond.GetParameters()),
	}
}

func toStorageConditions(conditions map[string]*openfgav1.Condition) map[string]*storagev1.Condition {
	if conditions == nil {
		return nil
	}

	result := make(map[string]*storagev1.Condition, len(conditions))
	for k, v := range conditions {
		result[k] = toStorageCondition(v)
	}
	return result
}

func fromStorageConditions(conditions map[string]*storagev1.Condition) map[string]*openfgav1.Condition {
	if conditions == nil {
		return nil
	}

	result := make(map[string]*openfgav1.Condition, len(conditions))
	for k, v := range conditions {
		result[k] = fromStorageCondition(v)
	}
	return result
}

// ConditionParamTypeRef conversions

func toStorageConditionParamTypeRef(ref *openfgav1.ConditionParamTypeRef) *storagev1.ConditionParamTypeRef {
	if ref == nil {
		return nil
	}

	genericTypes := make([]*storagev1.ConditionParamTypeRef, len(ref.GetGenericTypes()))
	for i, gt := range ref.GetGenericTypes() {
		genericTypes[i] = toStorageConditionParamTypeRef(gt)
	}

	return &storagev1.ConditionParamTypeRef{
		TypeName:     storagev1.ConditionParamTypeRef_TypeName(ref.GetTypeName()),
		GenericTypes: genericTypes,
	}
}

func fromStorageConditionParamTypeRef(ref *storagev1.ConditionParamTypeRef) *openfgav1.ConditionParamTypeRef {
	if ref == nil {
		return nil
	}

	genericTypes := make([]*openfgav1.ConditionParamTypeRef, len(ref.GetGenericTypes()))
	for i, gt := range ref.GetGenericTypes() {
		genericTypes[i] = fromStorageConditionParamTypeRef(gt)
	}

	return &openfgav1.ConditionParamTypeRef{
		TypeName:     openfgav1.ConditionParamTypeRef_TypeName(ref.GetTypeName()),
		GenericTypes: genericTypes,
	}
}

func toStorageConditionParamTypeRefMap(params map[string]*openfgav1.ConditionParamTypeRef) map[string]*storagev1.ConditionParamTypeRef {
	if params == nil {
		return nil
	}

	result := make(map[string]*storagev1.ConditionParamTypeRef, len(params))
	for k, v := range params {
		result[k] = toStorageConditionParamTypeRef(v)
	}
	return result
}

func fromStorageConditionParamTypeRefMap(params map[string]*storagev1.ConditionParamTypeRef) map[string]*openfgav1.ConditionParamTypeRef {
	if params == nil {
		return nil
	}

	result := make(map[string]*openfgav1.ConditionParamTypeRef, len(params))
	for k, v := range params {
		result[k] = fromStorageConditionParamTypeRef(v)
	}
	return result
}

// Store conversions

func toStorageStore(store *openfgav1.Store) *storagev1.Store {
	if store == nil {
		return nil
	}

	return &storagev1.Store{
		Id:        store.GetId(),
		Name:      store.GetName(),
		CreatedAt: store.GetCreatedAt(),
		UpdatedAt: store.GetUpdatedAt(),
		DeletedAt: store.GetDeletedAt(),
	}
}

func fromStorageStore(store *storagev1.Store) *openfgav1.Store {
	if store == nil {
		return nil
	}

	return &openfgav1.Store{
		Id:        store.GetId(),
		Name:      store.GetName(),
		CreatedAt: store.GetCreatedAt(),
		UpdatedAt: store.GetUpdatedAt(),
		DeletedAt: store.GetDeletedAt(),
	}
}

func fromStorageStores(stores []*storagev1.Store) []*openfgav1.Store {
	if stores == nil {
		return nil
	}

	result := make([]*openfgav1.Store, len(stores))
	for i, s := range stores {
		result[i] = fromStorageStore(s)
	}
	return result
}

// TupleChange conversions

func toStorageTupleChange(change *openfgav1.TupleChange) *storagev1.TupleChange {
	if change == nil {
		return nil
	}

	return &storagev1.TupleChange{
		TupleKey:  toStorageTupleKey(change.GetTupleKey()),
		Operation: storagev1.TupleOperation(change.GetOperation()),
		Timestamp: change.GetTimestamp(),
	}
}

func fromStorageTupleChange(change *storagev1.TupleChange) *openfgav1.TupleChange {
	if change == nil {
		return nil
	}

	return &openfgav1.TupleChange{
		TupleKey:  fromStorageTupleKey(change.GetTupleKey()),
		Operation: openfgav1.TupleOperation(change.GetOperation()),
		Timestamp: change.GetTimestamp(),
	}
}

func toStorageTupleChanges(changes []*openfgav1.TupleChange) []*storagev1.TupleChange {
	if changes == nil {
		return nil
	}

	result := make([]*storagev1.TupleChange, len(changes))
	for i, c := range changes {
		result[i] = toStorageTupleChange(c)
	}
	return result
}

func fromStorageTupleChanges(changes []*storagev1.TupleChange) []*openfgav1.TupleChange {
	if changes == nil {
		return nil
	}

	result := make([]*openfgav1.TupleChange, len(changes))
	for i, c := range changes {
		result[i] = fromStorageTupleChange(c)
	}
	return result
}
