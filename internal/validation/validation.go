package validation

import (
	"errors"
	"fmt"
	"reflect"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// ValidateUserObjectRelation returns nil if the tuple is well-formed and valid according to the provided model.
//
// Do NOT use this when reading or writing tuples to storage. Use ValidateTuple instead, because it's stricter.
func ValidateUserObjectRelation(typesys *typesystem.TypeSystem, tk *openfgav1.TupleKey) error {
	if err := ValidateUser(typesys, tk.GetUser()); err != nil {
		return err
	}

	if err := ValidateObject(typesys, tk); err != nil {
		return err
	}

	if err := ValidateRelation(typesys, tk); err != nil {
		return err
	}

	return nil
}

// ValidateTuple returns nil if a tuple is well formed and valid according to the provided model.
// It is a superset of ValidateUserObjectRelation; it also validates TTU relations and type restrictions.
//
// Do NOT use this when validating a tuple that is an input to a Check or WriteAssertions request.
func ValidateTuple(typesys *typesystem.TypeSystem, tk *openfgav1.TupleKey) error {
	if err := ValidateUserObjectRelation(typesys, tk); err != nil {
		return &tuple.InvalidTupleError{Cause: err, TupleKey: tk}
	}

	// now we assume our tuple is well-formed, it's time to check
	// the tuple against other model and type-restriction constraints

	err := validateTuplesetRestrictions(typesys, tk)
	if err != nil {
		return &tuple.InvalidTupleError{Cause: err, TupleKey: tk}
	}

	objectType := tuple.GetType(tk.GetObject())
	relation := tk.GetRelation()

	hasTypeInfo, err := typesys.HasTypeInfo(objectType, relation)
	if err != nil {
		return err
	}

	if hasTypeInfo {
		err := validateTypeRestrictions(typesys, tk)
		if err != nil {
			return &tuple.InvalidTupleError{Cause: err, TupleKey: tk}
		}

		if err := ValidateCondition(typesys, tk); err != nil {
			return err
		}
	}

	return nil
}

// validateTuplesetRestrictions validates the provided TupleKey against tupleset restrictions.
//
// Given a rewrite definition such as 'viewer from parent', the 'parent' relation is known as the
// tupleset. This method ensures the following are *not* possible:
//
// 1. `document:1#parent@folder:1#parent` (cannot evaluate/assign a userset value to a tupleset relation)
// 2. `document:1#parent@*` (cannot evaluate/assign untyped wildcard to a tupleset relation (1.0 models))
// 3. `document:1#parent@folder:*` (cannot evaluate/assign typed wildcard to a tupleset relation (1.1. models))
func validateTuplesetRestrictions(typesys *typesystem.TypeSystem, tk *openfgav1.TupleKey) error {
	objectType := tuple.GetType(tk.GetObject())
	relation := tk.GetRelation()

	isTupleset, err := typesys.IsTuplesetRelation(objectType, relation)
	if err != nil {
		return err
	}

	if !isTupleset {
		return nil
	}

	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		return err
	}

	rewrite := rel.GetRewrite().GetUserset()

	// tupleset relation involving a rewrite
	if rewrite != nil && reflect.TypeOf(rewrite) != reflect.TypeOf(&openfgav1.Userset_This{}) {
		return fmt.Errorf("unexpected rewrite encountered with tupleset relation '%s#%s'", objectType, relation)
	}

	user := tk.GetUser()

	// if a tupleset relation is related to an object but not a typed wildcard (e.g. 'user:*')
	// then it is valid
	if tuple.IsValidObject(user) && !tuple.IsTypedWildcard(user) {
		return nil
	}

	// tupleset relation involving a wildcard (covers the '*' and 'type:*' cases)
	if tuple.IsWildcard(user) {
		return fmt.Errorf("unexpected wildcard relationship with tupleset relation '%s#%s'", objectType, relation)
	}

	// tupleset relation involving a userset (e.g. object#relation) or a user_id (e.g. not a valid object)
	if !tuple.IsValidObject(user) {
		return fmt.Errorf("unexpected user '%s' with tupleset relation '%s#%s'", user, objectType, relation)
	}

	return nil
}

// validateTypeRestrictions makes sure the type restrictions are enforced.
// 1. If the tuple is of the form doc:budget#reader@person:bob, then 'doc#reader' must allow type 'person'.
// 2. If the tuple is of the form doc:budget#reader@group:abc#member, then 'doc#reader' must allow 'group#member'.
// 3. If the tuple is of the form doc:budget#reader@person:*, we allow it only if 'doc#reader' allows the typed wildcard 'person:*'.
func validateTypeRestrictions(typesys *typesystem.TypeSystem, tk *openfgav1.TupleKey) error {
	objectType := tuple.GetType(tk.GetObject())           // e.g. "doc"
	userType, _ := tuple.SplitObject(tk.GetUser())        // e.g. (person, bob) or (group, abc#member) or ("", person:*)
	_, userRel := tuple.SplitObjectRelation(tk.GetUser()) // e.g. (person:bob, "") or (group:abc, member) or (person:*, "")

	typeDefinitionForObject, ok := typesys.GetTypeDefinition(objectType)
	if !ok {
		return fmt.Errorf("type '%s' does not exist in the authorization model", objectType)
	}

	relationsForObject := typeDefinitionForObject.GetMetadata().GetRelations()

	relationInformation := relationsForObject[tk.Relation]

	user := tk.GetUser()

	if tuple.IsObjectRelation(user) {
		// case 2 documented above
		for _, typeInformation := range relationInformation.GetDirectlyRelatedUserTypes() {
			if typeInformation.GetType() == userType && typeInformation.GetRelation() == userRel {
				return nil
			}
		}

		return fmt.Errorf("'%s#%s' is not an allowed type restriction for '%s#%s'", userType, userRel, objectType, tk.GetRelation())
	}

	if tuple.IsTypedWildcard(user) {
		// case 3 documented above
		for _, typeInformation := range relationInformation.GetDirectlyRelatedUserTypes() {
			if typeInformation.GetType() == userType && typeInformation.GetWildcard() != nil {
				return nil
			}
		}

		return fmt.Errorf("the typed wildcard '%s' is not an allowed type restriction for '%s#%s'", user, objectType, tk.GetRelation())
	}

	// the user must be an object (case 1), so check directly against the objectType
	for _, typeInformation := range relationInformation.GetDirectlyRelatedUserTypes() {
		if typeInformation.GetType() == userType && typeInformation.GetWildcard() == nil && typeInformation.GetRelation() == "" {
			return nil
		}
	}

	return fmt.Errorf("type '%s' is not an allowed type restriction for '%s#%s'", userType, objectType, tk.GetRelation())
}

// ValidateCondition enforces conditions on a relationship tuple
func ValidateCondition(typesys *typesystem.TypeSystem, tk *openfgav1.TupleKey) error {
	objectType := tuple.GetType(tk.Object)
	userType := tuple.GetType(tk.User)
	userRelation := tuple.GetRelation(tk.User)

	typeRestrictions, err := typesys.GetDirectlyRelatedUserTypes(objectType, tk.Relation)
	if err != nil {
		return err
	}

	if tk.Condition == nil {
		for _, directlyRelatedType := range typeRestrictions {
			if directlyRelatedType.Condition != "" {
				continue
			}

			if directlyRelatedType.Type != userType {
				continue
			}

			if directlyRelatedType.GetRelationOrWildcard() != nil {
				if directlyRelatedType.GetRelation() != "" && directlyRelatedType.GetRelation() != userRelation {
					continue
				}

				if directlyRelatedType.GetWildcard() != nil && !tuple.IsTypedWildcard(tk.User) {
					continue
				}
			}

			return nil
		}

		return &tuple.InvalidConditionalTupleError{
			Cause: fmt.Errorf("condition is missing"), TupleKey: tk,
		}
	}

	condition, ok := typesys.GetConditions()[tk.Condition.Name]
	if !ok {
		return &tuple.InvalidConditionalTupleError{
			Cause: fmt.Errorf("undefined condition"), TupleKey: tk,
		}
	}

	validCondition := false
	for _, directlyRelatedType := range typeRestrictions {
		if directlyRelatedType.Type == userType && directlyRelatedType.Condition == tk.Condition.Name {
			validCondition = true
			break
		}
	}

	if !validCondition {
		return &tuple.InvalidConditionalTupleError{
			Cause: fmt.Errorf("invalid condition for type restriction"), TupleKey: tk,
		}
	}

	contextStruct := tk.GetCondition().GetContext()
	contextFieldMap := contextStruct.GetFields()

	typedParams, err := condition.CastContextToTypedParameters(contextFieldMap)
	if err != nil {
		return &tuple.InvalidConditionalTupleError{
			Cause: err, TupleKey: tk,
		}
	}

	for key := range contextFieldMap {
		_, ok := typedParams[key]
		if !ok {
			return &tuple.InvalidConditionalTupleError{
				Cause:    fmt.Errorf("found invalid context parameter: %s", key),
				TupleKey: tk,
			}
		}
	}

	return nil
}

// FilterInvalidTuples implements the TupleFilterFunc signature and can be used to provide
// a generic filtering mechanism when reading tuples. It is particularly useful to filter
// out tuples that aren't valid according to the provided model, which can help filter
// tuples that were introduced due to another authorization model.
func FilterInvalidTuples(typesys *typesystem.TypeSystem) storage.TupleKeyFilterFunc {
	return func(tupleKey *openfgav1.TupleKey) bool {
		err := ValidateTuple(typesys, tupleKey)
		return err == nil
	}
}

// ValidateObject validates the provided object string 'type:id' against the provided
// model. An object is considered valid if it validates against one of the type
// definitions included in the provided model.
func ValidateObject(typesys *typesystem.TypeSystem, tk *openfgav1.TupleKey) error {
	object := tk.GetObject()

	if !tuple.IsValidObject(object) {
		return fmt.Errorf("invalid 'object' field format")
	}

	if tuple.IsTypedWildcard(object) {
		return fmt.Errorf("the 'object' field cannot reference a typed wildcard")
	}

	objectType := tuple.GetType(object)
	_, ok := typesys.GetTypeDefinition(objectType)
	if !ok {
		return &tuple.TypeNotFoundError{TypeName: objectType}
	}

	return nil
}

// ValidateRelation validates the relation on the provided objectType against the given model.
// A relation is valid if it is defined as a relation for the type definition of the given
// objectType.
func ValidateRelation(typesys *typesystem.TypeSystem, tk *openfgav1.TupleKey) error {
	object := tk.GetObject()
	relation := tk.GetRelation()

	if !tuple.IsValidRelation(relation) {
		return fmt.Errorf("the 'relation' field is malformed")
	}

	objectType := tuple.GetType(object)

	_, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
			return &tuple.TypeNotFoundError{TypeName: objectType}
		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {
			return &tuple.RelationNotFoundError{Relation: relation, TypeName: objectType}
		}

		return err
	}

	return nil
}

// ValidateUser validates the 'user' string provided by validating that it meets
// the model constraints. For 1.0 and 1.1 models if the user field is a userset
// value, then the objectType and relation must be defined. For 1.1 models the
// user field must either be a userset or an object, and if it's an object we
// verify the objectType is defined in the model.
func ValidateUser(typesys *typesystem.TypeSystem, user string) error {
	if !tuple.IsValidUser(user) {
		return fmt.Errorf("the 'user' field is malformed")
	}

	schemaVersion := typesys.GetSchemaVersion()

	// the 'user' field must be an object (e.g. 'type:id') or object#relation (e.g. 'type:id#relation')
	if schemaVersion == typesystem.SchemaVersion1_1 {
		if !tuple.IsValidObject(user) && !tuple.IsObjectRelation(user) {
			return fmt.Errorf("the 'user' field must be an object (e.g. document:1) or an 'object#relation' or a typed wildcard (e.g. group:*)")
		}

		if tuple.IsObjectRelation(user) {
			userObj, _ := tuple.SplitObjectRelation(user)

			if tuple.IsTypedWildcard(userObj) {
				return fmt.Errorf("the 'user' field cannot reference a typed wildcard in a userset value")
			}
		}
	}

	userObject, userRelation := tuple.SplitObjectRelation(user)
	userObjectType := tuple.GetType(userObject)

	// for 1.0 and 1.1 models if the 'user' field is a userset then we validate the 'object#relation'
	// by making sure the user objectType and relation are defined in the model.
	if tuple.IsObjectRelation(user) {
		_, err := typesys.GetRelation(userObjectType, userRelation)
		if err != nil {
			if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
				return &tuple.TypeNotFoundError{TypeName: userObjectType}
			}

			if errors.Is(err, typesystem.ErrRelationUndefined) {
				return &tuple.RelationNotFoundError{Relation: userRelation, TypeName: userObjectType}
			}
		}
	}

	// if the model is a 1.1 model we make sure that the objectType of the 'user' field is a defined
	// type in the model.
	if schemaVersion == typesystem.SchemaVersion1_1 {
		_, ok := typesys.GetTypeDefinition(userObjectType)
		if !ok {
			return &tuple.TypeNotFoundError{TypeName: userObjectType}
		}
	}

	return nil
}
