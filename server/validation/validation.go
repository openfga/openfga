package validation

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// ValidateTuple checks whether a tuple is welformed and valid according to the provided model.
func ValidateTuple(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {

	typesys := typesystem.New(model)

	if err := ValidateUser(model, tk); err != nil {
		return err
	}

	if err := ValidateObject(model, tk); err != nil {
		return err
	}

	if err := ValidateRelation(model, tk); err != nil {
		return err
	}

	// now we assume our tuple is well-formed, it's time to check
	// the tuple against other model and type-restriction constraints

	err := validateTuplesetRestrictions(model, tk)
	if err != nil {
		return err
	}

	objectType := tuple.GetType(tk.GetObject())
	relation := tk.GetRelation()

	hasTypeInfo, err := typesys.HasTypeInfo(objectType, relation)
	if err != nil {
		return err
	}

	if hasTypeInfo {
		err := validateTypeRestrictions(model, tk)
		if err != nil {
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
func validateTuplesetRestrictions(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {

	typesys := typesystem.New(model)

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
	if rewrite != nil && reflect.TypeOf(rewrite) != reflect.TypeOf(&openfgapb.Userset_This{}) {
		return &tuple.InvalidTupleError{
			Reason:   fmt.Sprintf("unexpected rewrite encountered with tupelset relation '%s#%s'", objectType, relation),
			TupleKey: tk,
		}
	}

	user := tk.GetUser()

	// if a tupleset relation is related to an object but not a typed wildcard (e.g. 'user:*')
	// then it is valid
	if tuple.IsValidObject(user) && !tuple.IsTypedWildcard(user) {
		return nil
	}

	// tupleset relation involving a wildcard (covers the '*' and 'type:*' cases)
	if tuple.IsWildcard(user) {
		return &tuple.InvalidTupleError{
			Reason:   fmt.Sprintf("unexpected wildcard relationship with tupleset relation '%s#%s'", objectType, relation),
			TupleKey: tk,
		}
	}

	// tupleset relation involving a userset (e.g. object#relation)
	return &tuple.InvalidTupleError{
		Reason:   fmt.Sprintf("unexpected userset relationship '%s' with tupleset relation '%s#%s'", user, objectType, relation),
		TupleKey: tk,
	}
}

// validateTypeRestrictions makes sure the type restrictions are enforced.
// 1. If the tuple is of the form (user=person:bob, relation=reader, object=doc:budget), then the type "doc", relation "reader" must allow type "person".
// 2. If the tuple is of the form (user=group:abc#member, relation=reader, object=doc:budget), then the type "doc", relation "reader" must allow type "group", relation "member".
// 3. If the tuple is of the form (user=*, relation=reader, object=doc:budget), we allow it only if the type "doc" relation "reader" allows at least one type (with no relation)
func validateTypeRestrictions(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {
	objectType := tuple.GetType(tk.GetObject())           // e.g. "doc"
	userType, userID := tuple.SplitObject(tk.GetUser())   // e.g. (person, bob) or (group, abc#member) or ("", *)
	_, userRel := tuple.SplitObjectRelation(tk.GetUser()) // e.g. (person:bob, "") or (group:abc, member) or (*, "")

	ts := typesystem.New(model)

	typeDefinitionForObject, ok := ts.GetTypeDefinition(objectType)
	if !ok {
		return &tuple.InvalidTupleError{
			Reason:   fmt.Sprintf("type '%s' does not exist in the authorization model", objectType),
			TupleKey: tk,
		}
	}

	relationsForObject := typeDefinitionForObject.GetMetadata().GetRelations()
	if relationsForObject == nil {
		if ts.GetSchemaVersion() == typesystem.SchemaVersion1_1 {
			// if we get here, there's a bug in the validation of WriteAuthorizationModel API
			msg := "invalid authorization model"
			return fmt.Errorf(msg)
		}

		// authorization model is old/unspecified and does not have type information
		return nil
	}

	// at this point we know the auth model has type information
	if userType != "" {
		if _, ok := ts.GetTypeDefinition(userType); !ok {
			return &tuple.InvalidTupleError{
				Reason:   fmt.Sprintf("undefined type for user '%s'", tk.GetUser()),
				TupleKey: tk,
			}
		}
	}

	relationInformation := relationsForObject[tk.Relation]

	// case 1
	if userRel == "" && userID != "*" {
		for _, typeInformation := range relationInformation.GetDirectlyRelatedUserTypes() {
			if typeInformation.GetType() == userType {
				return nil
			}
		}
	} else if userRel != "" { // case 2
		for _, typeInformation := range relationInformation.GetDirectlyRelatedUserTypes() {
			if typeInformation.GetType() == userType && typeInformation.GetRelation() == userRel {
				return nil
			}
		}
	} else if userID == "*" { // case 3
		for _, typeInformation := range relationInformation.GetDirectlyRelatedUserTypes() {
			if typeInformation.GetType() != "" && typeInformation.GetRelation() == "" {
				return nil
			}
		}
	}

	return &tuple.InvalidTupleError{Reason: fmt.Sprintf("User '%s' is not allowed to have relation %s with %s", tk.User, tk.Relation, tk.Object), TupleKey: tk}
}

// NoopFilterFunc returns a filter function that does not filter out any tuple provided
// to it.
func NoopFilterFunc() storage.TupleKeyFilterFunc {
	return func(tupleKey *openfgapb.TupleKey) bool {
		return true
	}
}

// FilterInvalidTuples implements the TupleFilterFunc signature and can be used to provide
// a generic filtering mechanism when reading tuples. It is particularly useful to filter
// out tuples that aren't valid according to the provided model, which can help filter
// tuples that were introduced due to another authorization model.
func FilterInvalidTuples(model *openfgapb.AuthorizationModel) storage.TupleKeyFilterFunc {

	return func(tupleKey *openfgapb.TupleKey) bool {

		err := ValidateTuple(model, tupleKey)
		return err == nil
	}
}

// validateObject validates the provided object string 'type:id' against the provided
// model. An object is considered valid if it validates against one of the type
// definitions included in the provided model.
func ValidateObject(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {

	object := tk.GetObject()

	if !tuple.IsValidObject(object) {
		return &tuple.InvalidObjectFormatError{TupleKey: tk}
	}

	typesys := typesystem.New(model)

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
func ValidateRelation(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {

	object := tk.GetObject()
	relation := tk.GetRelation()

	if !tuple.IsValidRelation(relation) {
		return &tuple.InvalidTupleError{Reason: "invalid relation", TupleKey: tk}
	}

	typesys := typesystem.New(model)

	objectType := tuple.GetType(object)

	_, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
			return &tuple.TypeNotFoundError{TypeName: objectType}
		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {
			return &tuple.RelationNotFoundError{Relation: relation, TypeName: objectType, TupleKey: tk}
		}

		return err
	}

	return nil
}

// ValidateUser validates the 'user' field in the provided tuple key by validating
// that it meets the model constraints. For 1.0 and 1.1 models if the user field is
// a userset value, then the objectType and relation must be defined. For 1.1 models
// the user field must either be a userset or an object, and if it's an object we
// verify the objectType is defined in the model.
func ValidateUser(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {

	if !tuple.IsValidUser(tk.GetUser()) {
		return &tuple.InvalidTupleError{Reason: "the 'user' field is invalid", TupleKey: tk}
	}

	typesys := typesystem.New(model)
	schemaVersion := typesys.GetSchemaVersion()

	user := tk.GetUser()

	// the 'user' field must be an object (e.g. 'type:id') or object#relation (e.g. 'type:id#relation')
	if schemaVersion == typesystem.SchemaVersion1_1 {
		if !tuple.IsValidObject(user) && !tuple.IsObjectRelation(user) {
			return &tuple.InvalidTupleError{Reason: "the 'user' field must be an object (e.g. document:1) or an 'object#relation' or a typed wildcard (e.g. group:*)"}
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
				return &tuple.RelationNotFoundError{Relation: userRelation, TypeName: userObjectType, TupleKey: tk}
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
