package validation

import (
	"errors"
	"fmt"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// ValidateTuple checks whether a tuple is valid. If it is not, returns as error. A tuple is valid
// if it validates according to the following rules:
//
// Rule 1:
// Rule 2:
//
// Rule 3: Wildcards '*' are not peritted on tupleset relations.
// If there exists a rewrite definition of the form `viewer from parent` anywhere in the model, then no * may be written to that 'parent' relation.
//
// Rule 4: Usersets 'object#relation' are not permitted on tupleset relations.
// If there exists a rewrite definition of the form 'viewer from parent' anywhere in the model, then no userset may be written to that 'parent' relation.
//
// etc..
func ValidateTuple(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {

	typesys := typesystem.New(model)

	if err := ValidateObject(tk, model); err != nil {
		return err
	}

	if err := ValidateRelation(tk, model); err != nil {
		return err
	}

	if err := ValidateUser(tk, model); err != nil {
		return err
	}

	// now we assume our tuple is well-formed, it's time to check
	// the tuple against other model and type-restriction constraints

	err := validateTuplesetRestrictions(model, tk)
	if err != nil {
		return err
	}

	objectType, _ := tuple.SplitObject(tk.GetObject())
	relation := tk.GetRelation()

	hasTypeInfo, err := typesys.HasTypeInfo(objectType, relation)
	if err != nil {
		// todo: handle error
	}

	if hasTypeInfo {
		// err := validateTypeRestrictions()
		// if err != nil {

		// }
	}

	return nil
}

// validateTuplesetRestrictions validates the provided TupleKey against tupleset restrictions.
//
// Given a rewrite definition such as 'viewer from parent', the 'parent' relation is known as the
// tupleset. This method ensures the following are *not* possible:
//
// 1. `document:1#parent@folder:1#parent` (cannot assign a userset value to a tupleset relation)
// 2. `document:1#parent@*` (cannot assign untyped wildcard to a tupleset relation (1.0 models))
// 3. `document:1#parent@folder:*` (cannot assign typed wildcard to a tupleset relation (1.1. models))
func validateTuplesetRestrictions(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {

	// todo(jon-whit): is case #3 necessary to validate here?? Technically it would be an invalid model, so
	// tuple validations here on that would seem overkill

	objectType, _ := tuple.SplitObject(tk.GetObject())

	typesys := typesystem.New(model)

	// validate tupleset relations involving userset (user) values
	for _, ttuDefinitions := range typesys.GetAllTupleToUsersetsDefinitions()[objectType] {
		for _, ttuDef := range ttuDefinitions {
			if ttuDef.Tupleset.Relation == tk.Relation {
				return fmt.Errorf("Userset '%s' is not allowed to have relation '%s' with '%s'", tk.User, tk.Relation, tk.Object)
			}
		}
	}

	return nil
}

func validateTypeRestrictions(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {
	objectType, _ := tuple.SplitObject(tk.GetObject())    // e.g. "doc"
	userType, userID := tuple.SplitObject(tk.GetUser())   // e.g. (person, bob) or (group, abc#member) or ("", *)
	_, userRel := tuple.SplitObjectRelation(tk.GetUser()) // e.g. (person:bob, "") or (group:abc, member) or (*, "")

	ts := typesystem.New(model)

	typeDefinitionForObject, ok := ts.GetTypeDefinition(objectType)
	if !ok {
		msg := fmt.Sprintf("type '%s' does not exist in the authorization model", objectType)
		//return serverErrors.NewInternalError(msg, errors.New(msg))
		return fmt.Errorf(msg)
	}

	relationsForObject := typeDefinitionForObject.GetMetadata().GetRelations()
	if relationsForObject == nil {
		if ts.GetSchemaVersion() == typesystem.SchemaVersion1_1 {
			// if we get here, there's a bug in the validation of WriteAuthorizationModel API
			msg := "invalid authorization model"
			//return serverErrors.NewInternalError(msg, errors.New(msg))
			return fmt.Errorf(msg)
		} else {
			// authorization model is old/unspecified and does not have type information
			return nil
		}
	}

	// at this point we know the auth model has type information
	if userType != "" {
		if _, ok := ts.GetTypeDefinition(userType); !ok {
			//return serverErrors.InvalidWriteInput
			return fmt.Errorf("todo: update me")
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

	//return serverErrors.InvalidTuple(fmt.Sprintf("User '%s' is not allowed to have relation %s with %s", tk.User, tk.Relation, tk.Object), tk)
	return fmt.Errorf("todo: Update me")
}

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

// ValidateObject validates the provided object string 'type:id' against the provided
// model. An object is valid if it is welformed AND it validates against one of the
// type definitions included in the provided model.
func ValidateObject(tk *openfgapb.TupleKey, model *openfgapb.AuthorizationModel) error {

	object := tk.GetObject()

	if !tuple.IsValidObject(object) {
		return &tuple.InvalidObjectFormatError{TupleKey: tk}
	}

	objectType, objectID := tuple.SplitObject(object)
	if objectType == "" || objectID == "" {
		return &tuple.InvalidObjectFormatError{TupleKey: tk}
	}

	typesys := typesystem.New(model)
	_, ok := typesys.GetTypeDefinition(objectType)
	if !ok {
		return &tuple.TypeNotFoundError{TypeName: objectType}
	}

	return nil
}

// ValidateRelation validates the relation on the provided objectType against the given model.
// A relation is valid if it is welformed AND it is defined as a relation for the type definition
// of the given objectType.
func ValidateRelation(tk *openfgapb.TupleKey, model *openfgapb.AuthorizationModel) error {

	relation := tk.GetRelation()
	objectType, _ := tuple.SplitObject(tk.GetObject())

	if !tuple.IsValidRelation(relation) {
		return &tuple.InvalidTupleError{Reason: "invalid relation", TupleKey: tk}
	}

	// todo(jon-whit): memoize this
	typesys := typesystem.New(model)

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
// that it is welformed and that it meets the model constraints. For 1.0 and 1.1
// models if the user field is a userset value, then the objectType and relation
// must be defined. For 1.1 models the user field must either be a userset or an
// object, and if it's an object we verify the objectType is defined in the model.
func ValidateUser(tk *openfgapb.TupleKey, model *openfgapb.AuthorizationModel) error {

	user := tk.GetUser()

	typesys := typesystem.New(model)
	schemaVersion := typesys.GetSchemaVersion()

	if !tuple.IsValidUser(user) {
		return &tuple.InvalidTupleError{Reason: "the 'user' field is invalid", TupleKey: tk}
	}

	// the 'user' field must be an object (e.g. 'type:id') or object#relation (e.g. 'type:id#relation')
	if schemaVersion == typesystem.SchemaVersion1_1 {
		if !tuple.IsValidObject(user) && !tuple.IsObjectRelation(user) {
			return &tuple.InvalidTupleError{Reason: "the 'user' field must be an object (e.g. document:1) or an 'object#relation'"}
		}
	}

	userObject, userRelation := tuple.SplitObjectRelation(user)
	userObjectType, _ := tuple.SplitObject(userObject)

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
