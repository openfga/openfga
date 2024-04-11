package listusers

import (
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	serverErrors "github.com/openfga/openfga/pkg/server/errors"

	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/typesystem"
)

func ValidateListUsersRequest(req *openfgav1.ListUsersRequest, typesys *typesystem.TypeSystem) error {
	if err := validateContextualTuples(req, typesys); err != nil {
		return err
	}

	if err := validateUsersFilters(req, typesys); err != nil {
		return err
	}

	return validateTargetRelation(req, typesys)
}

func validateContextualTuples(request *openfgav1.ListUsersRequest, typeSystem *typesystem.TypeSystem) error {
	for _, contextualTuple := range request.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTuple(typeSystem, contextualTuple); err != nil {
			return serverErrors.HandleTupleValidateError(err)
		}
	}

	return nil
}

func validateUsersFilters(request *openfgav1.ListUsersRequest, typeSystem *typesystem.TypeSystem) error {
	for _, userFilter := range request.GetUserFilters() {
		if err := validateUserFilter(typeSystem, userFilter); err != nil {
			return err
		}
	}

	return nil
}

func validateUserFilter(typeSystem *typesystem.TypeSystem, usersFilter *openfgav1.ListUsersFilter) error {
	filterObjectType := usersFilter.GetType()

	if _, typeExists := typeSystem.GetTypeDefinition(filterObjectType); !typeExists {
		return serverErrors.TypeNotFound(filterObjectType)
	}

	return validateUserFilterRelation(typeSystem, usersFilter, filterObjectType)
}

func validateUserFilterRelation(typeSystem *typesystem.TypeSystem, usersFilter *openfgav1.ListUsersFilter, filterObjectType string) error {
	filterObjectRelation := usersFilter.GetRelation()
	if filterObjectRelation == "" {
		return nil
	}

	_, err := typeSystem.GetRelation(filterObjectType, filterObjectRelation)
	if err == nil {
		return nil
	}

	if errors.Is(err, typesystem.ErrRelationUndefined) {
		return serverErrors.RelationNotFound(filterObjectRelation, filterObjectType, nil)
	}

	return serverErrors.HandleError("", err)
}

func validateTargetRelation(request *openfgav1.ListUsersRequest, typeSystem *typesystem.TypeSystem) error {
	objectType := request.GetObject().GetType()
	targetRelation := request.GetRelation()

	_, err := typeSystem.GetRelation(objectType, targetRelation)
	if err == nil {
		return nil
	}

	if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
		return serverErrors.TypeNotFound(objectType)
	}

	if errors.Is(err, typesystem.ErrRelationUndefined) {
		return serverErrors.RelationNotFound(targetRelation, objectType, nil)
	}

	return serverErrors.HandleError("", err)
}
