package commands

import (
	"context"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var supportedSchemaVersions = map[string]struct{}{"1.0": {}, "1.1": {}}

// WriteAuthorizationModelCommand performs updates of the store authorization model.
type WriteAuthorizationModelCommand struct {
	backend storage.TypeDefinitionWriteBackend
	logger  logger.Logger
}

func NewWriteAuthorizationModelCommand(
	backend storage.TypeDefinitionWriteBackend,
	logger logger.Logger,
) *WriteAuthorizationModelCommand {
	return &WriteAuthorizationModelCommand{
		backend: backend,
		logger:  logger,
	}
}

// Execute the command using the supplied request.
func (w *WriteAuthorizationModelCommand) Execute(ctx context.Context, req *openfgapb.WriteAuthorizationModelRequest) (*openfgapb.WriteAuthorizationModelResponse, error) {
	schemaVersion := req.GetSchemaVersion()
	if schemaVersion == "" {
		schemaVersion = "1.0"
	}

	if _, ok := supportedSchemaVersions[schemaVersion]; !ok {
		return nil, serverErrors.UnsupportedSchemaVersion
	}

	typeDefinitions := req.GetTypeDefinitions().GetTypeDefinitions()

	// Until this is solved: https://github.com/envoyproxy/protoc-gen-validate/issues/74
	if len(typeDefinitions) > w.backend.MaxTypesInTypeDefinition() {
		return nil, serverErrors.ExceededEntityLimit("type definitions in an authorization model", w.backend.MaxTypesInTypeDefinition())
	}

	if err := validateAuthorizationModel(req.GetSchemaVersion(), typeDefinitions); err != nil {
		return nil, err
	}

	id, err := id.NewString()
	if err != nil {
		return nil, err
	}

	utils.LogDBStats(ctx, w.logger, "WriteAuthzModel", 0, 1)
	if err := w.backend.WriteAuthorizationModel(ctx, req.GetStoreId(), id, typeDefinitions); err != nil {
		return nil, serverErrors.NewInternalError("Error writing authorization model configuration", err)
	}

	return &openfgapb.WriteAuthorizationModelResponse{
		AuthorizationModelId: id,
	}, nil
}

// validateAuthorizationModel validates the model according to the following rules:
//  1. Do not allow duplicate types (or duplication relations but that is inherent in the map structure)
//  2. For every rewrite the relations in the rewrite must:
//     a. Be valid relations on the same type in the authorization model (in cases of computedUserset)
//     b. Be valid relations on another existing type (in cases of tupleToUserset)
//
// If it is a 1.1 model (with types on relations), additionally:
//  3. Every type on a relation must be a valid type:
//     a. For a type (e.g. user) this means checking that this type is in the model
//     b. For a type#relation this means checking that this type with this relation is in the model
//  4. Check that a relation is assignable if and only if it has a non-zero list of types
func validateAuthorizationModel(version string, tds []*openfgapb.TypeDefinition) error {
	if containsDuplicateTypes(tds) {
		return serverErrors.CannotAllowDuplicateTypesInOneRequest
	}

	if err := areUsersetRewritesValid(tds); err != nil {
		return err
	}

	if version == "1.1" {
		if err := areRelationalTypesValid(tds); err != nil {
			return err
		}
	}

	return nil
}

func containsDuplicateTypes(tds []*openfgapb.TypeDefinition) bool {
	seenTypes := map[string]struct{}{}

	for _, td := range tds {
		if _, ok := seenTypes[td.GetType()]; ok {
			return true
		}
		seenTypes[td.GetType()] = struct{}{}
	}

	return false
}

func areUsersetRewritesValid(tds []*openfgapb.TypeDefinition) error {
	allRelations := map[string]struct{}{}
	typeToRelations := map[string]map[string]struct{}{}
	for _, td := range tds {
		typeName := td.GetType()
		typeToRelations[typeName] = map[string]struct{}{}
		for relationName := range td.GetRelations() {
			typeToRelations[typeName][relationName] = struct{}{}
			allRelations[relationName] = struct{}{}
		}
	}

	for _, td := range tds {
		for relationName, usersetRewrite := range td.GetRelations() {
			err := isUsersetRewriteValid(allRelations, typeToRelations[td.GetType()], td.GetType(), relationName, usersetRewrite)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// isUsersetRewriteValid checks if a particular userset rewrite is valid. The first argument is all the relations in
// the model, the second argument is the subset of relations on the type where the rewrite occurs.
func isUsersetRewriteValid(allRelations map[string]struct{}, relationsOnType map[string]struct{}, objectType, relation string, usersetRewrite *openfgapb.Userset) error {
	if usersetRewrite.GetUserset() == nil {
		return serverErrors.EmptyRewrites(objectType, relation)
	}

	switch t := usersetRewrite.GetUserset().(type) {
	case *openfgapb.Userset_ComputedUserset:
		computedUserset := t.ComputedUserset.GetRelation()
		if computedUserset == relation {
			return serverErrors.CannotAllowMultipleReferencesToOneRelation
		}
		if _, ok := relationsOnType[computedUserset]; !ok {
			return serverErrors.RelationNotFound(computedUserset, "", nil)
		}
	case *openfgapb.Userset_TupleToUserset:
		tupleset := t.TupleToUserset.GetTupleset().GetRelation()
		if _, ok := allRelations[tupleset]; !ok {
			return serverErrors.RelationNotFound(tupleset, "", nil)
		}

		computedUserset := t.TupleToUserset.GetComputedUserset().GetRelation()
		if _, ok := relationsOnType[computedUserset]; !ok {
			return serverErrors.RelationNotFound(computedUserset, "", nil)
		}
	case *openfgapb.Userset_Union:
		for _, child := range t.Union.GetChild() {
			err := isUsersetRewriteValid(allRelations, relationsOnType, objectType, relation, child)
			if err != nil {
				return err
			}
		}
	case *openfgapb.Userset_Intersection:
		for _, child := range t.Intersection.GetChild() {
			err := isUsersetRewriteValid(allRelations, relationsOnType, objectType, relation, child)
			if err != nil {
				return err
			}
		}
	case *openfgapb.Userset_Difference:
		err := isUsersetRewriteValid(allRelations, relationsOnType, objectType, relation, t.Difference.Base)
		if err != nil {
			return err
		}

		err = isUsersetRewriteValid(allRelations, relationsOnType, objectType, relation, t.Difference.Subtract)
		if err != nil {
			return err
		}
	}

	return nil
}

func areRelationalTypesValid(tds []*openfgapb.TypeDefinition) error {
	typeToRelations := map[string]map[string]struct{}{}
	for _, td := range tds {
		typeName := td.GetType()
		typeToRelations[typeName] = map[string]struct{}{}
		for relationName := range td.GetRelations() {
			typeToRelations[typeName][relationName] = struct{}{}
		}
	}

	// Here we are checking that every type on a relation is valid. This means:
	// 1. If it is a type (e.g. user) then this type is a type in the model
	// 2. If it is a type#relation then this type with this relation is in the model
	for _, td := range tds {
		for _relation, _ := range td.GetRelations() {
			metadata, ok := td.GetMetadata().GetRelations()[_relation]
			if ok {
				for _, userType := range metadata.GetDirectlyRelatedUserTypes() {
					objectType := userType.GetType()
					relation := userType.GetRelation()

					if _, ok := typeToRelations[objectType]; !ok {
						return serverErrors.InvalidRelationType(td.GetType(), _relation, objectType, relation)
					}

					if relation != "" {
						if _, ok := typeToRelations[objectType][relation]; !ok {
							return serverErrors.InvalidRelationType(td.GetType(), _relation, objectType, relation)
						}
					}
				}
			}
		}
	}

	// Finally we check that a relation is assignable if and only if it has a non-zero AssignableRelations array
	for _, td := range tds {
		for relation, rewrite := range td.GetRelations() {
			metadata, ok := td.GetMetadata().GetRelations()[relation]
			if isAssignable(rewrite) {
				if !ok || len(metadata.GetDirectlyRelatedUserTypes()) == 0 {
					return serverErrors.AssignableRelationHasNoTypes(td.GetType(), relation)
				}
			} else {
				if ok && len(metadata.GetDirectlyRelatedUserTypes()) > 0 {
					return serverErrors.NonassignableRelationHasAType(td.GetType(), relation)
				}
			}
		}
	}

	return nil
}

func isAssignable(rewrite *openfgapb.Userset) bool {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_This:
		return true
	case *openfgapb.Userset_Union:
		for _, child := range rw.Union.GetChild() {
			if isAssignable(child) {
				return true
			}
		}
	case *openfgapb.Userset_Intersection:
		for _, child := range rw.Intersection.GetChild() {
			if isAssignable(child) {
				return true
			}
		}
	case *openfgapb.Userset_Difference:
		difference := rw.Difference
		if isAssignable(difference.GetBase()) || isAssignable(difference.GetSubtract()) {
			return true
		}
	}

	return false
}
