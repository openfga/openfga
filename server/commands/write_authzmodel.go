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
	typeDefinitions := req.GetTypeDefinitions().GetTypeDefinitions()

	// Until this is solved: https://github.com/envoyproxy/protoc-gen-validate/issues/74
	if len(typeDefinitions) > w.backend.MaxTypesInTypeDefinition() {
		return nil, serverErrors.ExceededEntityLimit("type definitions in an authorization model", w.backend.MaxTypesInTypeDefinition())
	}

	if err := validateAuthorizationModel(typeDefinitions); err != nil {
		return nil, err
	}

	id, err := id.NewString()
	if err != nil {
		return nil, err
	}

	utils.LogDBStats(ctx, w.logger, "WriteAuthzModel", 0, 1)
	if err := w.backend.WriteAuthorizationModel(ctx, req.GetStoreId(), id, typeDefinitions); err != nil {
		return nil, serverErrors.HandleError("Error writing authorization model configuration", err)
	}

	return &openfgapb.WriteAuthorizationModelResponse{
		AuthorizationModelId: id,
	}, nil
}

func validateAuthorizationModel(tds []*openfgapb.TypeDefinition) error {
	if containsDuplicateType(tds) {
		return serverErrors.CannotAllowDuplicateTypesInOneRequest
	}

	if err := areUsersetRewritesValid(tds); err != nil {
		return err
	}

	return nil
}

func containsDuplicateType(tds []*openfgapb.TypeDefinition) bool {
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
	typeRelations := map[string]map[string]struct{}{}
	for _, td := range tds {
		typeName := td.GetType()
		typeRelations[typeName] = map[string]struct{}{}
		for relationName := range td.GetRelations() {
			typeRelations[typeName][relationName] = struct{}{}
			allRelations[relationName] = struct{}{}
		}
	}

	for _, td := range tds {
		for relationName, usersetRewrite := range td.GetRelations() {
			err := isUsersetRewriteValid(allRelations, typeRelations[td.GetType()], relationName, usersetRewrite)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func isUsersetRewriteValid(allRelations map[string]struct{}, typeRelations map[string]struct{}, relationName string, usersetRewrite *openfgapb.Userset) error {
	if usersetRewrite.GetUserset() == nil {
		return serverErrors.EmptyRewrites(relationName)
	}

	switch t := usersetRewrite.GetUserset().(type) {
	case *openfgapb.Userset_ComputedUserset:
		relation := t.ComputedUserset.GetRelation()
		if relation == relationName {
			return serverErrors.CannotAllowMultipleReferencesToOneRelation
		}
		if _, ok := typeRelations[relation]; !ok {
			return serverErrors.RelationNotFound(relation, "", nil)
		}
	case *openfgapb.Userset_TupleToUserset:
		tupleset := t.TupleToUserset.GetTupleset().GetRelation()
		if _, ok := allRelations[tupleset]; !ok {
			return serverErrors.RelationNotFound(tupleset, "", nil)
		}

		computedUserset := t.TupleToUserset.GetComputedUserset().GetRelation()
		if _, ok := typeRelations[computedUserset]; !ok {
			return serverErrors.RelationNotFound(computedUserset, "", nil)
		}
	case *openfgapb.Userset_Union:
		for _, child := range t.Union.GetChild() {
			err := isUsersetRewriteValid(allRelations, typeRelations, relationName, child)
			if err != nil {
				return err
			}
		}
	case *openfgapb.Userset_Intersection:
		for _, child := range t.Intersection.GetChild() {
			err := isUsersetRewriteValid(allRelations, typeRelations, relationName, child)
			if err != nil {
				return err
			}
		}
	case *openfgapb.Userset_Difference:
		err := isUsersetRewriteValid(allRelations, typeRelations, relationName, t.Difference.Base)
		if err != nil {
			return err
		}

		err = isUsersetRewriteValid(allRelations, typeRelations, relationName, t.Difference.Subtract)
		if err != nil {
			return err
		}
	}

	return nil
}
