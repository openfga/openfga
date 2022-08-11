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
	if err := w.validateAuthorizationModel(req.GetTypeDefinitions().GetTypeDefinitions()); err != nil {
		return nil, err
	}

	id, err := id.NewString()
	if err != nil {
		return nil, err
	}

	utils.LogDBStats(ctx, w.logger, "WriteAuthzModel", 0, 1)
	if err := w.backend.WriteAuthorizationModel(ctx, req.GetStoreId(), id, req.GetTypeDefinitions()); err != nil {
		return nil, serverErrors.HandleError("Error writing authorization model configuration", err)
	}

	return &openfgapb.WriteAuthorizationModelResponse{
		AuthorizationModelId: id,
	}, nil
}

func (w *WriteAuthorizationModelCommand) validateAuthorizationModel(tds []*openfgapb.TypeDefinition) error {
	types := map[string]bool{}
	topLevelRelations := map[string]bool{}
	var tupleToUsersetTargets []string

	// Until this is solved: https://github.com/envoyproxy/protoc-gen-validate/issues/74
	if len(tds) > w.backend.MaxTypesInTypeDefinition() {
		return serverErrors.ExceededEntityLimit("type definitions in an authorization model", w.backend.MaxTypesInTypeDefinition())
	}
	for _, td := range tds {
		// Don't allow two type definitions with the same type
		if _, ok := types[td.GetType()]; ok {
			return serverErrors.CannotAllowDuplicateTypesInOneRequest
		} else {
			types[td.GetType()] = true
		}

		relations := td.GetRelations()
		for relationName := range relations {
			topLevelRelations[relationName] = true
		}

		targets, err := validateTypeDefinition(td)
		if err != nil {
			return err
		}
		tupleToUsersetTargets = append(tupleToUsersetTargets, targets...)
	}

	for _, relation := range tupleToUsersetTargets {
		if ok := topLevelRelations[relation]; !ok {
			return serverErrors.UnknownRelationWhenWritingAuthzModel(relation)
		}
	}

	return nil
}

func validateTypeDefinition(td *openfgapb.TypeDefinition) ([]string, error) {
	topLevelRelations := map[string]bool{}
	relations := td.GetRelations()
	for relation := range relations {
		topLevelRelations[relation] = true
	}

	var tupleToUsersetTargets []string
	for relation, userset := range relations {
		if userset.GetUserset() == nil {
			return nil, serverErrors.EmptyRelationDefinition(td.GetType(), relation)
		}
		targets, err := validateUserset(topLevelRelations, userset, relation)
		if err != nil {
			return nil, err
		}
		tupleToUsersetTargets = append(tupleToUsersetTargets, targets...)
	}

	return tupleToUsersetTargets, nil
}

// validateUserset ensures that usersets do not contain relations that are not top-level
func validateUserset(topLevelRelations map[string]bool, userset *openfgapb.Userset, name string) ([]string, error) {
	var tupleToUsersetTargets []string

	switch t := userset.GetUserset().(type) {
	case *openfgapb.Userset_ComputedUserset:
		relation := t.ComputedUserset.GetRelation()
		if relation == name {
			return nil, serverErrors.CannotAllowMultipleReferencesToOneRelation
		}
		if ok := topLevelRelations[relation]; !ok {
			return nil, serverErrors.UnknownRelationWhenWritingAuthzModel(relation)
		}
	case *openfgapb.Userset_Union:
		for _, us := range t.Union.GetChild() {
			targets, err := validateUserset(topLevelRelations, us, name)
			if err != nil {
				return nil, err
			}
			tupleToUsersetTargets = append(tupleToUsersetTargets, targets...)
		}
	case *openfgapb.Userset_Intersection:
		for _, us := range t.Intersection.GetChild() {
			targets, err := validateUserset(topLevelRelations, us, name)
			if err != nil {
				return nil, err
			}
			tupleToUsersetTargets = append(tupleToUsersetTargets, targets...)
		}
	case *openfgapb.Userset_Difference:
		targets1, err := validateUserset(topLevelRelations, t.Difference.Base, name)
		if err != nil {
			return nil, err
		}
		tupleToUsersetTargets = append(tupleToUsersetTargets, targets1...)
		targets2, err := validateUserset(topLevelRelations, t.Difference.Subtract, name)
		if err != nil {
			return nil, err
		}
		tupleToUsersetTargets = append(tupleToUsersetTargets, targets2...)
	case *openfgapb.Userset_TupleToUserset:
		relation := t.TupleToUserset.GetTupleset().GetRelation()
		if ok := topLevelRelations[relation]; !ok {
			return nil, serverErrors.UnknownRelationWhenWritingAuthzModel(relation)
		}
		// We don't have enough information to validate these targets here so pass them up to where we do.
		relation = t.TupleToUserset.GetComputedUserset().GetRelation()
		tupleToUsersetTargets = append(tupleToUsersetTargets, relation)
	}

	return tupleToUsersetTargets, nil
}
