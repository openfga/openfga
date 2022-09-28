package commands

import (
	"context"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/typesystem"
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
	// Until this is solved: https://github.com/envoyproxy/protoc-gen-validate/issues/74
	if len(req.GetTypeDefinitions()) > w.backend.MaxTypesInTypeDefinition() {
		return nil, serverErrors.ExceededEntityLimit("type definitions in an authorization model", w.backend.MaxTypesInTypeDefinition())
	}

	id, err := id.NewString()
	if err != nil {
		return nil, err
	}

	model, err := typesystem.Validate(&openfgapb.AuthorizationModel{
		Id:              id,
		SchemaVersion:   req.GetSchemaVersion(),
		TypeDefinitions: req.GetTypeDefinitions(),
	})
	if err != nil {
		return nil, serverErrors.InvalidAuthorizationModelInput(err)
	}

	utils.LogDBStats(ctx, w.logger, "WriteAuthzModel", 0, 1)
	err = w.backend.WriteAuthorizationModel(ctx, req.GetStoreId(), model)
	if err != nil {
		return nil, serverErrors.NewInternalError("Error writing authorization model configuration", err)
	}

	return &openfgapb.WriteAuthorizationModelResponse{
		AuthorizationModelId: id,
	}, nil
}
