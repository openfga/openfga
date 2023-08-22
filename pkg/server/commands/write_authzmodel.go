package commands

import (
	"context"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
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
func (w *WriteAuthorizationModelCommand) Execute(ctx context.Context, req *openfgav1.WriteAuthorizationModelRequest) (*openfgav1.WriteAuthorizationModelResponse, error) {
	// Until this is solved: https://github.com/envoyproxy/protoc-gen-validate/issues/74
	if len(req.GetTypeDefinitions()) > w.backend.MaxTypesPerAuthorizationModel() {
		return nil, serverErrors.ExceededEntityLimit("type definitions in an authorization model", w.backend.MaxTypesPerAuthorizationModel())
	}

	// Fill in the schema version for old requests, which don't contain it, while we migrate to the new schema version.
	if req.SchemaVersion == "" {
		req.SchemaVersion = typesystem.SchemaVersion1_1
	}

	model := &openfgav1.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   req.GetSchemaVersion(),
		TypeDefinitions: req.GetTypeDefinitions(),
		Conditions:      req.GetConditions(),
	}

	_, err := typesystem.NewAndValidate(ctx, model)
	if err != nil {
		return nil, serverErrors.InvalidAuthorizationModelInput(err)
	}

	err = w.backend.WriteAuthorizationModel(ctx, req.GetStoreId(), model)
	if err != nil {
		return nil, serverErrors.NewInternalError("Error writing authorization model configuration", err)
	}

	return &openfgav1.WriteAuthorizationModelResponse{
		AuthorizationModelId: model.Id,
	}, nil
}
