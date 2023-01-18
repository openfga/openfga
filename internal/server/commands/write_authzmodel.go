package commands

import (
	"context"

	"github.com/oklog/ulid/v2"
	serverErrors "github.com/openfga/openfga/internal/server/errors"
	"github.com/openfga/openfga/internal/storage"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/typesystem"
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
	if len(req.GetTypeDefinitions()) > w.backend.MaxTypesPerAuthorizationModel() {
		return nil, serverErrors.ExceededEntityLimit("type definitions in an authorization model", w.backend.MaxTypesPerAuthorizationModel())
	}

	// Fill in the schema version for old requests, which don't contain it, while we migrate to the new schema version.
	// In the future mark this field as required in the protobufs.
	if req.SchemaVersion == "" {
		req.SchemaVersion = typesystem.SchemaVersion1_0
	}

	model := &openfgapb.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   req.GetSchemaVersion(),
		TypeDefinitions: req.GetTypeDefinitions(),
	}

	err := typesystem.Validate(model)
	if err != nil {
		return nil, serverErrors.InvalidAuthorizationModelInput(err)
	}

	err = w.backend.WriteAuthorizationModel(ctx, req.GetStoreId(), model)
	if err != nil {
		return nil, serverErrors.NewInternalError("Error writing authorization model configuration", err)
	}

	return &openfgapb.WriteAuthorizationModelResponse{
		AuthorizationModelId: model.Id,
	}, nil
}
