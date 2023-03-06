package commands

import (
	"context"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// WriteAuthorizationModelCommand performs updates of the store authorization model.
type WriteAuthorizationModelCommand struct {
	backend       storage.TypeDefinitionWriteBackend
	logger        logger.Logger
	allowSchema10 bool
}

func NewWriteAuthorizationModelCommand(
	backend storage.TypeDefinitionWriteBackend,
	logger logger.Logger,
	allowSchema10 bool,
) *WriteAuthorizationModelCommand {
	return &WriteAuthorizationModelCommand{
		backend:       backend,
		logger:        logger,
		allowSchema10: allowSchema10,
	}
}

// IsAuthorizationModelObsolete returns whether the model schema is allowed or it is obsolete
func IsAuthorizationModelObsolete(schemaVersion string, allowSchema10 bool) error {
	if !allowSchema10 && schemaVersion == typesystem.SchemaVersion1_0 {
		return serverErrors.ObsoleteAuthorizationModel()
	}
	return nil
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

	err := IsAuthorizationModelObsolete(req.SchemaVersion, w.allowSchema10)
	if err != nil {
		return nil, err
	}

	model := &openfgapb.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   req.GetSchemaVersion(),
		TypeDefinitions: req.GetTypeDefinitions(),
	}

	_, err = typesystem.NewAndValidate(model)
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
