package commands

import (
	"context"
	"errors"

	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type WriteAssertionsCommand struct {
	datastore          storage.OpenFGADatastore
	logger             logger.Logger
	typesystemResolver typesystem.TypesystemResolverFunc
}

func NewWriteAssertionsCommand(
	datastore storage.OpenFGADatastore,
	logger logger.Logger,
	typesystemResolver typesystem.TypesystemResolverFunc,
) *WriteAssertionsCommand {
	return &WriteAssertionsCommand{
		datastore:          datastore,
		logger:             logger,
		typesystemResolver: typesystemResolver,
	}
}

func (w *WriteAssertionsCommand) Execute(ctx context.Context, req *openfgapb.WriteAssertionsRequest) (*openfgapb.WriteAssertionsResponse, error) {
	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	assertions := req.GetAssertions()

	typesys, err := w.typesystemResolver(ctx, store, modelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}

		return nil, serverErrors.HandleError("", err)
	}

	if !typesystem.IsSchemaVersionSupported(typesys.GetSchemaVersion()) {
		return nil, serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion)
	}

	for _, assertion := range assertions {
		if err := validation.ValidateUserObjectRelation(typesys, assertion.TupleKey); err != nil {
			return nil, serverErrors.ValidationError(err)
		}
	}

	err = w.datastore.WriteAssertions(ctx, store, modelID, assertions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgapb.WriteAssertionsResponse{}, nil
}
