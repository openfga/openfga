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
	datastore     storage.OpenFGADatastore
	logger        logger.Logger
	allowSchema10 bool
}

func NewWriteAssertionsCommand(
	datastore storage.OpenFGADatastore,
	logger logger.Logger,
	allowSchema10 bool,
) *WriteAssertionsCommand {
	return &WriteAssertionsCommand{
		datastore:     datastore,
		logger:        logger,
		allowSchema10: allowSchema10,
	}
}

func (w *WriteAssertionsCommand) Execute(ctx context.Context, req *openfgapb.WriteAssertionsRequest) (*openfgapb.WriteAssertionsResponse, error) {
	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	assertions := req.GetAssertions()

	model, err := w.datastore.ReadAuthorizationModel(ctx, store, modelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(req.GetAuthorizationModelId())
		}

		return nil, serverErrors.HandleError("", err)
	}
	err = IsAuthorizationModelObsolete(model.SchemaVersion, w.allowSchema10)
	if err != nil {
		return nil, err
	}

	typesys := typesystem.New(model)

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
