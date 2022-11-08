package commands

import (
	"context"
	"errors"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/validation"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type WriteAssertionsCommand struct {
	datastore storage.OpenFGADatastore
	logger    logger.Logger
}

func NewWriteAssertionsCommand(
	datastore storage.OpenFGADatastore,
	logger logger.Logger,
) *WriteAssertionsCommand {
	return &WriteAssertionsCommand{
		datastore: datastore,
		logger:    logger,
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

	for _, assertion := range assertions {
		if _, err := validation.ValidateTuple(ctx, model, assertion.TupleKey); err != nil {
			return nil, serverErrors.HandleTupleValidateError(err)
		}
	}

	err = w.datastore.WriteAssertions(ctx, store, modelID, assertions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgapb.WriteAssertionsResponse{}, nil
}
