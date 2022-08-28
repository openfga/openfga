package commands

import (
	"context"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
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

	dbCallsCounter := utils.NewDBCallCounter()
	for _, assertion := range assertions {
		if _, err := validation.ValidateTuple(ctx, w.datastore, store, modelID, assertion.TupleKey, dbCallsCounter); err != nil {
			return nil, serverErrors.HandleTupleValidateError(err)
		}
	}
	dbCallsCounter.AddWriteCall()
	utils.LogDBStats(ctx, w.logger, "WriteAssertions", dbCallsCounter.GetReadCalls(), dbCallsCounter.GetWriteCalls())

	err := w.datastore.WriteAssertions(ctx, store, modelID, assertions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgapb.WriteAssertionsResponse{}, nil
}
