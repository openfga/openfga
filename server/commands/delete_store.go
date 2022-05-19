package commands

import (
	"context"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type DeleteStoreCommand struct {
	storesBackend storage.StoresBackend
	logger        logger.Logger
}

func NewDeleteStoreCommand(
	storesBackend storage.StoresBackend,
	logger logger.Logger,
) *DeleteStoreCommand {
	return &DeleteStoreCommand{
		storesBackend: storesBackend,
		logger:        logger,
	}
}

func (s *DeleteStoreCommand) Execute(ctx context.Context, req *openfgapb.DeleteStoreRequest) (*openfgapb.DeleteStoreResponse, error) {
	store, err := s.storesBackend.GetStore(ctx, req.StoreId)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &openfgapb.DeleteStoreResponse{}, nil
		}

		return nil, serverErrors.HandleError("", err)
	}

	utils.LogDBStats(ctx, s.logger, "DeleteStore", 1, 1)

	if err := s.storesBackend.DeleteStore(ctx, store.Id); err != nil {
		return nil, serverErrors.HandleError("Error deleting store", err)
	}
	return &openfgapb.DeleteStoreResponse{}, nil
}
