package commands

import (
	"context"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
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

func (s *DeleteStoreCommand) Execute(ctx context.Context, req *openfgav1pb.DeleteStoreRequest) error {
	store, err := s.storesBackend.GetStore(ctx, req.StoreId)
	if err != nil {
		if errors.Is(err, storage.NotFound) {
			return nil
		}

		return serverErrors.HandleError("", err)
	}

	utils.LogDBStats(ctx, s.logger, "DeleteStore", 1, 1)

	if err := s.storesBackend.DeleteStore(ctx, store.Id); err != nil {
		return serverErrors.HandleError("Error deleting store", err)
	}

	return nil
}
