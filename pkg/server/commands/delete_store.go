package commands

import (
	"context"
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
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

func (s *DeleteStoreCommand) Execute(ctx context.Context, req *openfgav1.DeleteStoreRequest) (*openfgav1.DeleteStoreResponse, error) {
	store, err := s.storesBackend.GetStore(ctx, req.StoreId)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &openfgav1.DeleteStoreResponse{}, nil
		}

		return nil, serverErrors.HandleError("", err)
	}

	if err := s.storesBackend.DeleteStore(ctx, store.Id); err != nil {
		return nil, serverErrors.HandleError("Error deleting store", err)
	}
	return &openfgav1.DeleteStoreResponse{}, nil
}
