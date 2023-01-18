package commands

import (
	"context"
	"errors"

	serverErrors "github.com/openfga/openfga/internal/server/errors"
	"github.com/openfga/openfga/internal/storage"
	"github.com/openfga/openfga/pkg/logger"
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

	if err := s.storesBackend.DeleteStore(ctx, store.Id); err != nil {
		return nil, serverErrors.HandleError("Error deleting store", err)
	}
	return &openfgapb.DeleteStoreResponse{}, nil
}
