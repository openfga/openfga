package commands

import (
	"context"
	"errors"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type UpdateStoreCommand struct {
	storesBackend storage.StoresBackend
	logger        logger.Logger
}

func NewUpdateStoreCommand(
	storesBackend storage.StoresBackend,
	logger logger.Logger,
) *UpdateStoreCommand {
	return &UpdateStoreCommand{
		storesBackend: storesBackend,
		logger:        logger,
	}
}

func (s *UpdateStoreCommand) Execute(ctx context.Context, req *openfgapb.UpdateStoreRequest) (*openfgapb.UpdateStoreResponse, error) {
	store, err := s.storesBackend.GetStore(ctx, req.StoreId)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &openfgapb.UpdateStoreResponse{}, nil
		}

		return nil, serverErrors.HandleError("", err)
	}

	updatedStore, err := s.storesBackend.UpdateStore(ctx, store.Id, req.Name)
	if err != nil {
		return nil, serverErrors.HandleError("Error updating store", err)
	}

	return &openfgapb.UpdateStoreResponse{
		Id:        updatedStore.Id,
		Name:      updatedStore.Name,
		CreatedAt: updatedStore.CreatedAt,
		UpdatedAt: updatedStore.UpdatedAt,
	}, nil
}
