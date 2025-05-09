package commands

import (
	"context"
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

type UpdateStoreCommand struct {
	storesBackend storage.StoresBackend
	logger        logger.Logger
}

type UpdateStoreCmdOption func(*UpdateStoreCommand)

func WithUpdateStoreCmdLogger(l logger.Logger) UpdateStoreCmdOption {
	return func(c *UpdateStoreCommand) {
		c.logger = l
	}
}

func NewUpdateStoreCommand(
	storesBackend storage.StoresBackend,
	opts ...UpdateStoreCmdOption,
) *UpdateStoreCommand {
	cmd := &UpdateStoreCommand{
		storesBackend: storesBackend,
		logger:        logger.NewNoopLogger(),
	}
	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

func (s *UpdateStoreCommand) Execute(ctx context.Context, req *openfgav1.UpdateStoreRequest) (*openfgav1.UpdateStoreResponse, error) {
	storeID := req.GetStoreId()

	store, err := s.storesBackend.GetStore(ctx, storeID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, serverErrors.ErrStoreIDNotFound
		}

		return nil, serverErrors.HandleError("", err)
	}

	response := openfgav1.UpdateStoreResponse{
		Id:        storeID,
		Name:      req.GetName(),
		CreatedAt: store.GetCreatedAt(),
		UpdatedAt: store.GetUpdatedAt(),
	}

	if store.GetName() == req.GetName() {
		return &response, nil
	}

	updatedStore, err := s.storesBackend.UpdateStore(ctx, req)
	if err != nil {
		return nil, serverErrors.HandleError("Error updating store", err)
	}

	response.UpdatedAt = updatedStore.GetUpdatedAt()
	return &response, nil
}
