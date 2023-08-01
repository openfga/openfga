package commands

import (
	"context"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

type CreateStoreCommand struct {
	storesBackend storage.StoresBackend
	logger        logger.Logger
}

func NewCreateStoreCommand(
	storesBackend storage.StoresBackend,
	logger logger.Logger,
) *CreateStoreCommand {
	return &CreateStoreCommand{
		storesBackend: storesBackend,
		logger:        logger,
	}
}

func (s *CreateStoreCommand) Execute(ctx context.Context, req *openfgav1.CreateStoreRequest) (*openfgav1.CreateStoreResponse, error) {
	store, err := s.storesBackend.CreateStore(ctx, &openfgav1.Store{
		Id:   ulid.Make().String(),
		Name: req.Name,
	})
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgav1.CreateStoreResponse{
		Id:        store.Id,
		Name:      store.Name,
		CreatedAt: store.CreatedAt,
		UpdatedAt: store.UpdatedAt,
	}, nil
}
