package commands

import (
	"context"

	"github.com/oklog/ulid/v2"
	serverErrors "github.com/openfga/openfga/internal/server/errors"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
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

func (s *CreateStoreCommand) Execute(ctx context.Context, req *openfgapb.CreateStoreRequest) (*openfgapb.CreateStoreResponse, error) {
	store, err := s.storesBackend.CreateStore(ctx, &openfgapb.Store{
		Id:   ulid.Make().String(),
		Name: req.Name,
	})
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgapb.CreateStoreResponse{
		Id:        store.Id,
		Name:      store.Name,
		CreatedAt: store.CreatedAt,
		UpdatedAt: store.UpdatedAt,
	}, nil
}
