package commands

import (
	"context"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
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

func (s *CreateStoreCommand) Execute(ctx context.Context, req *openfgav1pb.CreateStoreRequest) (*openfgav1pb.CreateStoreResponse, error) {
	storeId, err := id.NewString()
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	store, err := s.storesBackend.CreateStore(ctx, &openfga.Store{
		Id:   storeId,
		Name: req.Name,
	})
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	utils.LogDBStats(ctx, s.logger, "CreateStore", 0, 1)

	return &openfgav1pb.CreateStoreResponse{
		Id:        store.Id,
		Name:      store.Name,
		CreatedAt: store.CreatedAt,
		UpdatedAt: store.UpdatedAt,
	}, nil
}
