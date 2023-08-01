package commands

import (
	"context"
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

type GetStoreQuery struct {
	logger        logger.Logger
	storesBackend storage.StoresBackend
}

func NewGetStoreQuery(storesBackend storage.StoresBackend, logger logger.Logger) *GetStoreQuery {
	return &GetStoreQuery{
		logger:        logger,
		storesBackend: storesBackend,
	}
}

func (q *GetStoreQuery) Execute(ctx context.Context, req *openfgav1.GetStoreRequest) (*openfgav1.GetStoreResponse, error) {
	storeID := req.GetStoreId()
	store, err := q.storesBackend.GetStore(ctx, storeID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, serverErrors.StoreIDNotFound
		}
		return nil, serverErrors.HandleError("", err)
	}
	return &openfgav1.GetStoreResponse{
		Id:        store.Id,
		Name:      store.Name,
		CreatedAt: store.CreatedAt,
		UpdatedAt: store.UpdatedAt,
	}, nil
}
