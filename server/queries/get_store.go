package queries

import (
	"context"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
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

func (q *GetStoreQuery) Execute(ctx context.Context, req *openfgav1pb.GetStoreRequest) (*openfgav1pb.GetStoreResponse, error) {
	storeID := req.GetStoreId()
	store, err := q.storesBackend.GetStore(ctx, storeID)
	if err != nil {
		if errors.Is(err, storage.NotFound) {
			return nil, serverErrors.StoreIDNotFound
		}
		return nil, serverErrors.HandleError("", err)
	}
	return &openfgav1pb.GetStoreResponse{
		Id:        store.Id,
		Name:      store.Name,
		CreatedAt: store.CreatedAt,
		UpdatedAt: store.UpdatedAt,
	}, nil
}
