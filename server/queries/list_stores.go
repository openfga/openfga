package queries

import (
	"context"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ListStoresQuery struct {
	logger        logger.Logger
	encoder       encoder.Encoder
	storesBackend storage.StoresBackend
}

func NewListStoresQuery(storesBackend storage.StoresBackend, encoder encoder.Encoder, logger logger.Logger) *ListStoresQuery {
	return &ListStoresQuery{
		logger:        logger,
		encoder:       encoder,
		storesBackend: storesBackend,
	}
}

func (q *ListStoresQuery) Execute(ctx context.Context, req *openfgapb.ListStoresRequest) (*openfgapb.ListStoresResponse, error) {
	decodedContToken, err := q.encoder.Decode(req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}
	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))
	utils.LogDBStats(ctx, q.logger, "ListStores", 1, 0)

	stores, continuationToken, err := q.storesBackend.ListStores(ctx, paginationOptions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	encodedToken, err := q.encoder.Encode(continuationToken)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	resp := &openfgapb.ListStoresResponse{
		Stores:            stores,
		ContinuationToken: encodedToken,
	}

	return resp, nil
}
