package commands

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

type ListStoresQuery struct {
	storesBackend storage.StoresBackend
	logger        logger.Logger
}

type ListStoresQueryOption func(*ListStoresQuery)

func WithListStoresQueryLogger(l logger.Logger) ListStoresQueryOption {
	return func(q *ListStoresQuery) {
		q.logger = l
	}
}

func NewListStoresQuery(storesBackend storage.StoresBackend, opts ...ListStoresQueryOption) *ListStoresQuery {
	q := &ListStoresQuery{
		storesBackend: storesBackend,
		logger:        logger.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(q)
	}
	return q
}

func (q *ListStoresQuery) Execute(ctx context.Context, req *openfgav1.ListStoresRequest, storeIDs []string) (*openfgav1.ListStoresResponse, error) {
	opts := storage.ListStoresOptions{
		IDs:        storeIDs,
		Name:       req.GetName(),
		Pagination: storage.NewPaginationOptions(req.GetPageSize().GetValue(), req.GetContinuationToken()),
	}
	stores, contToken, err := q.storesBackend.ListStores(ctx, opts)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	resp := &openfgav1.ListStoresResponse{
		Stores:            stores,
		ContinuationToken: contToken,
	}

	return resp, nil
}
