package commands

import (
	"context"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ListStoresQuery struct {
	storesBackend storage.StoresBackend
	logger        logger.Logger
	encrypter     encrypter.Encrypter
	encoder       encoder.Encoder
}

func NewListStoresQuery(storesBackend storage.StoresBackend, logger logger.Logger, encrypter encrypter.Encrypter, encoder encoder.Encoder) *ListStoresQuery {
	return &ListStoresQuery{
		storesBackend: storesBackend,
		logger:        logger,
		encrypter:     encrypter,
		encoder:       encoder,
	}
}

func (q *ListStoresQuery) Execute(ctx context.Context, req *openfgapb.ListStoresRequest) (*openfgapb.ListStoresResponse, error) {
	decodedContToken, err := utils.DecodeAndDecrypt(q.encrypter, q.encoder, req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}

	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))
	utils.LogDBStats(ctx, q.logger, "ListStores", 1, 0)

	stores, continuationToken, err := q.storesBackend.ListStores(ctx, paginationOptions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	encodedToken, err := utils.EncryptAndEncode(q.encrypter, q.encoder, continuationToken)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	resp := &openfgapb.ListStoresResponse{
		Stores:            stores,
		ContinuationToken: encodedToken,
	}

	return resp, nil
}
