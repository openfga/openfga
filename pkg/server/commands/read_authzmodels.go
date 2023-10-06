package commands

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

type ReadAuthorizationModelsQuery struct {
	backend storage.AuthorizationModelReadBackend
	logger  logger.Logger
	encoder encoder.Encoder
}

func NewReadAuthorizationModelsQuery(backend storage.AuthorizationModelReadBackend, logger logger.Logger, encoder encoder.Encoder) *ReadAuthorizationModelsQuery {
	return &ReadAuthorizationModelsQuery{
		backend: backend,
		logger:  logger,
		encoder: encoder,
	}
}

func (q *ReadAuthorizationModelsQuery) Execute(ctx context.Context, req *openfgav1.ReadAuthorizationModelsRequest) (*openfgav1.ReadAuthorizationModelsResponse, error) {
	decodedContToken, err := q.encoder.Decode(req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}

	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))

	models, contToken, err := q.backend.ReadAuthorizationModels(ctx, req.GetStoreId(), paginationOptions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	encodedContToken, err := q.encoder.Encode(contToken)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	resp := &openfgav1.ReadAuthorizationModelsResponse{
		AuthorizationModels: models,
		ContinuationToken:   encodedContToken,
	}
	return resp, nil
}
