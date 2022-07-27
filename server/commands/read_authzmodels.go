package commands

import (
	"context"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
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

func (q *ReadAuthorizationModelsQuery) Execute(ctx context.Context, req *openfgapb.ReadAuthorizationModelsRequest) (*openfgapb.ReadAuthorizationModelsResponse, error) {
	decodedContToken, err := q.encoder.Decode(req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}

	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))
	utils.LogDBStats(ctx, q.logger, "ReadAuthzModels", 1, 0)

	models, contToken, err := q.backend.ReadAuthorizationModels(ctx, req.GetStoreId(), paginationOptions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	encodedContToken, err := q.encoder.Encode(contToken)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	resp := &openfgapb.ReadAuthorizationModelsResponse{
		AuthorizationModels: models,
		ContinuationToken:   encodedContToken,
	}
	return resp, nil
}
