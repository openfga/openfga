package queries

import (
	"context"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ReadAuthorizationModelsQuery struct {
	backend storage.AuthorizationModelReadBackend
	encoder encoder.Encoder
	logger  logger.Logger
}

func NewReadAuthorizationModelsQuery(backend storage.AuthorizationModelReadBackend, encoder encoder.Encoder, logger logger.Logger) *ReadAuthorizationModelsQuery {
	return &ReadAuthorizationModelsQuery{
		backend: backend,
		encoder: encoder,
		logger:  logger,
	}
}

func (q *ReadAuthorizationModelsQuery) Execute(ctx context.Context, req *openfgav1pb.ReadAuthorizationModelsRequest) (*openfgav1pb.ReadAuthorizationModelsResponse, error) {
	decodedContToken, err := q.encoder.Decode(req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}
	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))
	utils.LogDBStats(ctx, q.logger, "ReadAuthzModels", 1, 0)

	authorizationModelIDs, contToken, err := q.backend.ReadAuthorizationModels(ctx, req.GetStoreId(), paginationOptions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	encodedContToken, err := q.encoder.Encode(contToken)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	resp := &openfgav1pb.ReadAuthorizationModelsResponse{
		AuthorizationModelIds: authorizationModelIDs,
		ContinuationToken:     encodedContToken,
	}
	return resp, nil
}
