package queries

import (
	"context"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// ReadAuthorizationModelQuery retrieves a single type definition from a storage backend.
type ReadAuthorizationModelQuery struct {
	backend storage.AuthorizationModelReadBackend
	logger  logger.Logger
}

func NewReadAuthorizationModelQuery(backend storage.AuthorizationModelReadBackend, logger logger.Logger) *ReadAuthorizationModelQuery {
	return &ReadAuthorizationModelQuery{backend: backend, logger: logger}
}

func (query *ReadAuthorizationModelQuery) Execute(ctx context.Context, req *openfgav1pb.ReadAuthorizationModelRequest) (*openfgav1pb.ReadAuthorizationModelResponse, error) {
	utils.LogDBStats(ctx, query.logger, "ReadAuthzModel", 1, 0)
	modelID := req.GetId()
	azm, err := query.backend.ReadAuthorizationModel(ctx, req.GetStoreId(), modelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}
		return nil, serverErrors.HandleError("", err)
	}
	return &openfgav1pb.ReadAuthorizationModelResponse{
		AuthorizationModel: azm,
	}, nil
}
