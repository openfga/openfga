package commands

import (
	"context"
	"errors"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// ReadAuthorizationModelQuery retrieves a single type definition from a storage backend.
type ReadAuthorizationModelQuery struct {
	backend storage.AuthorizationModelReadBackend
	logger  logger.Logger
}

func NewReadAuthorizationModelQuery(backend storage.AuthorizationModelReadBackend, logger logger.Logger) *ReadAuthorizationModelQuery {
	return &ReadAuthorizationModelQuery{backend: backend, logger: logger}
}

func (query *ReadAuthorizationModelQuery) Execute(ctx context.Context, req *openfgapb.ReadAuthorizationModelRequest) (*openfgapb.ReadAuthorizationModelResponse, error) {
	modelID := req.GetId()
	azm, err := query.backend.ReadAuthorizationModel(ctx, req.GetStoreId(), modelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}
		return nil, serverErrors.HandleError("", err)
	}
	return &openfgapb.ReadAuthorizationModelResponse{
		AuthorizationModel: azm,
	}, nil
}
