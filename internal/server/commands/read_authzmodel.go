package commands

import (
	"context"
	"errors"

	serverErrors "github.com/openfga/openfga/internal/server/errors"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
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

func (q *ReadAuthorizationModelQuery) Execute(ctx context.Context, req *openfgapb.ReadAuthorizationModelRequest) (*openfgapb.ReadAuthorizationModelResponse, error) {
	modelID := req.GetId()
	azm, err := q.backend.ReadAuthorizationModel(ctx, req.GetStoreId(), modelID)
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
