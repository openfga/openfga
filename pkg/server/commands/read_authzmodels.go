package commands

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

type ReadAuthorizationModelsQuery struct {
	backend storage.AuthorizationModelReadBackend
	logger  logger.Logger
}

type ReadAuthModelsQueryOption func(*ReadAuthorizationModelsQuery)

func WithReadAuthModelsQueryLogger(l logger.Logger) ReadAuthModelsQueryOption {
	return func(rm *ReadAuthorizationModelsQuery) {
		rm.logger = l
	}
}

func NewReadAuthorizationModelsQuery(backend storage.AuthorizationModelReadBackend, opts ...ReadAuthModelsQueryOption) *ReadAuthorizationModelsQuery {
	rm := &ReadAuthorizationModelsQuery{
		backend: backend,
		logger:  logger.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(rm)
	}
	return rm
}

func (q *ReadAuthorizationModelsQuery) Execute(ctx context.Context, req *openfgav1.ReadAuthorizationModelsRequest) (*openfgav1.ReadAuthorizationModelsResponse, error) {
	opts := storage.ReadAuthorizationModelsOptions{
		Pagination: storage.NewPaginationOptions(req.GetPageSize().GetValue(), req.GetContinuationToken()),
	}
	models, contToken, err := q.backend.ReadAuthorizationModels(ctx, req.GetStoreId(), opts)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	resp := &openfgav1.ReadAuthorizationModelsResponse{
		AuthorizationModels: models,
		ContinuationToken:   contToken,
	}
	return resp, nil
}
