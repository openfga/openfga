package commands

import (
	"context"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

type ReadAssertionsQuery struct {
	backend storage.AssertionsBackend
	logger  logger.Logger
}

func NewReadAssertionsQuery(backend storage.AssertionsBackend, logger logger.Logger) *ReadAssertionsQuery {
	return &ReadAssertionsQuery{
		backend: backend,
		logger:  logger,
	}
}

func (q *ReadAssertionsQuery) Execute(ctx context.Context, store, authorizationModelID string) (*openfgav1.ReadAssertionsResponse, error) {
	assertions, err := q.backend.ReadAssertions(ctx, store, authorizationModelID)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}
	return &openfgav1.ReadAssertionsResponse{
		AuthorizationModelId: authorizationModelID,
		Assertions:           assertions,
	}, nil
}
