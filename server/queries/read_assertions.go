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

func (query *ReadAssertionsQuery) Execute(ctx context.Context, store, authorizationModelID string) (*openfgav1pb.ReadAssertionsResponse, error) {
	utils.LogDBStats(ctx, query.logger, "ReadAssertions", 1, 0)

	assertions, err := query.backend.ReadAssertions(ctx, store, authorizationModelID)
	if err != nil {
		if errors.Is(err, storage.NotFound) {
			return nil, serverErrors.AssertionsNotForAuthorizationModelFound(authorizationModelID)
		}
		return nil, serverErrors.HandleError("", err)
	}
	return &openfgav1pb.ReadAssertionsResponse{
		AuthorizationModelId: authorizationModelID,
		Assertions:           assertions,
	}, nil
}
