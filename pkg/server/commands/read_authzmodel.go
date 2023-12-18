package commands

import (
	"context"
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/typesystem"
)

// ReadAuthorizationModelQuery retrieves a single type definition from a storage backend.
type ReadAuthorizationModelQuery struct {
	logger             logger.Logger
	typesystemResolver typesystem.TypesystemResolver
}

type ReadAuthModelQueryOption func(*ReadAuthorizationModelQuery)

func WithReadAuthModelQueryLogger(l logger.Logger) ReadAuthModelQueryOption {
	return func(m *ReadAuthorizationModelQuery) {
		m.logger = l
	}
}

func NewReadAuthorizationModelQuery(
	typesystemResolver typesystem.TypesystemResolver,
	opts ...ReadAuthModelQueryOption,
) *ReadAuthorizationModelQuery {
	m := &ReadAuthorizationModelQuery{
		logger:             logger.NewNoopLogger(),
		typesystemResolver: typesystemResolver,
	}

	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (q *ReadAuthorizationModelQuery) Execute(
	ctx context.Context,
	req *openfgav1.ReadAuthorizationModelRequest,
) (*openfgav1.ReadAuthorizationModelResponse, error) {
	modelID := req.GetId()

	typesys, err := q.typesystemResolver.ResolveTypesystem(ctx, req.GetStoreId(), req.GetId(), false)
	if err != nil {
		if errors.Is(err, typesystem.ErrModelNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}

		return nil, serverErrors.HandleError("", err)
	}

	return &openfgav1.ReadAuthorizationModelResponse{
		AuthorizationModel: typesys.GetAuthorizationModel(),
	}, nil
}
