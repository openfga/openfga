package commands

import (
	"context"
	"errors"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/oklog/ulid/v2"

	"github.com/openfga/openfga/pkg/typesystem"

	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
)

type ReadAuthorizationModelOrLatestQuery struct {
	backend storage.AuthorizationModelReadBackend
}

// NewReadAuthorizationModelOrLatestQuery is a reusable command meant for other commands
// where model ID parameter is optional.
func NewReadAuthorizationModelOrLatestQuery(backend storage.AuthorizationModelReadBackend) *ReadAuthorizationModelOrLatestQuery {
	return &ReadAuthorizationModelOrLatestQuery{
		backend: backend,
	}
}

// Execute validates a given model ID, and tries to read it.
// If model ID is empty, it tries to fetch the latest model from that store.
// It sets the resolved model as a context tag.
func (q *ReadAuthorizationModelOrLatestQuery) Execute(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
	if modelID != "" {
		if _, err := ulid.Parse(modelID); err != nil {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}
	}

	latestModelID := modelID
	var err error
	if modelID == "" {
		latestModelID, err = q.backend.FindLatestAuthorizationModelID(ctx, storeID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, serverErrors.LatestAuthorizationModelNotFound(storeID)
			}
			return nil, serverErrors.HandleError("", err)
		}
	}

	typesys, err := q.backend.ReadAuthorizationModel(ctx, storeID, latestModelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}
		return nil, serverErrors.HandleError("", err)
	}

	grpc_ctxtags.Extract(ctx).Set("authorization_model_id", latestModelID)
	return typesys, nil
}
