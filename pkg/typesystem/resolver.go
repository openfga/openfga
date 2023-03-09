package typesystem

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	modelSchemaVersionCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "model_schema_version_counter",
			Help: "A counter counting the different authorization model schema versions that have been resolved",
		},
		[]string{"schema_version"},
	)
)

// TypesystemResolverFunc is a function that implementations can implement to provide lookup and
// resolution of a Typesystem.
type TypesystemResolverFunc func(ctx context.Context, storeID, modelID string) (*TypeSystem, error)

// MemoizedTypesystemResolverFunc memoizes the provided TypesystemResolverFunc. It returns
// the value from the first invocation of the underlying resolver function.
//
// The memoized resolver function is safe for concurrent use.
func MemoizedTypesystemResolverFunc(fn TypesystemResolverFunc) TypesystemResolverFunc {
	var cache sync.Map

	return func(ctx context.Context, storeID, modelID string) (*TypeSystem, error) {
		tracer.Start(ctx, "MemoizedTypesystemResolverFunc")

		// if modelID is empty always fetch the latest model and construct the typesystem from it
		if modelID == "" {
			return fn(ctx, storeID, modelID)
		}

		key := fmt.Sprintf("%s/%s", storeID, modelID)

		if val, found := cache.Load(key); found {
			return val.(*TypeSystem), nil
		}

		typesys, err := fn(ctx, storeID, modelID)
		if err != nil {
			return nil, err
		}

		cache.Store(key, typesys)
		return typesys, nil
	}
}

// NewTypesystemResolver returns a TypesystemResolverFunc that either fetches the provided authorization
// model (if provided) or looks up the latest authorizaiton model, and then it constructs a TypeSystem from
// the resolved model.
func NewTypesystemResolver(reader storage.AuthorizationModelReadBackend) TypesystemResolverFunc {
	return func(ctx context.Context, storeID, modelID string) (*TypeSystem, error) {
		ctx, span := tracer.Start(ctx, "ResolveTypesystem")
		defer span.End()

		var err error

		if modelID != "" {
			if _, err := ulid.Parse(modelID); err != nil {
				return nil, ErrModelNotFound
			}
		}

		if modelID == "" {
			if modelID, err = reader.FindLatestAuthorizationModelID(ctx, storeID); err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return nil, ErrNoModelsDefined
				}

				return nil, fmt.Errorf("failed to FindLatestAuthorizationModelID: %w", err)
			}
		}

		model, err := reader.ReadAuthorizationModel(ctx, storeID, modelID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, ErrModelNotFound
			}

			return nil, fmt.Errorf("failed to ReadAuthorizationModel: %w", err)
		}

		modelSchemaVersionCounter.WithLabelValues(model.GetSchemaVersion()).Add(1)

		return New(model), nil
	}
}
