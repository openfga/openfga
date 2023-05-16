package typesystem

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/karlseguin/ccache/v3"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage"
)

const (
	typesystemCacheTTL = 168 * time.Hour // 7 days
)

// TypesystemResolverFunc is a function that implementations can implement to provide lookup and
// resolution of a Typesystem.
type TypesystemResolverFunc func(ctx context.Context, storeID, modelID string) (*TypeSystem, error)

// MemoizedTypesystemResolver returns a TypesystemResolverFunc that either fetches the provided authorization
// model (if provided) or looks up the latest authorizaiton model, and then it constructs a TypeSystem from
// the resolved model. The type-system resolution is memoized so if another lookup of the same model occurs,
// then the earlier TypeSystem that was constructed will be used.
//
// The memoized resolver function is safe for concurrent use.
func MemoizedTypesystemResolverFunc(reader storage.AuthorizationModelReadBackend) TypesystemResolverFunc {

	cache := ccache.New(ccache.Configure[*TypeSystem]())

	return func(ctx context.Context, storeID, modelID string) (*TypeSystem, error) {
		tracer.Start(ctx, "MemoizedTypesystemResolverFunc")

		var err error

		if modelID != "" {
			if _, err := ulid.Parse(modelID); err != nil {
				return nil, ErrModelNotFound
			}
		}

		if modelID == "" {
			if modelID, err = reader.FindLatestAuthorizationModelID(ctx, storeID); err != nil {
				if !errors.Is(err, storage.ErrNotFound) {
					return nil, ErrModelNotFound
				}

				return nil, fmt.Errorf("failed to FindLatestAuthorizationModelID: %w", err)
			}
		}

		key := fmt.Sprintf("%s/%s", storeID, modelID)

		item := cache.Get(key)
		if item != nil {
			return item.Value(), nil
		}

		model, err := reader.ReadAuthorizationModel(ctx, storeID, modelID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, ErrModelNotFound
			}

			return nil, fmt.Errorf("failed to ReadAuthorizationModel: %w", err)
		}

		typesys := New(model)
		cache.Set(key, typesys, typesystemCacheTTL)

		return typesys, nil
	}
}
