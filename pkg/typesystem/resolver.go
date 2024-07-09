package typesystem

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/karlseguin/ccache/v3"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/pkg/storage"
)

// TODO there is a duplicate cache of models elsewhere: https://github.com/openfga/openfga/issues/1045

const (
	typesystemCacheTTL = 168 * time.Hour // 7 days.
)

type TypesystemResolverFunc func(ctx context.Context, storeID, modelID string) (*TypeSystem, error)

// MemoizedTypesystemResolverFunc does several things.
//
// If given a model ID: validates the model ID, and tries to fetch it from the cache.
// If not found in the cache, fetches from the datastore, validates it, stores in cache, and returns it.
//
// If not given a model ID: fetches the latest model ID from the datastore, then sees if the model ID is in the cache.
// If it is, returns it. Else, validates it and returns it.
func MemoizedTypesystemResolverFunc(datastore storage.AuthorizationModelReadBackend) (TypesystemResolverFunc, func()) {
	lookupGroup := singleflight.Group{}

	// cache holds models that have already been validated.
	cache := ccache.New(ccache.Configure[*TypeSystem]())

	return func(ctx context.Context, storeID, modelID string) (*TypeSystem, error) {
		ctx, span := tracer.Start(ctx, "MemoizedTypesystemResolverFunc")
		defer span.End()

		var err error

		if modelID != "" {
			if _, err := ulid.Parse(modelID); err != nil {
				return nil, ErrModelNotFound
			}
		}

		var v interface{}
		var key string
		if modelID == "" {
			v, err, _ = lookupGroup.Do(fmt.Sprintf("FindLatestAuthorizationModel:%s", storeID), func() (interface{}, error) {
				return datastore.FindLatestAuthorizationModel(ctx, storeID)
			})
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return nil, ErrModelNotFound
				}

				return nil, fmt.Errorf("failed to FindLatestAuthorizationModel: %w", err)
			}

			model := v.(*openfgav1.AuthorizationModel)
			key = fmt.Sprintf("%s/%s", storeID, model.GetId())
			item := cache.Get(key)
			if item != nil {
				// This model has been validated before
				return item.Value(), nil
			}
		} else {
			key = fmt.Sprintf("%s/%s", storeID, modelID)
			item := cache.Get(key)
			if item != nil {
				return item.Value(), nil
			}

			v, err, _ = lookupGroup.Do(fmt.Sprintf("ReadAuthorizationModel:%s/%s", storeID, modelID), func() (interface{}, error) {
				return datastore.ReadAuthorizationModel(ctx, storeID, modelID)
			})
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return nil, ErrModelNotFound
				}

				return nil, fmt.Errorf("failed to ReadAuthorizationModel: %w", err)
			}
		}

		model := v.(*openfgav1.AuthorizationModel)

		typesys, err := NewAndValidate(ctx, model)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidModel, err)
		}

		cache.Set(key, typesys, typesystemCacheTTL)

		return typesys, nil
	}, cache.Stop
}
