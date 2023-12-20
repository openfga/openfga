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

const (
	typesystemCacheTTL = 168 * time.Hour // 7 days
)

// TypesystemResolverFunc is a function that implementations can implement to provide lookup and
// resolution of a Typesystem.
type TypesystemResolverFunc func(ctx context.Context, storeID, modelID string) (*TypeSystem, error)

// MemoizedTypesystemResolverFunc returns a TypesystemResolverFunc that either fetches the provided authorization
// model (if provided) or looks up the latest authorization model, and then it constructs a TypeSystem from
// the resolved model. The type-system resolution is memoized so if another lookup of the same model occurs,
// then the earlier TypeSystem that was constructed will be used.
//
// The memoized resolver function is safe for concurrent use.
func MemoizedTypesystemResolverFunc(datastore storage.AuthorizationModelReadBackend) TypesystemResolverFunc {
	lookupGroup := singleflight.Group{}

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

		if modelID == "" {
			v, err, _ := lookupGroup.Do(fmt.Sprintf("FindLatestAuthorizationModelID:%s", storeID), func() (interface{}, error) {
				return datastore.FindLatestAuthorizationModelID(ctx, storeID)
			})
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return nil, ErrModelNotFound
				}

				return nil, fmt.Errorf("failed to FindLatestAuthorizationModelID: %w", err)
			}

			modelID = v.(string)
		}

		key := fmt.Sprintf("%s/%s", storeID, modelID)

		item := cache.Get(key)
		if item != nil {
			return item.Value(), nil
		}

		v, err, _ := lookupGroup.Do(fmt.Sprintf("ReadAuthorizationModel:%s/%s", storeID, modelID), func() (interface{}, error) {
			return datastore.ReadAuthorizationModel(ctx, storeID, modelID)
		})
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, ErrModelNotFound
			}

			return nil, fmt.Errorf("failed to ReadAuthorizationModel: %w", err)
		}

		model := v.(*openfgav1.AuthorizationModel)

		typesys, err := NewAndValidate(ctx, model)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidModel, err)
		}

		cache.Set(key, typesys, typesystemCacheTTL)

		return typesys, nil
	}
}
