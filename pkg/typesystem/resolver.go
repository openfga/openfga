package typesystem

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

// TODO there is a duplicate cache of models elsewhere: https://github.com/openfga/openfga/issues/1045

const (
	typesystemCacheTTL = 168 * time.Hour // 7 days.
)

type TypesystemResolverFunc func(ctx context.Context, storeID, modelID string) (*TypeSystem, error)

// TypesystemResolverConfig configures a memoized typesystem resolver.
type TypesystemResolverConfig struct {
	tracer trace.Tracer
}

// TypesystemResolverOption configures a memoized typesystem resolver.
type TypesystemResolverOption func(*TypesystemResolverConfig)

// WithTracerProvider sets the OpenTelemetry tracer provider used by the resolver.
func WithTracerProvider(tracerProvider trace.TracerProvider) TypesystemResolverOption {
	return func(config *TypesystemResolverConfig) {
		if tracerProvider == nil {
			return
		}
		config.tracer = tracerProvider.Tracer("openfga/pkg/typesystem")
	}
}

// MemoizedTypesystemResolverFunc does several things.
//
// If given a model ID: validates the model ID, and tries to fetch it from the cache.
// If not found in the cache, fetches from the datastore, validates it, stores in cache, and returns it.
//
// If not given a model ID: fetches the latest model ID from the datastore, then sees if the model ID is in the cache.
// If it is, returns it. Else, validates it and returns it.
func MemoizedTypesystemResolverFunc(datastore storage.AuthorizationModelReadBackend, maxSize int) (TypesystemResolverFunc, func(), error) {
	return MemoizedTypesystemResolverFuncWithOpts(datastore, maxSize)
}

// MemoizedTypesystemResolverFuncWithOpts is like MemoizedTypesystemResolverFunc
// but accepts options that customize the resolver.
func MemoizedTypesystemResolverFuncWithOpts(datastore storage.AuthorizationModelReadBackend, maxSize int, opts ...TypesystemResolverOption) (TypesystemResolverFunc, func(), error) {
	lookupGroup := singleflight.Group{}
	config := &TypesystemResolverConfig{
		tracer: tracer,
	}
	for _, opt := range opts {
		opt(config)
	}

	// cache holds models that have already been validated.
	cache, err := storage.NewInMemoryLRUCache[*TypeSystem](
		storage.WithMaxCacheSize[*TypeSystem](int64(maxSize)),
	)
	if err != nil {
		return nil, nil, err
	}

	return func(ctx context.Context, storeID, modelID string) (*TypeSystem, error) {
		ctx, span := config.tracer.Start(ctx, "resolveTypesystem", trace.WithAttributes(
			attribute.String("store_id", storeID),
		))
		defer func() {
			span.SetAttributes(attribute.String("authorization_model_id", modelID))
			span.End()
		}()

		var err error

		if modelID != "" {
			if _, err := ulid.Parse(modelID); err != nil {
				return nil, ErrModelNotFound
			}
		}

		var model *openfgav1.AuthorizationModel

		if modelID == "" {
			v, err, _ := lookupGroup.Do("FindLatestAuthorizationModel:"+storeID, func() (interface{}, error) {
				return datastore.FindLatestAuthorizationModel(ctx, storeID)
			})
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return nil, ErrModelNotFound
				}

				return nil, fmt.Errorf("failed to FindLatestAuthorizationModel: %w", err)
			}

			model = v.(*openfgav1.AuthorizationModel)
			modelID = model.GetId()
		}

		kb := keys.GetBuilder()
		kb.EncodeString("TS")
		kb.EncodeString(storeID)
		kb.EncodeString(modelID)
		key := kb.Key()
		kb.Close()

		item := cache.Get(key)
		if item != nil {
			return item, nil
		}

		if model == nil {
			v, err, _ := lookupGroup.Do(fmt.Sprintf("ReadAuthorizationModel:%s/%s", storeID, modelID), func() (interface{}, error) {
				return datastore.ReadAuthorizationModel(ctx, storeID, modelID)
			})
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return nil, ErrModelNotFound
				}

				return nil, fmt.Errorf("failed to ReadAuthorizationModel: %w", err)
			}

			model = v.(*openfgav1.AuthorizationModel)
		}

		typesys, err := newAndValidate(ctx, model, config.tracer)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidModel, err)
		}

		cache.Set(key, typesys, typesystemCacheTTL)

		return typesys, nil
	}, cache.Stop, nil
}
