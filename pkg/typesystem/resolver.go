package typesystem

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/karlseguin/ccache/v3"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"golang.org/x/sync/singleflight"
)

const (
	typesystemCacheTTL = 168 * time.Hour // 7 days
)

// TypesystemResolverFunc is a function that implementations can implement to provide lookup and
// resolution of a Typesystem.
type TypesystemResolverFunc func(ctx context.Context, storeID, modelID string, validate bool) (*TypeSystem, error)

type TypesystemResolver interface {
	ResolveTypesystem(ctx context.Context, storeID, modelID string, validate bool) (*TypeSystem, error)
}

type cachedTypesystemResolver struct {
	modelReader  storage.AuthorizationModelReadBackend
	cache        *ccache.Cache[*TypeSystem]
	singleflight singleflight.Group
}

type TypesystemResolverOption func(c *cachedTypesystemResolver)

func WithTypesystemResolverCache(cache *ccache.Cache[*TypeSystem]) TypesystemResolverOption {
	return func(c *cachedTypesystemResolver) {
		c.cache = cache
	}
}

func NewCachedTypesystemResolver(
	modelReader storage.AuthorizationModelReadBackend,
	opts ...TypesystemResolverOption,
) *cachedTypesystemResolver {
	resolver := &cachedTypesystemResolver{
		modelReader: modelReader,
	}

	for _, opt := range opts {
		opt(resolver)
	}

	if resolver.cache == nil {
		resolver.cache = ccache.New(ccache.Configure[*TypeSystem]())
	}

	return resolver
}

// ResolveTypesystem resolves
func (c *cachedTypesystemResolver) ResolveTypesystem(
	ctx context.Context,
	storeID string,
	modelID string,
	validate bool,
) (*TypeSystem, error) {
	ctx, span := tracer.Start(ctx, "ResolveTypesystem")
	defer span.End()

	var err error

	if modelID != "" {
		if _, err := ulid.Parse(modelID); err != nil {
			return nil, ErrModelNotFound
		}
	}

	if modelID == "" {
		v, err, _ := c.singleflight.Do(fmt.Sprintf("FindLatestAuthorizationModelID:%s", storeID), func() (interface{}, error) {
			return c.modelReader.FindLatestAuthorizationModelID(ctx, storeID)
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

	item := c.cache.Get(key)
	if item != nil {
		return item.Value(), nil
	}

	v, err, _ := c.singleflight.Do(fmt.Sprintf("ReadAuthorizationModel:%s/%s", storeID, modelID), func() (interface{}, error) {
		return c.modelReader.ReadAuthorizationModel(ctx, storeID, modelID)
	})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrModelNotFound
		}

		return nil, fmt.Errorf("failed to ReadAuthorizationModel: %w", err)
	}

	model := v.(*openfgav1.AuthorizationModel)

	var typesys *TypeSystem
	if validate {
		typesys, err = NewAndValidate(ctx, model)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidModel, err)
		}
	} else {
		typesys = New(model)
	}

	c.cache.Set(key, typesys, typesystemCacheTTL)

	return typesys, nil
}
