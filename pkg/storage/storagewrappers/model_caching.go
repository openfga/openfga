package storagewrappers

import (
	"context"
	"time"

	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

const ttl = time.Hour * 168

var (
	_ storage.OpenFGADatastore = (*cachedOpenFGADatastore)(nil)
	_ storage.CacheItem        = (*cachedAuthorizationModel)(nil)
)

type cachedAuthorizationModel struct {
	*openfgav1.AuthorizationModel
}

func (c *cachedAuthorizationModel) CacheEntityType() string {
	return "authz_model"
}

type cachedOpenFGADatastore struct {
	storage.OpenFGADatastore
	lookupGroup singleflight.Group
	cache       storage.InMemoryCache[*cachedAuthorizationModel]
}

// NewCachedOpenFGADatastore returns a wrapper over a datastore that caches up to maxSize
// [*openfgav1.AuthorizationModel] on every call to storage.ReadAuthorizationModel.
// It caches with unlimited TTL because models are immutable. It uses LRU for eviction.
func NewCachedOpenFGADatastore(inner storage.OpenFGADatastore, maxSize int) (*cachedOpenFGADatastore, error) {
	cache, err := storage.NewInMemoryLRUCache[*cachedAuthorizationModel](storage.WithMaxCacheSize[*cachedAuthorizationModel](int64(maxSize)))
	if err != nil {
		return nil, err
	}
	return &cachedOpenFGADatastore{
		OpenFGADatastore: inner,
		cache:            *cache,
	}, nil
}

// ModelCacheKeyPrefix is the literal prefix every cached authorization model
// entry written by cachedOpenFGADatastore uses.
const ModelCacheKeyPrefix = "MODEL"

// ModelCacheKey returns the canonical cache key the model-caching datastore
// writes/reads for (storeID, modelID). Exported so callers and tests can
// reference the same key the datastore writes without duplicating its
// construction.
func ModelCacheKey(storeID, modelID string) keys.Key {
	b := keys.GetBuilder()
	defer b.Close()

	b.EncodeString(ModelCacheKeyPrefix)
	b.EncodeString(storeID)
	b.EncodeString(modelID)
	return b.Key()
}

// ReadAuthorizationModel reads the model corresponding to store and model ID.
func (c *cachedOpenFGADatastore) ReadAuthorizationModel(ctx context.Context, storeID, modelID string) (*openfgav1.AuthorizationModel, error) {
	cacheKey := ModelCacheKey(storeID, modelID)
	cachedEntry := c.cache.Get(cacheKey)

	if cachedEntry != nil {
		return cachedEntry.AuthorizationModel, nil
	}

	model, err := c.OpenFGADatastore.ReadAuthorizationModel(ctx, storeID, modelID)
	if err != nil {
		return nil, err
	}

	c.cache.Set(cacheKey, &cachedAuthorizationModel{model}, ttl) // These are immutable, once created, there cannot be edits, therefore they can be cached without ttl.

	return model, nil
}

// FindLatestAuthorizationModel see [storage.AuthorizationModelReadBackend].FindLatestAuthorizationModel.
func (c *cachedOpenFGADatastore) FindLatestAuthorizationModel(ctx context.Context, storeID string) (*openfgav1.AuthorizationModel, error) {
	v, err, _ := c.lookupGroup.Do("FindLatestAuthorizationModel:"+storeID, func() (interface{}, error) {
		return c.OpenFGADatastore.FindLatestAuthorizationModel(ctx, storeID)
	})
	if err != nil {
		return nil, err
	}
	return v.(*openfgav1.AuthorizationModel), nil
}

// Close closes the datastore and cleans up any residual resources.
func (c *cachedOpenFGADatastore) Close() {
	c.cache.Stop()
	c.OpenFGADatastore.Close()
}
