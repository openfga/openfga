package storagewrappers

import (
	"context"
	"fmt"
	"time"

	"github.com/karlseguin/ccache/v3"
	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

const ttl = time.Hour * 168

var _ storage.OpenFGADatastore = (*cachedOpenFGADatastore)(nil)

type cachedOpenFGADatastore struct {
	storage.OpenFGADatastore
	lookupGroup singleflight.Group
	cache       *ccache.Cache[*typesystem.TypeSystem]
}

// NewCachedOpenFGADatastore returns a wrapper over a datastore that caches up to maxSize
// [*openfgav1.AuthorizationModel] on every call to storage.ReadAuthorizationModel.
// It caches with unlimited TTL because models are immutable. It uses LRU for eviction.
func NewCachedOpenFGADatastore(inner storage.OpenFGADatastore, maxSize int) *cachedOpenFGADatastore {
	return &cachedOpenFGADatastore{
		OpenFGADatastore: inner,
		cache:            ccache.New(ccache.Configure[*typesystem.TypeSystem]().MaxSize(int64(maxSize))),
	}
}

func (c *cachedOpenFGADatastore) ReadAuthorizationModel(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
	cacheKey := fmt.Sprintf("%s:%s", storeID, modelID)
	cachedEntry := c.cache.Get(cacheKey)

	if cachedEntry != nil {
		return cachedEntry.Value(), nil
	}

	v, err, _ := c.lookupGroup.Do(fmt.Sprintf("ReadAuthorizationModel:%s/%s", storeID, modelID), func() (interface{}, error) {
		return c.OpenFGADatastore.ReadAuthorizationModel(ctx, storeID, modelID)
	})
	if err != nil {
		return nil, err
	}

	typesys := v.(*typesystem.TypeSystem)

	c.cache.Set(cacheKey, typesys, ttl) // these are immutable, once created, there cannot be edits, therefore they can be cached without ttl

	return typesys, nil
}

// FindLatestAuthorizationModelID returns the last model `id` written for a store.
func (c *cachedOpenFGADatastore) FindLatestAuthorizationModelID(ctx context.Context, storeID string) (string, error) {
	v, err, _ := c.lookupGroup.Do(fmt.Sprintf("FindLatestAuthorizationModelID:%s", storeID), func() (interface{}, error) {
		return c.OpenFGADatastore.FindLatestAuthorizationModelID(ctx, storeID)
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// Close closes the datastore and cleans up any residual resources.
func (c *cachedOpenFGADatastore) Close() {
	c.cache.Stop()
	c.OpenFGADatastore.Close()
}
