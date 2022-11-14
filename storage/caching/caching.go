package caching

import (
	"context"
	"fmt"
	"time"

	"github.com/karlseguin/ccache/v2"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const ttl = time.Hour * 168

var _ storage.OpenFGADatastore = (*cachedOpenFGADatastore)(nil)

type cachedOpenFGADatastore struct {
	storage.OpenFGADatastore
	cache *ccache.Cache
}

func NewCachedOpenFGADatastore(inner storage.OpenFGADatastore, maxSize int) *cachedOpenFGADatastore {
	return &cachedOpenFGADatastore{
		OpenFGADatastore: inner,
		cache:            ccache.New(ccache.Configure().MaxSize(int64(maxSize))),
	}
}

func (c *cachedOpenFGADatastore) ReadTypeDefinition(ctx context.Context, storeID, modelID, name string) (*openfgapb.TypeDefinition, error) {
	cacheKey := fmt.Sprintf("%s:%s:%s", storeID, modelID, name)
	cachedEntry := c.cache.Get(cacheKey)

	if cachedEntry != nil {
		return cachedEntry.Value().(*openfgapb.TypeDefinition), nil
	}

	td, err := c.OpenFGADatastore.ReadTypeDefinition(ctx, storeID, modelID, name)
	if err != nil {
		return nil, err
	}

	c.cache.Set(cacheKey, td, ttl) // these are immutable, once created, there cannot be edits, therefore they can be cached without ttl

	return td, nil
}

func (c *cachedOpenFGADatastore) ReadAuthorizationModel(ctx context.Context, storeID, modelID string) (*openfgapb.AuthorizationModel, error) {
	cacheKey := fmt.Sprintf("%s:%s", storeID, modelID)
	cachedEntry := c.cache.Get(cacheKey)

	if cachedEntry != nil {
		return cachedEntry.Value().(*openfgapb.AuthorizationModel), nil
	}

	model, err := c.OpenFGADatastore.ReadAuthorizationModel(ctx, storeID, modelID)
	if err != nil {
		return nil, err
	}

	c.cache.Set(cacheKey, model, ttl) // these are immutable, once created, there cannot be edits, therefore they can be cached without ttl

	return model, nil
}
