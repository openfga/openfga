package caching

import (
	"context"
	"strings"
	"time"

	"github.com/karlseguin/ccache/v2"
	"github.com/openfga/openfga/pkg/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const (
	Separator = ":"
	TTL       = time.Hour * 168
)

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

func (c *cachedOpenFGADatastore) ReadTypeDefinition(ctx context.Context, store, modelID, name string) (*openfgapb.TypeDefinition, error) {
	cacheKey := strings.Join([]string{store, modelID, name}, Separator)
	cachedEntry := c.cache.Get(cacheKey)

	if cachedEntry != nil {
		return cachedEntry.Value().(*openfgapb.TypeDefinition), nil
	}

	ns, err := c.OpenFGADatastore.ReadTypeDefinition(ctx, store, modelID, name)
	if err != nil {
		return nil, errors.ErrorWithStack(err)
	}

	c.cache.Set(cacheKey, ns, TTL) // these are immutable, once created, there cannot be edits, therefore they can be cached without TTL
	return ns, nil
}

func NewChangelogMetadataCache(maxEntries int64) *ccache.Cache {
	return ccache.New(ccache.Configure().MaxSize(maxEntries))
}
