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

type typeDefinitionContextCachingBackend struct {
	innerBackend storage.TypeDefinitionReadBackend
	cache        *ccache.Cache
}

func NewTypeDefinitionCachingBackend(innerBackend storage.TypeDefinitionReadBackend, maxEntries int64) *typeDefinitionContextCachingBackend {
	return &typeDefinitionContextCachingBackend{
		innerBackend: innerBackend,
		cache:        ccache.New(ccache.Configure().MaxSize(maxEntries)),
	}
}

func (cachingBackend *typeDefinitionContextCachingBackend) ReadTypeDefinition(ctx context.Context, store, modelID, name string) (*openfgapb.TypeDefinition, error) {
	cacheKey := strings.Join([]string{store, modelID, name}, Separator)
	cachedEntry := cachingBackend.cache.Get(cacheKey)

	if cachedEntry != nil {
		return cachedEntry.Value().(*openfgapb.TypeDefinition), nil
	}

	ns, err := cachingBackend.innerBackend.ReadTypeDefinition(ctx, store, modelID, name)
	if err != nil {
		return nil, errors.ErrorWithStack(err)
	}

	cachingBackend.cache.Set(cacheKey, ns, TTL) // these are immutable, once created, there cannot be edits, therefore they can be cached without TTL
	return ns, nil
}

func NewChangelogMetadataCache(maxEntries int64) *ccache.Cache {
	return ccache.New(ccache.Configure().MaxSize(maxEntries))
}
