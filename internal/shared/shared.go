package shared

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/internal/cachecontroller"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/storage"
)

// SharedCheckResources contains resources that can be shared across Check requests.
type SharedCheckResources struct {
	SingleflightGroup *singleflight.Group
	WaitGroup         *sync.WaitGroup
	ServerCtx         context.Context
	CheckCache        storage.InMemoryCache[any]
	CacheController   cachecontroller.CacheController
}

func NewSharedCheckResources(sharedCtx context.Context, sharedSf *singleflight.Group, ds storage.OpenFGADatastore, settings serverconfig.CacheSettings) (*SharedCheckResources, error) {
	s := &SharedCheckResources{
		WaitGroup:         &sync.WaitGroup{},
		SingleflightGroup: sharedSf,
		ServerCtx:         sharedCtx,
		CacheController:   cachecontroller.NewNoopCacheController(),
	}

	if settings.ShouldCreateNewCache() {
		var err error
		s.CheckCache, err = storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(settings.CheckCacheLimit)),
		}...)
		if err != nil {
			return nil, err
		}
	}

	if settings.ShouldCreateCacheController() {
		s.CacheController = cachecontroller.NewCacheController(ds, s.CheckCache, settings.CacheControllerTTL, settings.CheckIteratorCacheTTL)
	}

	return s, nil
}

func (s *SharedCheckResources) Close() {
	// wait for any goroutines still in flight before
	// closing the cache instance to avoid data races
	s.WaitGroup.Wait()
	if s.CheckCache != nil {
		s.CheckCache.Stop()
	}
}
