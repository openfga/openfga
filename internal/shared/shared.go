package shared

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/internal/cachecontroller"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/storage"
)

type SharedResources struct {
	SingleflightGroup *singleflight.Group
	WaitGroup         *sync.WaitGroup
	ServerCtx         context.Context
	CheckCache        storage.InMemoryCache[any]
	CacheController   cachecontroller.CacheController
}

func NewSharedResources(sharedCtx context.Context, sharedSf *singleflight.Group, ds storage.OpenFGADatastore, settings serverconfig.CacheSettings) (*SharedResources, error) {
	s := &SharedResources{
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

func (s *SharedResources) Close() {
	if s.CheckCache != nil {
		s.CheckCache.Stop()
	}
	s.WaitGroup.Wait()
}
