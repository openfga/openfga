package shared

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
)

// SharedDatastoreResourcesOpt defines an option that can be used to change the behavior of SharedDatastoreResources
// instance.
type SharedDatastoreResourcesOpt func(*SharedDatastoreResources)

// WithLogger sets the logger for CachedDatastore.
func WithLogger(logger logger.Logger) SharedDatastoreResourcesOpt {
	return func(scr *SharedDatastoreResources) {
		scr.Logger = logger
	}
}

// SharedDatastoreResources contains resources that can be shared across Check requests.
type SharedDatastoreResources struct {
	SingleflightGroup *singleflight.Group
	WaitGroup         *sync.WaitGroup
	ServerCtx         context.Context
	CheckCache        storage.InMemoryCache[any]
	CacheController   cachecontroller.CacheController
	Logger            logger.Logger
}

func NewSharedDatastoreResources(
	sharedCtx context.Context,
	sharedSf *singleflight.Group,
	ds storage.OpenFGADatastore,
	settings serverconfig.CacheSettings,
	opts ...SharedDatastoreResourcesOpt,
) (*SharedDatastoreResources, error) {
	s := &SharedDatastoreResources{
		WaitGroup:         &sync.WaitGroup{},
		SingleflightGroup: sharedSf,
		ServerCtx:         sharedCtx,
		CacheController:   cachecontroller.NewNoopCacheController(),
		Logger:            logger.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(s)
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
		s.CacheController = cachecontroller.NewCacheController(ds, s.CheckCache, settings.CacheControllerTTL, settings.CheckIteratorCacheTTL, cachecontroller.WithLogger(s.Logger))
	}

	return s, nil
}

func (s *SharedDatastoreResources) Close() {
	// wait for any goroutines still in flight before
	// closing the cache instance to avoid data races
	s.WaitGroup.Wait()
	if s.CheckCache != nil {
		s.CheckCache.Stop()
	}
}
