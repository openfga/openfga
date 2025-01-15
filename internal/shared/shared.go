package shared

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/internal/cacheinvalidator"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
)

// SharedCheckResourcesOpt defines an option that can be used to change the behavior of SharedCheckResources
// instance.
type SharedCheckResourcesOpt func(*SharedCheckResources)

// WithLogger sets the logger for CachedDatastore.
func WithLogger(logger logger.Logger) SharedCheckResourcesOpt {
	return func(scr *SharedCheckResources) {
		scr.Logger = logger
	}
}

// SharedCheckResources contains resources that can be shared across Check requests.
type SharedCheckResources struct {
	SingleflightGroup *singleflight.Group
	WaitGroup         *sync.WaitGroup
	ServerCtx         context.Context
	CheckCache        storage.InMemoryCache[any]
	CacheInvalidator  cacheinvalidator.CacheInvalidator
	Logger            logger.Logger
}

func NewSharedCheckResources(sharedCtx context.Context, sharedSf *singleflight.Group, ds storage.OpenFGADatastore, settings serverconfig.CacheSettings, opts ...SharedCheckResourcesOpt) (*SharedCheckResources, error) {
	s := &SharedCheckResources{
		WaitGroup:         &sync.WaitGroup{},
		SingleflightGroup: sharedSf,
		ServerCtx:         sharedCtx,
		CacheInvalidator:  cacheinvalidator.NewNoopCacheInvalidator(),
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

	if settings.ShouldCreateCacheInvalidator() {
		s.CacheInvalidator = cacheinvalidator.NewCacheInvalidator(ds, s.CheckCache, settings.CacheInvalidatorTTL, settings.CheckIteratorCacheTTL, cacheinvalidator.WithLogger(s.Logger))
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
