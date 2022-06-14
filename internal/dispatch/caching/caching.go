// Package caching implements a dispatcher with a caching layer.
package caching

import (
	"context"
	"unsafe"

	"github.com/openfga/openfga/internal/dispatch"
	"github.com/openfga/openfga/pkg/cache"
	dispatchpb "github.com/openfga/openfga/pkg/proto/dispatch/v1"
	"google.golang.org/protobuf/proto"
)

var (
	checkCacheEntryCost = int64(unsafe.Sizeof(checkCacheEntry{}))
)

var _ dispatch.Dispatcher = &cachingDispatcher{}

// cachingDispatcher provides an implementation of the dispatch.Dispatcher interface
// with caching built in.
type cachingDispatcher struct {
	redispatcher dispatch.Dispatcher
	cache        cache.Cache

	// todo(jon-whit): add caching metrics
}

type checkCacheEntry struct {
	response *dispatchpb.DispatchCheckResponse
}

type expandCacheEntry struct {
	response *dispatchpb.DispatchExpandResponse
}

// NewCachingDispatcher creates a new instance of a dispatcher with caching built in.
func NewCachingDispatcher() (dispatch.Dispatcher, error) {

	return &cachingDispatcher{}, nil
}

// DispatchCheck provides an implementation of the dispatch.Check interface and implements
// the Check API. It attempts to load a cached result of a prior check on the same relation
// tuple, but falls back to the resolution of the graph if no cached result was found.
func (cd *cachingDispatcher) DispatchCheck(ctx context.Context, req *dispatchpb.DispatchCheckRequest) (*dispatchpb.DispatchCheckResponse, error) {

	var cacheKey interface{}

	if cachedResult, found := cd.cache.Get(cacheKey); found {
		cachedResult := cachedResult.(checkCacheEntry)
		if req.GetMetadata().GetDepthRemaining() >= cachedResult.response.GetMetadata().GetDepthRequired() {
			return cachedResult.response, nil
		}
	}

	computed, err := cd.redispatcher.DispatchCheck(ctx, req)

	// fill the cache only if there was no error
	if err == nil {

		computedCopy := proto.Clone(computed).(*dispatchpb.DispatchCheckResponse)
		computedCopy.Metadata.CachedDispatchCount = computedCopy.Metadata.DispatchCount
		computedCopy.Metadata.DispatchCount = 0

		cacheValue := checkCacheEntry{computedCopy}
		cd.cache.Set(cacheKey, cacheValue, checkCacheEntryCost)
	}

	// Return computed and err in every case - computed should contain resolved metadata if if err is non-nil
	return computed, err
}

func (cd *cachingDispatcher) DispatchExpand(ctx context.Context, req *dispatchpb.DispatchExpandRequest) (*dispatchpb.DispatchExpandResponse, error) {
	return cd.redispatcher.DispatchExpand(ctx, req)
}

// IsReady reports whether this caching dispatcher is ready to serve
// requests.
func (cd *cachingDispatcher) IsReady() bool {
	return cd.cache != nil && cd.redispatcher.IsReady()
}

// Close closes the caching dispatcher by closing and cleaning up any residual resources.
func (cd *cachingDispatcher) Close(ctx context.Context) error {

	if cache := cd.cache; cache != nil {
		cache.Close()
	}

	return nil
}
