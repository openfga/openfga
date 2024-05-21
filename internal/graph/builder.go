package graph

import (
	"github.com/openfga/openfga/pkg/logger"
)

type CheckResolverBuilder struct {
	logger logger.Logger

	localCheckerOpts       []LocalCheckerOption
	cacheOpts              []CachedCheckResolverOpt
	dispatchThrottlingOpts []DispatchThrottlingCheckResolverOpt

	cycleDetectionCheckResolver     *CycleDetectionCheckResolver
	cachedCheckResolver             *CachedCheckResolver
	dispatchThrottlingCheckResolver *DispatchThrottlingCheckResolver
	localCheckResolver              *LocalChecker
}

// CheckQueryBuilderOpt defines an option that can be used to change the behavior of CheckResolverBuilder
// instance.
type CheckQueryBuilderOpt func(checkResolver *CheckResolverBuilder)

// WithLocalCheckerOpts sets the opts to be used to build LocalChecker.
func WithLocalCheckerOpts(opts ...LocalCheckerOption) CheckQueryBuilderOpt {
	return func(r *CheckResolverBuilder) {
		r.localCheckerOpts = opts
	}
}

// WithCachedCheckResolverOpts sets the opts to be used to build CachedCheckResolver.
func WithCachedCheckResolverOpts(opts ...CachedCheckResolverOpt) CheckQueryBuilderOpt {
	return func(r *CheckResolverBuilder) {
		r.cacheOpts = opts
	}
}

// WithDispatchThrottlingCheckResolverOpts sets the opts to be used to build DispatchThrottlingCheckResolver.
func WithDispatchThrottlingCheckResolverOpts(opts ...DispatchThrottlingCheckResolverOpt) CheckQueryBuilderOpt {
	return func(r *CheckResolverBuilder) {
		r.dispatchThrottlingOpts = opts
	}
}

func NewCheckQueryBuilder(opts ...CheckQueryBuilderOpt) *CheckResolverBuilder {
	checkQueryBuilder := &CheckResolverBuilder{}
	for _, opt := range opts {
		opt(checkQueryBuilder)
	}
	return checkQueryBuilder
}

// NewLayeredCheckResolver constructs a CheckResolver that is composed of various CheckResolver layers.
// Specifically, it constructs a CheckResolver with the following composition:
//
//	CycleDetectionCheckResolver  <-----|
//		CachedCheckResolver              |
//			DispatchThrottlingCheckResolver |
//				LocalChecker                   |
//					CycleDetectionCheckResolver -|
//
// The returned CheckResolverCloser should be used to close all resolvers involved in the
// composition after you are done with the CheckResolver.
func (c *CheckResolverBuilder) NewLayeredCheckResolver(
	cacheEnabled bool,
	dispatchThrottlingEnabled bool,
) (CheckResolver, CheckResolverCloser) {
	c.cycleDetectionCheckResolver = NewCycleDetectionCheckResolver()
	c.localCheckResolver = NewLocalChecker(c.localCheckerOpts...)

	c.cycleDetectionCheckResolver.SetDelegate(c.localCheckResolver)
	c.localCheckResolver.SetDelegate(c.cycleDetectionCheckResolver)

	if cacheEnabled {
		c.cachedCheckResolver = NewCachedCheckResolver(c.cacheOpts...)

		c.cachedCheckResolver.SetDelegate(c.localCheckResolver)
		c.cycleDetectionCheckResolver.SetDelegate(c.cachedCheckResolver)
	}

	if dispatchThrottlingEnabled {
		c.dispatchThrottlingCheckResolver = NewDispatchThrottlingCheckResolver(c.dispatchThrottlingOpts...)
		c.dispatchThrottlingCheckResolver.SetDelegate(c.localCheckResolver)
		if cacheEnabled {
			c.cachedCheckResolver.SetDelegate(c.dispatchThrottlingCheckResolver)
		} else {
			c.cycleDetectionCheckResolver.SetDelegate(c.dispatchThrottlingCheckResolver)
		}
	}

	return c.cycleDetectionCheckResolver, c.close
}

// close will ensure all the CheckResolver constructed are closed
func (c *CheckResolverBuilder) close() {
	c.localCheckResolver.Close()

	if c.cachedCheckResolver != nil {
		c.cachedCheckResolver.Close()
	}

	if c.dispatchThrottlingCheckResolver != nil {
		c.dispatchThrottlingCheckResolver.Close()
	}

	c.cycleDetectionCheckResolver.Close()
}
