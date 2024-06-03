package graph

type CheckResolverBuilder struct {
	localCheckerOpts       []LocalCheckerOption
	cacheOpts              []CachedCheckResolverOpt
	dispatchThrottlingOpts []DispatchThrottlingCheckResolverOpt

	cacheEnabled              bool
	dispatchThrottlingEnabled bool

	resolvers []CheckResolver
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

func WithCacheEnabled() CheckQueryBuilderOpt {
	return func(r *CheckResolverBuilder) {
		r.cacheEnabled = true
	}
}

func WithDispatchThrottlingEnabled() CheckQueryBuilderOpt {
	return func(r *CheckResolverBuilder) {
		r.dispatchThrottlingEnabled = true
	}
}

func NewCheckQueryBuilder(opts ...CheckQueryBuilderOpt) *CheckResolverBuilder {
	checkQueryBuilder := &CheckResolverBuilder{}
	for _, opt := range opts {
		opt(checkQueryBuilder)
	}
	return checkQueryBuilder
}

// Build constructs a CheckResolver that is composed of various CheckResolver layers.
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
func (c *CheckResolverBuilder) Build() (CheckResolver, CheckResolverCloser) {
	cycleDetectionCheckResolver := NewCycleDetectionCheckResolver()
	checkResolvers := []CheckResolver{cycleDetectionCheckResolver}

	if c.cacheEnabled {
		cachedCheckResolver := NewCachedCheckResolver(c.cacheOpts...)
		checkResolvers = append(checkResolvers, cachedCheckResolver)
	}

	if c.dispatchThrottlingEnabled {
		dispatchThrottlingCheckResolver := NewDispatchThrottlingCheckResolver(c.dispatchThrottlingOpts...)
		checkResolvers = append(checkResolvers, dispatchThrottlingCheckResolver)
	}

	localCheckResolver := NewLocalChecker(c.localCheckerOpts...)
	checkResolvers = append(checkResolvers, localCheckResolver)
	c.resolvers = checkResolvers

	for i, resolver := range c.resolvers {
		if i == len(c.resolvers)-1 {
			resolver.SetDelegate(c.resolvers[0])
			continue
		}
		resolver.SetDelegate(c.resolvers[i+1])
	}

	return cycleDetectionCheckResolver, c.close
}

// close will ensure all the CheckResolver constructed are closed.
func (c *CheckResolverBuilder) close() {
	for _, resolver := range c.resolvers {
		resolver.Close()
	}
}
