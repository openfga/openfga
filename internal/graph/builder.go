package graph

type CheckResolverOrderedBuilder struct {
	resolvers                              []CheckResolver
	localCheckerOptions                    []LocalCheckerOption
	cachedCheckResolverOptions             []CachedCheckResolverOpt
	dispatchThrottlingCheckResolverOptions []DispatchThrottlingCheckResolverOpt
}

type CheckResolverOrderedBuilderOpt func(checkResolver *CheckResolverOrderedBuilder)

// WithLocalCheckerOpts sets the opts to be used to build LocalChecker.
func WithLocalCheckerOpts(opts ...LocalCheckerOption) CheckResolverOrderedBuilderOpt {
	return func(r *CheckResolverOrderedBuilder) {
		r.localCheckerOptions = opts
	}
}

// WithCachedCheckResolverOpts sets the opts to be used to build CachedCheckResolver.
func WithCachedCheckResolverOpts(opts ...CachedCheckResolverOpt) CheckResolverOrderedBuilderOpt {
	return func(r *CheckResolverOrderedBuilder) {
		r.cachedCheckResolverOptions = opts
	}
}

// WithDispatchThrottlingCheckResolverOpts sets the opts to be used to build DispatchThrottlingCheckResolver.
func WithDispatchThrottlingCheckResolverOpts(opts ...DispatchThrottlingCheckResolverOpt) CheckResolverOrderedBuilderOpt {
	return func(r *CheckResolverOrderedBuilder) {
		r.dispatchThrottlingCheckResolverOptions = opts
	}
}

func NewOrderedCheckResolvers(opts ...CheckResolverOrderedBuilderOpt) *CheckResolverOrderedBuilder {
	checkResolverBuilder := &CheckResolverOrderedBuilder{}
	checkResolverBuilder.resolvers = []CheckResolver{}
	for _, opt := range opts {
		opt(checkResolverBuilder)
	}
	return checkResolverBuilder
}

// Build constructs a CheckResolver that is composed of various CheckResolvers in the manner of a circular linked list.  CycleDetectionCheckResolver is always the first resolver in the composition and the last resolver added will always point to it.
// The resolvers should added from least resource intensive to most resource intensive.
//
//	CycleDetectionCheckResolver  <----------------------|
//		[...Other resolvers depending on the opts order]
//			CycleDetectionCheckResolver -------^
//
// The returned CheckResolverCloser should be used to close all resolvers involved in the list.
func (c *CheckResolverOrderedBuilder) Build() (CheckResolver, CheckResolverCloser) {
	c.resolvers = append(c.resolvers, NewCycleDetectionCheckResolver())

	if len(c.cachedCheckResolverOptions) > 0 {
		c.resolvers = append(c.resolvers, NewCachedCheckResolver(c.cachedCheckResolverOptions...))
	}

	if len(c.dispatchThrottlingCheckResolverOptions) > 0 {
		c.resolvers = append(c.resolvers, NewDispatchThrottlingCheckResolver(c.dispatchThrottlingCheckResolverOptions...))
	}

	c.resolvers = append(c.resolvers, NewLocalChecker(c.localCheckerOptions...))

	for i, resolver := range c.resolvers {
		if i == len(c.resolvers)-1 {
			resolver.SetDelegate(c.resolvers[0])
			continue
		}
		resolver.SetDelegate(c.resolvers[i+1])
	}

	return c.resolvers[0], c.close
}

// close will ensure all the CheckResolver constructed are closed.
func (c *CheckResolverOrderedBuilder) close() {
	for _, resolver := range c.resolvers {
		resolver.Close()
	}
}
