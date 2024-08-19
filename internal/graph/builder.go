package graph

type CheckResolverOrderedBuilder struct {
	resolvers                              []CheckResolver
	localCheckerOptions                    []LocalCheckerOption
	cachedCheckResolverEnabled             bool
	cachedCheckResolverOptions             []CachedCheckResolverOpt
	dispatchThrottlingCheckResolverEnabled bool
	dispatchThrottlingCheckResolverOptions []DispatchThrottlingCheckResolverOpt
	trackerCheckResolverEnabled            bool
	trackerCheckResolverOptions            []TrackerCheckResolverOpt
}

type CheckResolverOrderedBuilderOpt func(checkResolver *CheckResolverOrderedBuilder)

// WithLocalCheckerOpts sets the opts to be used to build LocalChecker.
func WithLocalCheckerOpts(opts ...LocalCheckerOption) CheckResolverOrderedBuilderOpt {
	return func(r *CheckResolverOrderedBuilder) {
		r.localCheckerOptions = opts
	}
}

// WithCachedCheckResolverOpts sets the opts to be used to build CachedCheckResolver.
func WithCachedCheckResolverOpts(enabled bool, opts ...CachedCheckResolverOpt) CheckResolverOrderedBuilderOpt {
	return func(r *CheckResolverOrderedBuilder) {
		r.cachedCheckResolverEnabled = enabled
		r.cachedCheckResolverOptions = opts
	}
}

// WithDispatchThrottlingCheckResolverOpts sets the opts to be used to build DispatchThrottlingCheckResolver.
func WithDispatchThrottlingCheckResolverOpts(enabled bool, opts ...DispatchThrottlingCheckResolverOpt) CheckResolverOrderedBuilderOpt {
	return func(r *CheckResolverOrderedBuilder) {
		r.dispatchThrottlingCheckResolverEnabled = enabled
		r.dispatchThrottlingCheckResolverOptions = opts
	}
}

// WithTrackerCheckResolverOpts sets the opts to be used to build TrackerCheckResolver.
func WithTrackerCheckResolverOpts(enabled bool, opts ...TrackerCheckResolverOpt) CheckResolverOrderedBuilderOpt {
	return func(r *CheckResolverOrderedBuilder) {
		r.trackerCheckResolverEnabled = enabled
		r.trackerCheckResolverOptions = opts
	}
}

func NewOrderedCheckResolvers(opts ...CheckResolverOrderedBuilderOpt) *CheckResolverOrderedBuilder {
	checkResolverBuilder := &CheckResolverOrderedBuilder{}
	for _, opt := range opts {
		opt(checkResolverBuilder)
	}
	return checkResolverBuilder
}

// Build constructs a CheckResolver that is composed of various CheckResolvers in the manner of a circular linked list.
// The resolvers should be added from least resource intensive to most resource intensive.
//
//	[...Other resolvers depending on the opts order]
//		LocalChecker    ----------------------------^
//
// The returned CheckResolverCloser should be used to close all resolvers involved in the list.
func (c *CheckResolverOrderedBuilder) Build() (CheckResolver, CheckResolverCloser) {
	c.resolvers = []CheckResolver{}

	if c.cachedCheckResolverEnabled {
		c.resolvers = append(c.resolvers, NewCachedCheckResolver(c.cachedCheckResolverOptions...))
	}

	if c.dispatchThrottlingCheckResolverEnabled {
		c.resolvers = append(c.resolvers, NewDispatchThrottlingCheckResolver(c.dispatchThrottlingCheckResolverOptions...))
	}

	if c.trackerCheckResolverEnabled {
		c.resolvers = append(c.resolvers, NewTrackCheckResolver(c.trackerCheckResolverOptions...))
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
