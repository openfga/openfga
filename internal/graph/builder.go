package graph

type CheckResolverBuilder struct {
	resolvers []CheckResolver
}

// CheckQueryBuilderOpt defines an option that can be used to change the behavior of CheckResolverBuilder
// instance.
type CheckQueryBuilderOpt func(checkResolver *CheckResolverBuilder)

// WithLocalChecker sets the opts to be used to build LocalChecker.
func WithLocalChecker(opts ...LocalCheckerOption) CheckQueryBuilderOpt {
	return func(r *CheckResolverBuilder) {
		r.resolvers = append(r.resolvers, NewLocalChecker(opts...))
	}
}

// WithCachedCheckResolver sets the opts to be used to build CachedCheckResolver.
func WithCachedCheckResolver(opts ...CachedCheckResolverOpt) CheckQueryBuilderOpt {
	return func(r *CheckResolverBuilder) {
		r.resolvers = append(r.resolvers, NewCachedCheckResolver(opts...))
	}
}

// WithDispatchThrottlingCheckResolver sets the opts to be used to build DispatchThrottlingCheckResolver.
func WithDispatchThrottlingCheckResolver(opts ...DispatchThrottlingCheckResolverOpt) CheckQueryBuilderOpt {
	return func(r *CheckResolverBuilder) {
		r.resolvers = append(r.resolvers, NewDispatchThrottlingCheckResolver(opts...))
	}
}

func NewCheckQueryBuilder(opts ...CheckQueryBuilderOpt) *CheckResolverBuilder {
	checkResolverBuilder := &CheckResolverBuilder{}
	checkResolverBuilder.resolvers = []CheckResolver{NewCycleDetectionCheckResolver()}
	for _, opt := range opts {
		opt(checkResolverBuilder)
	}
	return checkResolverBuilder
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
func (c *CheckResolverBuilder) close() {
	for _, resolver := range c.resolvers {
		resolver.Close()
	}
}
