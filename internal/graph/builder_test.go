package graph

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCheckResolverBuilder(t *testing.T) {
	type Test struct {
		name                               string
		LocalCheckerOption                 []LocalCheckerOption
		CachedCheckResolverOpt             []CachedCheckResolverOpt
		DispatchThrottlingCheckResolverOpt []DispatchThrottlingCheckResolverOpt
		expectedResolverOrder              []CheckResolver
	}

	tests := []Test{
		{
			name:                  "when_nothing_is_enabled",
			expectedResolverOrder: []CheckResolver{&CycleDetectionCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                   "when_cache_alone_is_enabled",
			CachedCheckResolverOpt: []CachedCheckResolverOpt{WithCacheTTL(1)},
			expectedResolverOrder:  []CheckResolver{&CycleDetectionCheckResolver{}, &CachedCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                               "when_dispatch_throttling_alone_is_enabled",
			DispatchThrottlingCheckResolverOpt: []DispatchThrottlingCheckResolverOpt{WithDispatchThrottlingCheckResolverConfig(DispatchThrottlingCheckResolverConfig{})},
			expectedResolverOrder:              []CheckResolver{&CycleDetectionCheckResolver{}, &DispatchThrottlingCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                               "when_both_are_enabled",
			DispatchThrottlingCheckResolverOpt: []DispatchThrottlingCheckResolverOpt{WithDispatchThrottlingCheckResolverConfig(DispatchThrottlingCheckResolverConfig{})},
			CachedCheckResolverOpt:             []CachedCheckResolverOpt{WithCacheTTL(1)},
			expectedResolverOrder:              []CheckResolver{&CycleDetectionCheckResolver{}, &CachedCheckResolver{}, &DispatchThrottlingCheckResolver{}, &LocalChecker{}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewOrderedCheckResolvers([]CheckResolverOrderedBuilderOpt{
				WithLocalCheckerOpts(test.LocalCheckerOption...),
				WithCachedCheckResolverOpts(test.CachedCheckResolverOpt...),
				WithDispatchThrottlingCheckResolverOpts(test.DispatchThrottlingCheckResolverOpt...),
			}...)
			_, checkResolverCloser := builder.Build()
			t.Cleanup(checkResolverCloser)

			for i, resolver := range builder.resolvers {
				require.Equal(t, reflect.TypeOf(test.expectedResolverOrder[i]), reflect.TypeOf(resolver))
			}
		})
	}
}
