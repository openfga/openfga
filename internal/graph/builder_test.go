package graph

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOrderedCheckResolverBuilder(t *testing.T) {
	type Test struct {
		name                                   string
		CachedCheckResolverEnabled             bool
		DispatchThrottlingCheckResolverEnabled bool
		TrackerCheckResolverEnabled            bool
		expectedResolverOrder                  []CheckResolver
	}

	tests := []Test{
		{
			name:                  "when_nothing_is_enabled",
			expectedResolverOrder: []CheckResolver{&CycleDetectionCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                       "when_cache_alone_is_enabled",
			CachedCheckResolverEnabled: true,
			expectedResolverOrder:      []CheckResolver{&CycleDetectionCheckResolver{}, &CachedCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                                   "when_dispatch_throttling_alone_is_enabled",
			DispatchThrottlingCheckResolverEnabled: true,
			expectedResolverOrder:                  []CheckResolver{&CycleDetectionCheckResolver{}, &DispatchThrottlingCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                        "when_track_check_is_enabled",
			TrackerCheckResolverEnabled: true,
			expectedResolverOrder:       []CheckResolver{&CycleDetectionCheckResolver{}, &TrackerCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                                   "when_all_are_enabled",
			CachedCheckResolverEnabled:             true,
			DispatchThrottlingCheckResolverEnabled: true,
			TrackerCheckResolverEnabled:            true,
			expectedResolverOrder:                  []CheckResolver{&CycleDetectionCheckResolver{}, &CachedCheckResolver{}, &DispatchThrottlingCheckResolver{}, &TrackerCheckResolver{}, &LocalChecker{}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewOrderedCheckResolvers([]CheckResolverOrderedBuilderOpt{
				WithCachedCheckResolverOpts(test.CachedCheckResolverEnabled),
				WithDispatchThrottlingCheckResolverOpts(test.DispatchThrottlingCheckResolverEnabled),
				WithTrackerCheckResolverOpts(test.TrackerCheckResolverEnabled),
			}...)
			_, checkResolverCloser := builder.Build()
			t.Cleanup(checkResolverCloser)

			for i, resolver := range builder.resolvers {
				require.Equal(t, reflect.TypeOf(test.expectedResolverOrder[i]), reflect.TypeOf(resolver))
			}
		})
	}
}
