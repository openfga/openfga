package check

import (
	"github.com/openfga/openfga/internal/graph"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
)

func TestNewLayeredCheckResolver(t *testing.T) {
	type Test struct {
		name                      string
		cacheEnabled              bool
		dispatchThrottlingEnabled bool
	}

	tests := []Test{
		{
			name:                      "when_nothing_is_enabled",
			cacheEnabled:              false,
			dispatchThrottlingEnabled: false,
		},
		{
			name:                      "when_cache_alone_is_enabled",
			cacheEnabled:              true,
			dispatchThrottlingEnabled: false,
		},
		{
			name:                      "when_dispatch_throttling_alone_is_enabled",
			cacheEnabled:              false,
			dispatchThrottlingEnabled: true,
		},
		{
			name:                      "when_both_are_enabled",
			cacheEnabled:              true,
			dispatchThrottlingEnabled: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewCheckQueryBuilder()
			var cacheCheckResolver *graph.CachedCheckResolver
			var dispatchCheckResolver *graph.DispatchThrottlingCheckResolver

			checkResolver, checkCloser := builder.NewLayeredCheckResolver(test.cacheEnabled, test.dispatchThrottlingEnabled)
			t.Cleanup(checkCloser)
			cycleDetectionCheckResolver, ok := checkResolver.(*graph.CycleDetectionCheckResolver)
			require.True(t, ok)

			if test.cacheEnabled {
				cacheCheckResolver, ok = cycleDetectionCheckResolver.GetDelegate().(*graph.CachedCheckResolver)
				require.True(t, ok)
			}

			if test.dispatchThrottlingEnabled {
				if !test.cacheEnabled {
					dispatchCheckResolver, ok = cycleDetectionCheckResolver.GetDelegate().(*graph.DispatchThrottlingCheckResolver)
					require.True(t, ok)
				} else {
					dispatchCheckResolver, ok = cacheCheckResolver.GetDelegate().(*graph.DispatchThrottlingCheckResolver)
					require.True(t, ok)
				}

				_, ok = dispatchCheckResolver.GetDelegate().(*graph.LocalChecker)
				require.True(t, ok)
			} else {
				if test.cacheEnabled {
					_, ok = cacheCheckResolver.GetDelegate().(*graph.LocalChecker)
					require.True(t, ok)
				} else {
					_, ok = cycleDetectionCheckResolver.GetDelegate().(*graph.LocalChecker)
					require.True(t, ok)
				}
			}
		})
	}
}

func TestOptsBeingPassed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockThrottler := mocks.NewMockThrottler(ctrl)

	cacheOpts := []graph.CachedCheckResolverOpt{
		graph.WithCacheTTL(1 * time.Second),
		graph.WithMaxCacheSize(2),
	}

	dispatchOpts := []graph.DispatchThrottlingCheckResolverOpt{
		graph.WithDispatchThrottlingCheckResolverConfig(graph.DispatchThrottlingCheckResolverConfig{
			DefaultThreshold: 3,
			MaxThreshold:     4,
		}),
		graph.WithThrottler(mockThrottler),
	}

	localCheckerOpts := []graph.LocalCheckerOption{
		graph.WithMaxConcurrentReads(6),
		graph.WithResolveNodeBreadthLimit(7),
	}

	checkResolver, checkCloser := NewCheckQueryBuilder(
		WithCachedCheckResolverOpts(cacheOpts...),
		WithDispatchThrottlingCheckResolverOpts(dispatchOpts...),
		WithLocalCheckerOpts(localCheckerOpts...),
	).NewLayeredCheckResolver(true, true)
	t.Cleanup(func() {
		mockThrottler.EXPECT().Close().Times(2)
		checkCloser()
	})

	cycleDetectionCheckResolver := checkResolver.(*graph.CycleDetectionCheckResolver)
	cacheCheckResolver := cycleDetectionCheckResolver.GetDelegate().(*graph.CachedCheckResolver)
	dispatchCheckResolver := cacheCheckResolver.GetDelegate().(*graph.DispatchThrottlingCheckResolver)
	localCheckResolver := dispatchCheckResolver.GetDelegate().(*graph.LocalChecker)

	require.Equal(t, 1*time.Second, cacheCheckResolver.cacheTTL)
	require.Equal(t, int64(2), cacheCheckResolver.maxCacheSize)
	require.Equal(t, uint32(3), dispatchCheckResolver.config.DefaultThreshold)
	require.Equal(t, uint32(4), dispatchCheckResolver.config.MaxThreshold)
	require.Equal(t, mockThrottler, dispatchCheckResolver.throttler)
	require.Equal(t, uint32(6), localCheckResolver.maxConcurrentReads)
	require.Equal(t, uint32(7), localCheckResolver.concurrencyLimit)
}

// TODO any good way to test the close method?
// func TestCheckResolverClose(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//}
