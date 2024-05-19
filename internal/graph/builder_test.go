package graph

import (
	"github.com/openfga/openfga/internal/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"testing"
	"time"
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
			var cacheCheckResolver *CachedCheckResolver
			var dispatchCheckResolver *DispatchThrottlingCheckResolver

			checkResolver, checkCloser := builder.NewLayeredCheckResolver(test.cacheEnabled, test.dispatchThrottlingEnabled)
			t.Cleanup(checkCloser)
			cycleDetectionCheckResolver, ok := checkResolver.(*CycleDetectionCheckResolver)
			require.True(t, ok)

			if test.cacheEnabled {
				cacheCheckResolver, ok = cycleDetectionCheckResolver.GetDelegate().(*CachedCheckResolver)
				require.True(t, ok)
			}

			if test.dispatchThrottlingEnabled {
				if !test.cacheEnabled {
					dispatchCheckResolver, ok = cycleDetectionCheckResolver.GetDelegate().(*DispatchThrottlingCheckResolver)
					require.True(t, ok)
				} else {
					dispatchCheckResolver, ok = cacheCheckResolver.GetDelegate().(*DispatchThrottlingCheckResolver)
					require.True(t, ok)
				}

				_, ok = dispatchCheckResolver.GetDelegate().(*LocalChecker)
				require.True(t, ok)
			} else {
				if test.cacheEnabled {
					_, ok = cacheCheckResolver.GetDelegate().(*LocalChecker)
					require.True(t, ok)
				} else {
					_, ok = cycleDetectionCheckResolver.GetDelegate().(*LocalChecker)
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

	cacheOpts := []CachedCheckResolverOpt{
		WithCacheTTL(1 * time.Second),
		WithMaxCacheSize(2),
	}

	dispatchOpts := []DispatchThrottlingCheckResolverOpt{
		WithDispatchThrottlingCheckResolverConfig(DispatchThrottlingCheckResolverConfig{
			DefaultThreshold: 3,
			MaxThreshold:     4,
		}),
		WithThrottler(mockThrottler),
	}

	localCheckerOpts := []LocalCheckerOption{
		WithMaxConcurrentReads(6),
		WithResolveNodeBreadthLimit(7),
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

	cycleDetectionCheckResolver := checkResolver.(*CycleDetectionCheckResolver)
	cacheCheckResolver := cycleDetectionCheckResolver.GetDelegate().(*CachedCheckResolver)
	dispatchCheckResolver := cacheCheckResolver.GetDelegate().(*DispatchThrottlingCheckResolver)
	localCheckResolver := dispatchCheckResolver.GetDelegate().(*LocalChecker)

	require.Equal(t, 1*time.Second, cacheCheckResolver.cacheTTL)
	require.Equal(t, int64(2), cacheCheckResolver.maxCacheSize)
	require.Equal(t, uint32(3), dispatchCheckResolver.config.DefaultThreshold)
	require.Equal(t, uint32(4), dispatchCheckResolver.config.MaxThreshold)
	require.Equal(t, mockThrottler, dispatchCheckResolver.throttler)
	require.Equal(t, uint32(6), localCheckResolver.maxConcurrentReads)
	require.Equal(t, uint32(7), localCheckResolver.concurrencyLimit)
}

// TODO any good way to test the close method?
//func TestCheckResolverClose(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//}
