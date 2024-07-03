package graph

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
)

func TestNewCheckResolverBuilder(t *testing.T) {
	type Test struct {
		name                      string
		cacheEnabled              bool
		dispatchThrottlingEnabled bool
		expectedResolverOrder     []CheckResolver
	}

	tests := []Test{
		{
			name:                      "when_nothing_is_enabled",
			cacheEnabled:              false,
			dispatchThrottlingEnabled: false,
			expectedResolverOrder:     []CheckResolver{&CycleDetectionCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                      "when_cache_alone_is_enabled",
			cacheEnabled:              true,
			dispatchThrottlingEnabled: false,
			expectedResolverOrder:     []CheckResolver{&CycleDetectionCheckResolver{}, &CachedCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                      "when_dispatch_throttling_alone_is_enabled",
			cacheEnabled:              false,
			dispatchThrottlingEnabled: true,
			expectedResolverOrder:     []CheckResolver{&CycleDetectionCheckResolver{}, &DispatchThrottlingCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                      "when_both_are_enabled",
			cacheEnabled:              true,
			dispatchThrottlingEnabled: true,
			expectedResolverOrder:     []CheckResolver{&CycleDetectionCheckResolver{}, &CachedCheckResolver{}, &DispatchThrottlingCheckResolver{}, &LocalChecker{}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := []CheckQueryBuilderOpt{}
			if test.cacheEnabled {
				opts = append(opts, WithCachedCheckResolver())
			}
			if test.dispatchThrottlingEnabled {
				opts = append(opts, WithDispatchThrottlingCheckResolver())
			}
			opts = append(opts, WithLocalChecker())
			builder := NewCheckQueryBuilder(opts...)
			_, checkCloser := builder.Build()
			t.Cleanup(checkCloser)

			for i, resolver := range builder.resolvers {
				require.Equal(t, reflect.TypeOf(test.expectedResolverOrder[i]), reflect.TypeOf(resolver))
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
		WithCachedCheckResolver(cacheOpts...),
		WithDispatchThrottlingCheckResolver(dispatchOpts...),
		WithLocalChecker(localCheckerOpts...),
	).Build()
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
// func TestCheckResolverClose(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//}
