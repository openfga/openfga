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
		ShadowResolverEnabled                  bool
		expectedResolverOrder                  []CheckResolver
	}

	tests := []Test{
		{
			name:                  "when_nothing_is_enabled",
			expectedResolverOrder: []CheckResolver{&LocalChecker{}},
		},
		{
			name:                       "when_cache_alone_is_enabled",
			CachedCheckResolverEnabled: true,
			expectedResolverOrder:      []CheckResolver{&CachedCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                                   "when_dispatch_throttling_alone_is_enabled",
			DispatchThrottlingCheckResolverEnabled: true,
			expectedResolverOrder:                  []CheckResolver{&DispatchThrottlingCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                                   "when_all_are_enabled",
			CachedCheckResolverEnabled:             true,
			DispatchThrottlingCheckResolverEnabled: true,
			expectedResolverOrder:                  []CheckResolver{&CachedCheckResolver{}, &DispatchThrottlingCheckResolver{}, &LocalChecker{}},
		},
		{
			name:                                   "when_all_are_enabled_with_shadow",
			CachedCheckResolverEnabled:             true,
			DispatchThrottlingCheckResolverEnabled: true,
			ShadowResolverEnabled:                  true,
			expectedResolverOrder:                  []CheckResolver{&CachedCheckResolver{}, &DispatchThrottlingCheckResolver{}, &ShadowResolver{main: &LocalChecker{}, shadow: &LocalChecker{}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewOrderedCheckResolvers([]CheckResolverOrderedBuilderOpt{
				WithCachedCheckResolverOpts(test.CachedCheckResolverEnabled),
				WithDispatchThrottlingCheckResolverOpts(test.DispatchThrottlingCheckResolverEnabled),
				WithShadowResolverEnabled(test.ShadowResolverEnabled),
			}...)
			checkResolver, checkResolverCloser, err := builder.Build()
			require.NoError(t, err)
			t.Cleanup(checkResolverCloser)

			for i, resolver := range builder.resolvers {
				require.Equal(t, reflect.TypeOf(test.expectedResolverOrder[i]), reflect.TypeOf(resolver))
			}

			localChecker, found := LocalCheckResolver(checkResolver)
			require.True(t, found)
			require.NotNil(t, localChecker)
		})
	}
}

func TestLocalCheckResolver(t *testing.T) {
	t.Run("nil_ptr", func(t *testing.T) {
		_, found := LocalCheckResolver(nil)
		require.False(t, found)
	})
	t.Run("no_delegate_for_non_local_resolver", func(t *testing.T) {
		cachedCheckResolver, err := NewCachedCheckResolver()
		require.NoError(t, err)
		defer cachedCheckResolver.Close()
		_, found := LocalCheckResolver(cachedCheckResolver)
		require.False(t, found)
	})
	t.Run("delegate_for_non_local_resolver", func(t *testing.T) {
		cachedCheckResolver, err := NewCachedCheckResolver()
		require.NoError(t, err)
		defer cachedCheckResolver.Close()
		localResolver := NewLocalChecker()
		cachedCheckResolver.SetDelegate(localResolver)
		dut, found := LocalCheckResolver(cachedCheckResolver)
		require.True(t, found)
		require.Equal(t, localResolver, dut)
	})
	t.Run("local_resolver", func(t *testing.T) {
		localResolver := NewLocalChecker()
		defer localResolver.Close()
		dut, found := LocalCheckResolver(localResolver)
		require.True(t, found)
		require.Equal(t, localResolver, dut)
	})
	t.Run("shadow_resolver", func(t *testing.T) {
		cachedCheckResolver, err := NewCachedCheckResolver()
		require.NoError(t, err)
		defer cachedCheckResolver.Close()
		mainResolver := NewLocalChecker()
		shadowResolver := NewLocalChecker()
		shadow := NewShadowChecker(mainResolver, shadowResolver)
		cachedCheckResolver.SetDelegate(shadow)
		dut, found := LocalCheckResolver(cachedCheckResolver)

		require.True(t, found)
		require.Equal(t, mainResolver, dut)
	})
}
