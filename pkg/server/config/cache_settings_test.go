package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheSettings(t *testing.T) {
	t.Run("should_create_new_cache", func(t *testing.T) {
		tests := []struct {
			name                   string
			cacheSettings          CacheSettings
			expectedCreateNewCache bool
		}{
			{
				name: "not_when_limit_is_zero",
				cacheSettings: CacheSettings{
					CheckCacheLimit: 0,
				},
				expectedCreateNewCache: false,
			},
			{
				name: "not_when_limit_is_zero_and_query_cache_enabled",
				cacheSettings: CacheSettings{
					CheckCacheLimit:        0,
					CheckQueryCacheEnabled: true,
				},
				expectedCreateNewCache: false,
			},
			{
				name: "not_when_limit_is_zero_and_iterator_cache_enabled",
				cacheSettings: CacheSettings{
					CheckCacheLimit:      0,
					IteratorCacheEnabled: true,
				},
				expectedCreateNewCache: false,
			},
			{
				name: "when_limit_over_zero_and_query_cache_enabled",
				cacheSettings: CacheSettings{
					CheckCacheLimit:        10,
					CheckQueryCacheEnabled: true,
				},
				expectedCreateNewCache: true,
			},
			{
				name: "when_limit_over_zero_and_iterator_cache_enabled",
				cacheSettings: CacheSettings{
					CheckCacheLimit:      10,
					IteratorCacheEnabled: true,
				},
				expectedCreateNewCache: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := tt.cacheSettings.ShouldCreateNewCache()
				assert.Equal(t, tt.expectedCreateNewCache, got)
			})
		}
	})

	t.Run("should_create_new_cache_controller", func(t *testing.T) {
		tests := []struct {
			name                          string
			cacheSettings                 CacheSettings
			expectedCreateCacheController bool
		}{
			{
				name: "not_when_limit_is_zero",
				cacheSettings: CacheSettings{
					CheckCacheLimit: 0,
				},
				expectedCreateCacheController: false,
			},
			{
				name: "when_limit_over_zero_and_query_cache_enabled",
				cacheSettings: CacheSettings{
					CheckCacheLimit:        10,
					CheckQueryCacheEnabled: true,
					CacheControllerEnabled: true,
				},
				expectedCreateCacheController: true,
			},
			{
				name: "when_limit_over_zero_and_iterator_cache_enabled",
				cacheSettings: CacheSettings{
					CheckCacheLimit:        10,
					IteratorCacheEnabled:   true,
					CacheControllerEnabled: true,
				},
				expectedCreateCacheController: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := tt.cacheSettings.ShouldCreateCacheController()
				assert.Equal(t, tt.expectedCreateCacheController, got)
			})
		}
	})

	t.Run("should_cache_queries", func(t *testing.T) {
		tests := []struct {
			name                       string
			cacheSettings              CacheSettings
			expectedShouldCacheQueries bool
		}{
			{
				name: "not_when_limit_is_zero",
				cacheSettings: CacheSettings{
					CheckCacheLimit: 0,
				},
				expectedShouldCacheQueries: false,
			},
			{
				name: "not_when_query_cache_disabled",
				cacheSettings: CacheSettings{
					CheckQueryCacheEnabled: false,
				},
				expectedShouldCacheQueries: false,
			},
			{
				name: "when_limit_over_zero_and_query_cache_enabled",
				cacheSettings: CacheSettings{
					CheckCacheLimit:        10,
					CheckQueryCacheEnabled: true,
				},
				expectedShouldCacheQueries: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := tt.cacheSettings.ShouldCacheCheckQueries()
				assert.Equal(t, tt.expectedShouldCacheQueries, got)
			})
		}
	})

	t.Run("should_cache_iterators", func(t *testing.T) {
		tests := []struct {
			name                   string
			cacheSettings          CacheSettings
			expectedCacheIterators bool
		}{
			{
				name: "not_when_limit_is_zero",
				cacheSettings: CacheSettings{
					CheckCacheLimit: 0,
				},
				expectedCacheIterators: false,
			},
			{
				name: "not_when_iterator_cache_disabled",
				cacheSettings: CacheSettings{
					IteratorCacheEnabled: false,
				},
				expectedCacheIterators: false,
			},
			{
				name: "when_limit_over_zero_and_iterator_cache_enabled",
				cacheSettings: CacheSettings{
					CheckCacheLimit:      10,
					IteratorCacheEnabled: true,
				},
				expectedCacheIterators: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := tt.cacheSettings.ShouldCacheIterators()
				assert.Equal(t, tt.expectedCacheIterators, got)
			})
		}
	})
}
