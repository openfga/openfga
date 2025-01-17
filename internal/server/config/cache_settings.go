package config

import "time"

type CacheSettings struct {
	CheckCacheLimit              uint32
	CacheInvalidatorEnabled      bool
	CacheInvalidatorTTL          time.Duration
	CheckQueryCacheEnabled       bool
	CheckQueryCacheTTL           time.Duration
	CheckIteratorCacheEnabled    bool
	CheckIteratorCacheMaxResults uint32
	CheckIteratorCacheTTL        time.Duration
}

func NewDefaultCacheSettings() CacheSettings {
	return CacheSettings{
		CheckCacheLimit:              DefaultCheckCacheLimit,
		CacheInvalidatorEnabled:      DefaultCacheInvalidatorEnabled,
		CacheInvalidatorTTL:          DefaultCacheInvalidatorTTL,
		CheckQueryCacheEnabled:       DefaultCheckQueryCacheEnabled,
		CheckQueryCacheTTL:           DefaultCheckQueryCacheTTL,
		CheckIteratorCacheEnabled:    DefaultCheckIteratorCacheEnabled,
		CheckIteratorCacheMaxResults: DefaultCheckIteratorCacheMaxResults,
		CheckIteratorCacheTTL:        DefaultCheckIteratorCacheTTL,
	}
}

func (c CacheSettings) ShouldCreateNewCache() bool {
	return c.ShouldCacheCheckQueries() || c.ShouldCacheIterators()
}

func (c CacheSettings) ShouldCreateCacheInvalidator() bool {
	return c.ShouldCreateNewCache() && c.CacheInvalidatorEnabled
}

func (c CacheSettings) ShouldCacheCheckQueries() bool {
	return c.CheckCacheLimit > 0 && c.CheckQueryCacheEnabled
}

func (c CacheSettings) ShouldCacheIterators() bool {
	return c.CheckCacheLimit > 0 && c.CheckIteratorCacheEnabled
}
