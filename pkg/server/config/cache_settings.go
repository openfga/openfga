package config

import "time"

type CacheSettings struct {
	CheckCacheLimit         uint32
	CacheControllerEnabled  bool
	CacheControllerTTL      time.Duration
	CheckQueryCacheEnabled  bool
	CheckQueryCacheTTL      time.Duration
	IteratorCacheEnabled    bool
	IteratorCacheMaxResults uint32
	IteratorCacheTTL        time.Duration
}

func NewDefaultCacheSettings() CacheSettings {
	return CacheSettings{
		CheckCacheLimit:         DefaultCheckCacheLimit,
		CacheControllerEnabled:  DefaultCacheControllerEnabled,
		CacheControllerTTL:      DefaultCacheControllerTTL,
		CheckQueryCacheEnabled:  DefaultCheckQueryCacheEnabled,
		CheckQueryCacheTTL:      DefaultCheckQueryCacheTTL,
		IteratorCacheEnabled:    DefaultIteratorCacheEnabled,
		IteratorCacheMaxResults: DefaultIteratorCacheMaxResults,
		IteratorCacheTTL:        DefaultIteratorCacheTTL,
	}
}

func (c CacheSettings) ShouldCreateNewCache() bool {
	return c.ShouldCacheCheckQueries() || c.ShouldCacheIterators()
}

func (c CacheSettings) ShouldCreateCacheController() bool {
	return c.ShouldCreateNewCache() && c.CacheControllerEnabled
}

func (c CacheSettings) ShouldCacheCheckQueries() bool {
	return c.CheckCacheLimit > 0 && c.CheckQueryCacheEnabled
}

func (c CacheSettings) ShouldCacheIterators() bool {
	return c.CheckCacheLimit > 0 && c.IteratorCacheEnabled
}
