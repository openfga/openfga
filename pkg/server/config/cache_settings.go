package config

import (
	"time"
)

type CacheSettings struct {
	CheckCacheLimit                    uint32
	CacheControllerEnabled             bool
	CacheControllerTTL                 time.Duration
	CheckQueryCacheEnabled             bool
	CheckQueryCacheTTL                 time.Duration
	CheckIteratorCacheEnabled          bool
	CheckIteratorCacheMaxResults       uint32
	CheckIteratorCacheTTL              time.Duration
	ListObjectsIteratorCacheEnabled    bool
	ListObjectsIteratorCacheMaxResults uint32
	ListObjectsIteratorCacheTTL        time.Duration
	SharedIteratorEnabled              bool
	SharedIteratorLimit                uint32
	SharedIteratorTTL                  time.Duration
	ShadowCheckCacheEnabled            bool
}

func NewDefaultCacheSettings() CacheSettings {
	return CacheSettings{
		CheckCacheLimit:                    DefaultCheckCacheLimit,
		CacheControllerEnabled:             DefaultCacheControllerEnabled,
		CacheControllerTTL:                 DefaultCacheControllerTTL,
		CheckQueryCacheEnabled:             DefaultCheckQueryCacheEnabled,
		CheckQueryCacheTTL:                 DefaultCheckQueryCacheTTL,
		CheckIteratorCacheEnabled:          DefaultCheckIteratorCacheEnabled,
		CheckIteratorCacheMaxResults:       DefaultCheckIteratorCacheMaxResults,
		CheckIteratorCacheTTL:              DefaultCheckIteratorCacheTTL,
		ListObjectsIteratorCacheEnabled:    DefaultListObjectsIteratorCacheEnabled,
		ListObjectsIteratorCacheMaxResults: DefaultListObjectsIteratorCacheMaxResults,
		ListObjectsIteratorCacheTTL:        DefaultListObjectsIteratorCacheTTL,
		SharedIteratorEnabled:              DefaultSharedIteratorEnabled,
		SharedIteratorLimit:                DefaultSharedIteratorLimit,
		SharedIteratorTTL:                  DefaultSharedIteratorTTL,
		ShadowCheckCacheEnabled:            DefaultShadowCheckCacheEnabled,
	}
}

func (c CacheSettings) ShouldCreateNewCache() bool {
	return c.ShouldCacheCheckQueries() || c.ShouldCacheCheckIterators() || c.ShouldCacheListObjectsIterators()
}

func (c CacheSettings) ShouldCreateCacheController() bool {
	return c.ShouldCreateNewCache() && c.CacheControllerEnabled
}

func (c CacheSettings) ShouldCacheCheckQueries() bool {
	return c.CheckCacheLimit > 0 && c.CheckQueryCacheEnabled
}

func (c CacheSettings) ShouldCacheCheckIterators() bool {
	return c.CheckCacheLimit > 0 && c.CheckIteratorCacheEnabled
}

func (c CacheSettings) ShouldCacheListObjectsIterators() bool {
	return c.ListObjectsIteratorCacheEnabled && c.ListObjectsIteratorCacheMaxResults > 0
}

func (c CacheSettings) ShouldCreateShadowNewCache() bool {
	return c.ShadowCheckCacheEnabled && c.ShouldCreateNewCache()
}
