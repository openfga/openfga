// Package metrics holds Prometheus counters shared across the v1 and v2
// Check cache implementations so both paths report into the same metric names.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
)

var (
	CacheLookupCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "check_cache_total_count",
		Help:      "The total number of Check cache lookups (excluding HIGHER_CONSISTENCY requests).",
	})

	CacheHitCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "check_cache_hit_count",
		Help:      "The total number of valid Check Query cache hits.",
	})

	CacheInvalidHitCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "check_cache_invalid_hit_count",
		Help:      "The total number of Check Query cache hits discarded because they were invalidated.",
	})
)
