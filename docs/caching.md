# Caching in OpenFGA

OpenFGA provides a sophisticated multi-layered caching system designed to improve performance by reducing database queries and computation overhead. This document explains how caching works in OpenFGA and how to configure it for optimal performance in your deployment.

## Overview

OpenFGA implements several complementary types of caching:

1. **Check Query Cache**: Caches results of Check API requests to avoid recomputing identical authorization checks
2. **Check Iterator Cache**: Caches database query results (iterators) to reduce database load during Check operations
3. **ListObjects Iterator Cache**: Caches database iterators specifically for ListObjects API calls
4. **Cache Controller**: Manages cache invalidation based on data changes to maintain consistency. It works in background, and it's not involved in the query resolution. After each TTL period, it checks the changelog, and invalidates the cache if tuples were written.

## Understanding Cache Types

### Check Query Cache

The Check Query Cache stores the complete results of Check API requests, allowing identical authorization checks to return immediately without any computation.

- **What it caches**: Complete Check responses including the final allow/deny decision
- **Cache key**: Generated from store ID, authorization model ID, tuple key, contextual tuples, and other request parameters
- **Benefits**: Eliminates computation for repeated identical checks
- **Use case**: High-frequency identical authorization checks

### Check Iterator Cache

The Check Iterator Cache stores database query results (tuple iterators) used during Check operations, reducing the number of database queries required.

- **What it caches**: Raw tuple query results from the database
- **Cache key**: Based on store ID, object type, relation, and user patterns
- **Benefits**: Reduces database load, especially for complex relationship traversals
- **Use case**: Complex authorization models with deep hierarchies

### ListObjects Iterator Cache

Similar to Check Iterator Cache but specifically optimized for ListObjects operations, which can be particularly database-intensive.

- **What it caches**: Database iterator results for ListObjects queries
- **Cache key**: Based on store ID, user, relation, object type, and contextual tuples
- **Benefits**: Dramatically improves ListObjects performance
- **Use case**: Frequent ListObjects calls with similar parameters

### Cache Controller

The Cache Controller manages cache invalidation by monitoring data changes and proactively invalidating stale cache entries.

- **How it works**: Monitors changelog for tuple writes/deletes and invalidates affected cache entries
- **Invalidation strategy**: Asynchronous, eventually consistent
- **Benefits**: Maintains cache accuracy while preserving performance
- **Use case**: Environments with frequent data changes

## Configuration Options

### Global Check Cache Settings

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_CHECK_CACHE_LIMIT` | `--check-cache-limit` | `10000` | Maximum number of items in the check cache |

### Check Query Cache

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_CHECK_QUERY_CACHE_ENABLED` | `--check-query-cache-enabled` | `false` | Enable caching of Check query results |
| `OPENFGA_CHECK_QUERY_CACHE_TTL` | `--check-query-cache-ttl` | `10s` | Time-to-live for cached Check results |

### Check Iterator Cache

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_CHECK_ITERATOR_CACHE_ENABLED` | `--check-iterator-cache-enabled` | `false` | Enable caching of database iterators for Check operations |
| `OPENFGA_CHECK_ITERATOR_CACHE_MAX_RESULTS` | `--check-iterator-cache-max-results` | `10000` | Maximum number of results to cache per iterator |
| `OPENFGA_CHECK_ITERATOR_CACHE_TTL` | `--check-iterator-cache-ttl` | `10s` | Time-to-live for cached iterators |

### ListObjects Iterator Cache

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_ENABLED` | `--list-objects-iterator-cache-enabled` | `false` | Enable caching of database iterators for ListObjects operations |
| `OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_MAX_RESULTS` | `--list-objects-iterator-cache-max-results` | `10000` | Maximum number of results to cache per iterator |
| `OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_TTL` | `--list-objects-iterator-cache-ttl` | `10s` | Time-to-live for cached ListObjects iterators |

### Cache Controller

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_CACHE_CONTROLLER_ENABLED` | `--cache-controller-enabled` | `false` | Enable automatic cache invalidation based on data changes |
| `OPENFGA_CACHE_CONTROLLER_TTL` | `--cache-controller-ttl` | `10s` | TTL for changelog cache entries used for invalidation detection |

#### Cache Controller

With the Cache Controller enabled, the controller TTL sets the maximum staleness window. Check/ListObjects cache TTLs primarily affect hit rate and memory usage, not freshness. Choose:

- Controller TTL ≤ your maximum acceptable staleness (e.g., 10–60s).
- Cache TTLs as long as your memory budget and workload allow.

## Recommended Configuration

```bash

# This configures the actual cache TTLs. If tuples are written, the other caches will be invalidated.
export OPENFGA_CACHE_CONTROLLER_ENABLED=true
export OPENFGA_CACHE_CONTROLLER_TTL=10s

# Enable Check query caching
export OPENFGA_CHECK_QUERY_CACHE_ENABLED=true
export OPENFGA_CHECK_CACHE_LIMIT=25000
export OPENFGA_CHECK_QUERY_CACHE_TTL=3600s

# Enable ListObjects query caching
export OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_ENABLED=true
export OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_MAX_RESULTS=25000
export OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_TTL=3600s
```

## Cache Invalidation and Consistency

### Automatic Cache Invalidation

When the Cache Controller is enabled, OpenFGA automatically invalidates cache entries based on data changes:

1. **Change Detection**: Monitors the changelog table for tuple writes/deletes
2. **Selective Invalidation**: Only invalidates cache entries affected by changes
3. **Asynchronous Processing**: Invalidation happens in the background to minimize latency
4. **Eventually Consistent**: Cache becomes consistent within the TTL window

### Invalidation Patterns

The Cache Controller uses these invalidation patterns:

```go
// Example of cache key patterns that get invalidated
"object:document:123:relation:viewer" // Specific object-relation cache
"user:alice:*" // All cache entries for user alice
"*" // Global invalidation for store (on major changes)
```

## Monitoring and Observability

### Cache Metrics

OpenFGA provides comprehensive metrics for cache monitoring:

#### Query Cache Metrics

- `openfga_check_cache_total_count`: Total number of cache lookups
- `openfga_check_cache_hit_count`: Number of successful cache hits
- `openfga_check_cache_invalid_hit_count`: Cache hits that were invalidated

#### Iterator Cache Metrics

- `openfga_tuples_cache_total_count`: Total iterator cache operations
- `openfga_tuples_cache_hit_count`: Iterator cache hits
- `openfga_tuples_cache_discard_count`: Discarded cache entries
- `openfga_tuples_cache_size`: Size distribution of cached iterators

#### General Cache Metrics

- `openfga_cache_item_count`: Number of items in cache by entity type
- `openfga_cache_item_removed_count`: Cache evictions by reason and entity type

#### Cache Controller Metrics

- `openfga_cachecontroller_cache_total_count`: Total cache controller operations
- `openfga_cachecontroller_cache_hit_count`: Cache controller cache hits
- `openfga_cachecontroller_cache_invalidation_count`: Number of invalidations performed
- `openfga_cachecontroller_invalidation_duration_ms`: Time spent on invalidation operations

## Best Practices

### Production Deployment

1. **Gradual rollout**: Enable caching incrementally, starting with query cache
2. **Monitor memory usage**: Track cache memory consumption
3. **Set up alerting**: Alert on low cache hit rates or high eviction rates
4. **Regular tuning**: Periodically review and adjust cache settings based on metrics

### Cache Sizing

1. **Memory allocation**: Reserve 10-30% of available memory for caches
2. **Distribution**: 
   - Query cache: 60-70% of cache memory
   - Iterator caches: 30-40% of cache memory
3. **Scaling**: Increase cache sizes proportionally with traffic growth

## Troubleshooting

### Common Issues

**Low Cache Hit Rates**
- **Symptoms**: High response times, increased database load
- **Causes**: Cache size too small, Cache TTLs too short, highly dynamic data
- **Solutions**: Increase cache limits, extend TTLs

**Memory Issues**
- **Symptoms**: Out of memory errors, high memory usage
- **Causes**: Cache limits set too high, large iterator results
- **Solutions**: Reduce cache limits, lower iterator max results, add memory

**Cache Inconsistency**
- **Symptoms**: Stale data returned, authorization errors
- **Causes**: Cache Controller disabled, long TTL, invalidation issues
- **Solutions**: Enable Cache Controller, reduce TTL, restart server

**High Cache Eviction Rates**
- **Symptoms**: Frequent cache misses despite high traffic
- **Causes**: Cache too small for working set size
- **Solutions**: Increase cache limits, analyze access patterns, optimize TTL

### Debugging Steps

1. **Check Configuration**: Verify environment variables and CLI flags
2. **Monitor Metrics**: Review cache hit rates and eviction counts
3. **Test Cache Behavior**: Use identical requests to verify caching
4. **Review Memory Usage**: Ensure adequate memory allocation

## Cache Architecture Details

### Cache Implementation

OpenFGA uses the [Theine](https://github.com/Yiling-J/theine-go) library for in-memory caching:

- **Eviction Policy**: Least Recently Used (LRU) with W-TinyLFU admission policy
- **Thread Safety**: Fully concurrent with minimal locking
- **Memory Efficiency**: Optimized memory layout and garbage collection friendly
- **Performance**: High-performance cache operations with sub-microsecond latencies

### Cache Key Generation

Cache keys are generated using a combination of:

```go
// Example cache key structure
"store:{storeID}:model:{modelID}:object:{object}:relation:{relation}:user:{user}:contextual:{hash}"
```

- **Deterministic**: Identical requests generate identical keys
- **Collision-resistant**: Uses xxHash for contextual tuple hashing
- **Hierarchical**: Supports pattern-based invalidation

### Integration with OpenFGA Components

The caching system integrates deeply with OpenFGA's architecture, with the Cache Controller playing a central role in maintaining data consistency:

- **Resolver Layer**: Check operations use CachedCheckResolver for query caching
- **Storage Layer**: Database operations use CachedDatastore wrappers for iterator caching  
- **Controller Layer**: CacheController manages proactive invalidation based on data changes
- **Metrics Layer**: Comprehensive observability throughout all cache interactions

This multi-layered approach ensures optimal performance while maintaining data consistency through intelligent invalidation.
