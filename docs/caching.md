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

The Cache Controller TTL has a critical relationship with other cache TTLs that affects invalidation behavior:

- **Purpose**: Controls how often the cache controller checks for data changes that might invalidate cached entries
- **Detection Window**: The cache controller only detects changes that occurred within its TTL window
- **Invalidation Effectiveness**: If cache controller TTL is too long relative to other caches, stale data may persist

**Important Rule**: Cache Controller TTL should be **shorter than or equal to** other cache TTLs for optimal invalidation:

```bash
# Good configuration - Controller checks more frequently than cache expiry
export OPENFGA_CACHE_CONTROLLER_TTL=10s           # Controller checks every 10s
export OPENFGA_CHECK_QUERY_CACHE_TTL=30s          # Query cache expires after 30s
export OPENFGA_CHECK_ITERATOR_CACHE_TTL=60s       # Iterator cache expires after 60s

# Problematic configuration - Controller checks less frequently
export OPENFGA_CACHE_CONTROLLER_TTL=60s           # Controller checks every 60s
export OPENFGA_CHECK_QUERY_CACHE_TTL=10s          # Query cache expires after 10s (too short!)
```

**Why this relationship matters**:

1. **Change Detection**: The controller only looks for changes within its TTL window
2. **Stale Data Risk**: If controller TTL > cache TTL, changes might be missed during the cache's lifetime
3. **Invalidation Lag**: Longer controller TTL means longer time before invalidation occurs

## Recommended Configuration

```bash

# This configures the actual cache TTLs. If tuples are written, the other caches will be invalidated.
export OPENFGA_CACHE_CONTROLLER_ENABLED=true
export OPENFGA_CACHE_CONTROLLER_TTL=10s

# Enable Check query caching
export OPENFGA_CHECK_QUERY_CACHE_ENABLED=true
export OPENFGA_CHECK_CACHE_LIMIT=25000
export OPENFGA_CHECK_QUERY_CACHE_TTL=30s

# Enable ListObjects query caching
export OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_ENABLED=true
export OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_MAX_RESULTS=25000
export OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_TTL=45s
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

### Request Logging

Cached requests include additional context in logs:

```json
{
  "level": "info",
  "msg": "Check request completed",
  "request_id": "...",
  "store_id": "...",
  "cache_hit": true,
  "cache_type": "check_query",
  "response_time_ms": 2
}
```

## Performance Tuning Guidelines

### Cache Hit Rate Optimization

1. **Monitor hit rates**: Aim for >80% hit rate for query caches, >60% for iterator caches
2. **Adjust TTL**: Longer TTL = higher hit rates but potentially stale data
3. **Increase cache size**: More items can be cached, improving hit rates

### Memory Usage Optimization

1. **Right-size cache limits**: Based on available memory and usage patterns
2. **Monitor eviction rates**: High evictions indicate undersized caches
3. **Balance cache types**: Allocate memory across different cache types based on workload
4. **Use iterator result limits**: Prevent large iterators from consuming excessive memory

#### TTL Relationship Rules

1. **Controller First**: Set Cache Controller TTL based on how quickly you need to detect changes
2. **Maximize Cache TTLs**: With Cache Controller enabled, set cache TTLs as high as practical for maximum performance
3. **Balance Performance vs Freshness**: With Cache Controller enabled, **only Controller TTL affects staleness** - longer cache TTLs improve performance without affecting data freshness

**Important**: When Cache Controller is enabled, individual cache TTLs only affect performance (cache hit rates), not data staleness. The Cache Controller will proactively invalidate stale entries regardless of their TTL, so you can safely use **very long cache TTLs** (hours or even days) for maximum performance without sacrificing data consistency.

**Without Cache Controller**: Cache TTLs directly control staleness, so keep them shorter (30s-300s) to balance performance and freshness.

## Best Practices

### Development and Testing

1. **Start with defaults**: Begin with conservative cache settings
2. **Enable monitoring**: Always enable metrics collection
3. **Test cache behavior**: Verify cache hits and invalidation work as expected

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

### Security Considerations

1. **Cache isolation**: Caches are isolated per store by design
2. **No cross-store leakage**: Cache keys include store ID to prevent data leaks
3. **Memory protection**: Cached data is encrypted in memory if using encrypted storage
4. **Audit logging**: Cache hits/misses are included in audit logs

## Troubleshooting

### Common Issues

**Low Cache Hit Rates**
- **Symptoms**: High response times, increased database load
- **Causes**: Cache size too small, TTL too short, highly dynamic data
- **Solutions**: Increase cache limits, extend TTL, analyze access patterns

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
3. **Analyze Logs**: Look for cache-related log entries
4. **Test Cache Behavior**: Use identical requests to verify caching
5. **Review Memory Usage**: Ensure adequate memory allocation


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
