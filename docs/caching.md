# Caching in OpenFGA

## Overview

OpenFGA implements several complementary types of caching:

1. [**Check Query Cache**](#check-query-cache): Caches results of sub-problems within Check API requests to avoid recomputing them.
2. [**Check Iterator Cache**](#check-iterator-cache): Caches database query results (iterators) to reduce database load during Check operations.
3. [**List Objects Iterator Cache**](#list-objects-iterator-cache): Same as Check Iterator Cache, but used for List Objects requests.
4. [**Cache Controller**](#cache-controller): Periodically invalidates cache entries in the background based on recent writes to the store.

**NOTE:**

- For any request, if the `HIGHER_CONSISTENCY` consistency preference is specified, caching is bypassed entirely.
- The cache is in-memory, so different replicas of the service do not share their caches. Therefore, its effectiveness depends on the probability of repeated/similar requests hitting the same replica, which may depend on the model, tuple distribution, number of replicas, and the load balancing algorithm used.

## Understanding Cache Types

### Check Query Cache

- **What it caches**: Authorization decisions (allow/deny) for the overall Check request as well as for any intermediate ("sub-problem") Checks.
- **Benefits**: Eliminates computation for repeated identical Checks or different Checks that share common relationship evaluations.

#### Example

Consider the [Entitlements sample store](https://github.com/openfga/sample-stores/tree/79fa8c1710f12d3f0befaba77f15463bb5a97860/stores/entitlements) and OpenFGA running with check query cache enabled. Given the query "does user `user:charles` have relationship `can_access` with `feature:draft_prs`?", the following cache entries (simplified) are set:

1. (Final result) Key: does user `user:charles` have relationship `can_access` with object `feature:draft_prs`? Value: `true`.
2. (Sub-problem) Key: does user `user:charles` have relationship `subscriber_member` with object `plan:enterprise`? Value: `true`.

### Check Iterator Cache

The Check Iterator Cache stores database query results (tuple iterators) used during Check operations, reducing the number of database queries required.

- **What it caches**: Raw tuple query results from the database.
- **Benefits**: Reduces database load by caching some of its results in-memory.

The maximum number of tuples stored for each iterator cache entry can be [configured](https://openfga.dev/docs/getting-started/setup-openfga/configuration).

#### Example

Consider the [Entitlements sample store](https://github.com/openfga/sample-stores/tree/79fa8c1710f12d3f0befaba77f15463bb5a97860/stores/entitlements) and OpenFGA running with check iterator cache enabled. Given the query "does user `user:charles` have relationship `can_access` with `feature:draft_prs`?", the following cache entries (simplified) are set:

1. Key: What users have relationship `associated_plan` with object `feature:draft_prs`? Value: `[plan:enterprise, plan:team]`.
2. Key: What users have relationship `subscriber` with object `plan:enterprise`? Value: `[organization:cups]`.
3. Key: What are the tuples where user `user:charles` has relationship `member` with objects of type `organization`? Value: `[organization:cups#member@user:charles]`.

### List Objects Iterator Cache

Same as the Check Iterator Cache, but used in List Objects.

Note: If this is enabled along with the [cache controller](#cache-controller), the cache controller will trigger invalidation on every List Objects request.

## Cache Controller

- **What it does**: Invalidates stale cache entries based on recent writes to the store.
- **Benefits**: Allows using longer TTLs for cache entries to increase hit rate.

The cache controller periodically checks the store's changelog and compares recent tuple writes/deletes to the time cache entries were set. When the cache controller sees new writes to the store, all previous Check query cache entries are invalidated, while only relevant Check iterator cache entries are invalidated based on what tuples the writes affected.

Invalidation runs asynchronously (eventually consistent) and is triggered on:

- Any Check requests, but not more than once every cache controller TTL.
- Every List Objects request if List Objects iterator cache is enabled, irrespective of cache controller TTL; however, only one invalidation can be running at a time.

With the cache controller enabled, its TTL sets the staleness window for cache. Check and List Objects cache TTLs will now only affect hit rate and memory usage, not freshness. Choose:

- Controller TTL ≤ your acceptable staleness (e.g., 10s).
- Cache TTLs as long as your memory budget and workload allow.

Note: "invalidation" is done by setting new cache entries which are checked before using a regular cache entry. Thus, the number of cache removals (eviction/expiration/deletion) in the metrics may be inflated.

### Example

Consider a single replica OpenFGA server with a cache controller TTL of 10s and check query, check iterator, list objects iterator cache TTLs of 300s:

Time | Event | Result | Notes | Invalidation
-----|-------|--------|-------|-------------
t=0s | Check 1 | Returns `allowed: true` | Computes result from database and caches locally | Triggered. Invalidation will not be triggered again by Check until the cache controller TTL has passed (t=10s).
t=0s + ε | Invalidation from Check 1 complete | Nothing to invalidate | Sees cache entries were set at t=0s which is after the latest write at t<0s, so nothing to invalidate |
t=1s | Write: Tuple deleted | Store updated | Previous Check is now invalid |
t=5s | Check 2 | Returns `allowed: true` (stale) | Uses local cache | Not triggered as the cache controller TTL hasn't passed since the last invalidation from Check.
t=10s | Check 3 | " | " | Triggered. Invalidation will not be triggered again by Check until the cache controller TTL has passed (t=20s).
t=10s + ε | Invalidation from Check 3 complete | Invalidates cache | Sees cache entries were set at t=0s which is before the latest write at t=1s, so invalidates cache |
t=11s | Check 4 | Returns `allowed: false` | Finds local cache entries but they are invalid, so computes result from database and sets new local cache entries | Not triggered as the cache controller TTL hasn't passed since the last invalidation from Check.
t=20s | Check 5 | Returns `allowed: false` | Uses local cache | Triggered. Invalidation will not be triggered again by Check until the cache controller TTL has passed (t=30s).
t=20s + ε | Invalidation from Check 5 complete | Nothing to invalidate | Sees cache entries were set at t=11s which is after the latest write at t=1s, so nothing to invalidate |
t=310s | Check 6 | Returns `allowed: false` | Uses local cache | Triggered. Invalidation will not be triggered again by Check until the cache controller TTL has passed (t=320s).
t=310s + ε | Invalidation from Check 6 complete | Nothing to invalidate | Sees cache entries were set at t=11s which is after the latest write at t=1s, so nothing to invalidate |
t=311s | Check 7 | Returns `allowed: false` | Local cache has finally expired, so computes result from database and caches locally | Not triggered as the cache controller TTL hasn't passed since the last invalidation from Check.
t=312s | Write: Tuple added | Store updated | Previous Check is now invalid |
t=313s | List Objects 1 | Returns stale results | Uses iterator cache entries from previous Check | Triggered.
t=313s + ε | Invalidation from List Objects 1 complete | Invalidates cache | Sees cache entries were set at t=310s which is before the latest write at t=312s, so invalidates cache |
t=314s | Check 8 | Returns `allowed: true` | Finds local cache entries but they are invalid, so computes result from database and sets new local cache entries | Not triggered as the cache controller TTL hasn't passed since the last invalidation from Check.

Notice:
- Stale data was only returned for up to the cache controller TTL after a write.
- When there was no write (between Check 4-6), the local cache was able to be used for a longer time.
- List Objects 1 triggered invalidation even though the cache controller TTL hasn't passed yet.

### Caveats

#### First Check Staleness

Because invalidation is triggered asynchronously by Check and List Objects requests, there is an accepted race condition where the first Check after a write (and subsequent Checks until the async job finishes) could return stale data if its Check response was cached before the write, even if it has been more than the cache controller TTL since the write.

For example, if we look at the previous example, if Check 3 had occurred at t=100s, it would have still returned a stale result since it triggers invalidation asynchronously but returns immediately. The next Check, however, would see the results of the invalidation and compute a fresh result.

Note that *any* Check request (if the cache controller TTL has passed since the last invalidation) or List Objects request (if list objects iterator cache is enabled) will trigger invalidation for the entire store, so this issue only occurs with very infrequent requests.

## Best Practices

### Enablement

The different caches must explicitly enabled - see [OpenFGA Configuration Options](https://openfga.dev/docs/getting-started/setup-openfga/configuration) for more info.

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
- **Causes**: Cache Controller disabled with long cache TTLs, code bugs
- **Solutions**: Enable Cache Controller, reduce TTL, restart server, reach out to OpenFGA team

**High Cache Eviction Rates**
- **Symptoms**: Frequent cache misses despite high traffic
- **Causes**: Cache too small for working set size
- **Solutions**: Increase cache limits, analyze access patterns, optimize TTL

### Debugging Steps

1. **Check Configuration**: Verify environment variables and CLI flags
2. **Monitor Metrics**: Review cache hit rates and eviction counts
3. **Test Cache Behavior**: Use identical requests with different timings to verify caching
4. **Review Memory Usage**: Ensure adequate memory allocation

## Cache Implementation

OpenFGA uses the [Theine](https://github.com/Yiling-J/theine-go) library for in-memory caching:

- **Eviction Policy**: Least Recently Used (LRU) with W-TinyLFU admission policy
- **Thread Safety**: Fully concurrent with minimal locking
- **Memory Efficiency**: Optimized memory layout and garbage collection friendly
- **Performance**: High-performance cache operations with sub-microsecond latencies
