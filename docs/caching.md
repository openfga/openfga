# Caching in OpenFGA

## Overview

OpenFGA implements several complementary types of caching:

1. [**Check Query Cache**](#check-query-cache): Caches results of sub-problems within Check API requests to avoid recomputing them.
2. [**Check Iterator Cache**](#check-iterator-cache): Caches database query results (iterators) to reduce database load during Check operations.
3. [**List Objects Iterator Cache**](#list-objects-iterator-cache): Same as Check Iterator Cache, but used for List Objects requests.
4. [**Cache Controller**](#cache-controller): Periodically invalidates cache entries in the background based on recent writes to the store.
5. [**Authorization Model & Typesystem Cache**](#authorization-model--typesystem-cache): Always-on caches for authorization models and their compiled typesystems.

**NOTE:**

- For any request, if the `HIGHER_CONSISTENCY` consistency preference is specified, caching is bypassed entirely (except for the authorization model and typesystem caches, which are always active).
- The cache is in-memory, so different replicas of the service do not share their caches. Therefore, its effectiveness depends on the probability of repeated/similar requests hitting the same replica, which may depend on the model, tuple distribution, number of replicas, and the load balancing algorithm used.

**Cache instances:** There are three independent in-memory cache instances:

- **Shared check cache**: backs the query cache, iterator caches, and cache controller. Sized by `OPENFGA_CHECK_CACHE_LIMIT`. This is the cache referenced by the caching flags in the sections below.
- **Authorization model cache**: caches validated authorization models. Always on; TTL 7 days; sized by `OPENFGA_DATASTORE_MAX_CACHE_SIZE`.
- **Typesystem cache**: caches compiled typesystems derived from authorization models. Always on; TTL 7 days; sized by `OPENFGA_DATASTORE_MAX_TYPESYSTEM_CACHE_SIZE`.

The model and typesystem caches use long TTLs because authorization models are **immutable**: a model update always produces a new model ID, which maps to a new cache key, so existing entries never go stale.

## Understanding Cache Types

### Check Query Cache

- **What it caches**: Authorization decisions (allow/deny) for the overall Check request as well as for any intermediate ("sub-problem") Checks.
- **Benefits**: Eliminates computation for repeated identical Checks or different Checks that share common relationship evaluations.
- **Cache key**: `SP + storeID + object + relation + user + invariantHash`
  - `invariantHash` is a hash of `(storeID, modelID, contextual tuples, context parameters)` - the parts of a Check request that don't vary across sub-problems but do influence results.
  - `storeID` appears in both the key itself and inside `invariantHash` as defense-in-depth: cache correctness does not depend on store IDs being globally unique across database restores or environment imports.
  - The actual stored key is an opaque binary sequence (TLV-encoded, hex-rendered) - not a human-readable string.
  - Essentially, "does `<user>` have `<relation>` with `<object>` for `<storeID>` & `<modelID>` given these contextual tuples and these context parameters?"
  - Note: this takes contextual tuples & context parameters into account (since they may have influenced the allow/deny decision).

#### Example

Consider the [Entitlements sample store](https://github.com/openfga/sample-stores/tree/79fa8c1710f12d3f0befaba77f15463bb5a97860/stores/entitlements) and OpenFGA running with check query cache enabled. Given the query "does user `user:charles` have relationship `can_access` with `feature:draft_prs`?", the following cache entries (simplified) are set:

1. (Final result) Key: does user `user:charles` have relationship `can_access` with object `feature:draft_prs`? Value: `true`.
2. (Sub-problem) Key: does user `user:charles` have relationship `subscriber_member` with object `plan:enterprise`? Value: `true`.

#### Configuration

| Config File | Env Var | Flag Name | Type | Description | Default Value |
|-------------|---------|-----------|------|-------------|---------------|
| `checkCache.limit` | <div id="OPENFGA_CHECK_CACHE_LIMIT"><code>OPENFGA_CHECK_CACHE_LIMIT</code></div> | `check-cache-limit` | integer | the size limit (in items) of the cache for Check (queries and iterators) | `10000` |
| `checkQueryCache.enabled` | <div id="OPENFGA_CHECK_QUERY_CACHE_ENABLED"><code>OPENFGA_CHECK_QUERY_CACHE_ENABLED</code></div> | `check-query-cache-enabled` | boolean | enable caching of Check requests. The key is a string representing a query, and the value is a boolean. For example, if you have a relation `define viewer: owner or editor`, and the query is Check(user:anne, viewer, doc:1), we'll evaluate the `owner` relation and the `editor` relation and cache both results: (user:anne, viewer, doc:1) -> allowed=true and (user:anne, owner, doc:1) -> allowed=true. The cache is stored in-memory; the cached values are overwritten on every change in the result, and cleared after the configured TTL. This flag improves latency, but turns Check and ListObjects into eventually consistent APIs. If the request's consistency is HIGHER_CONSISTENCY, this cache is not used. | `false` |
| `checkQueryCache.ttl` | <div id="OPENFGA_CHECK_QUERY_CACHE_TTL"><code>OPENFGA_CHECK_QUERY_CACHE_TTL</code></div> | `check-query-cache-ttl` | string (duration) | if caching of Check and ListObjects is enabled, this is the TTL of each value | `10s` |

### Check Iterator Cache

The Check Iterator Cache stores database query results (tuple iterators) used during Check operations, reducing the number of database queries required.

- **What it caches**: Raw tuple query results from the database.
- **Benefits**: Reduces database load by caching some of its results in-memory.

Note: Iterator cache entries only cache DB results. Therefore, they do not take contextual tuples or context parameters provided in the request into account (since these are not stored in the DB). They do, however, take tuples with conditions into account (since the condition and any context is stored alongside the tuple).

Specifically, there are 3 types of iterator cache entries:

1. Read (`IC + "READ"`)
   - **What it caches**: Direct tuple lookups for a specific object and relation, optionally filtered by user.
   - **Cache key**: `IC + "READ" + storeID + object + relation + user + hash(conditions)`
     - E.g., the logical key for "all subscriber tuples on plan:enterprise" is `IC / READ / <storeID> / plan:enterprise / subscriber / (no user filter) / hash(no conditions)`
2. Read Starting With User (`IC + "RSWU"`)
   - **What it caches**: Reverse lookups that find all tuples where a specific user appears as the subject.
   - **Cache key**: `IC + "RSWU" + storeID + objectType + relation + hash(userFilter, objectIDs, conditions)`
     - E.g., the logical key for "all organization#member tuples for user:charles" is `IC / RSWU / <storeID> / organization / member / hash(user:charles)`
3. Read Userset Tuples (`IC + "RUT"`)
   - **What it caches**: Tuples where the user field is a userset (e.g., `organization:acme#member`), used when resolving relations that directly accept usersets as assignable types.
   - **Cache key**: `IC + "RUT" + storeID + object + relation + hash(allowedUserTypeRestrictions, conditions)`
     - E.g., the logical key for "all viewer tuples on document:report that are of type organization#member" is `IC / RUT / <storeID> / document:report / viewer / hash(organization#member)`

The actual stored key for all three types is an opaque TLV-encoded binary sequence (hex-rendered), not a human-readable string.

#### Why these key fields?

Iterator keys include only the fields that determine what the database query returns:

- `storeID`, `object`/`objectType`, `relation`, and `user` are structural fields - they appear in the clear because the invalidation logic needs to reconstruct them from a tuple write without knowing what queries were previously run.
- Variable-length filter components (conditions, user-type restrictions, object-IDs) are folded into a hashed suffix. They narrow the query further but are not needed by the invalidation path.
- Contextual tuples and request context are intentionally excluded - they are request-time parameters not stored in the DB, so DB results are the same regardless of those inputs.

#### Configuration

| Config File | Env Var | Flag Name | Type | Description | Default Value |
|-------------|---------|-----------|------|-------------|---------------|
| `checkCache.limit` | <div id="OPENFGA_CHECK_CACHE_LIMIT"><code>OPENFGA_CHECK_CACHE_LIMIT</code></div> | `check-cache-limit` | integer | the size limit (in items) of the cache for Check (queries and iterators) | `10000` |
| `checkIteratorCache.enabled` | <div id="OPENFGA_CHECK_ITERATOR_CACHE_ENABLED"><code>OPENFGA_CHECK_ITERATOR_CACHE_ENABLED</code></div> | `check-iterator-cache-enabled` | boolean | enable caching of datastore iterators. The key is a string representing a database query, and the value is a list of tuples. Each iterator is the result of a database query, for example usersets related to a specific object, or objects related to a specific user, up to a certain number of tuples per iterator. If the request's consistency is HIGHER_CONSISTENCY, this cache is not used. | `false` |
| `checkIteratorCache.maxResults` | <div id="OPENFGA_CHECK_ITERATOR_CACHE_MAX_RESULTS"><code>OPENFGA_CHECK_ITERATOR_CACHE_MAX_RESULTS</code></div> | `check-iterator-cache-max-results` | integer | if caching of datastore iterators of Check requests is enabled, this is the limit of tuples to cache per key | `10000` |
| `checkIteratorCache.ttl` | <div id="OPENFGA_CHECK_ITERATOR_CACHE_TTL"><code>OPENFGA_CHECK_ITERATOR_CACHE_TTL</code></div> | `check-iterator-cache-ttl` | string (duration) | if caching of datastore iterators of Check requests is enabled, this is the TTL of each value | `10s` |

#### Example

Consider the [Entitlements sample store](https://github.com/openfga/sample-stores/tree/79fa8c1710f12d3f0befaba77f15463bb5a97860/stores/entitlements) and OpenFGA running with check iterator cache enabled. Given the query "does user `user:charles` have relationship `can_access` with `feature:draft_prs`?", the following cache entries (simplified) are set:

1. Key: (`IC/READ`) What users have relationship `associated_plan` with object `feature:draft_prs`? Value: `[plan:enterprise, plan:team]`.
2. Key: (`IC/READ`) What users have relationship `subscriber` with object `plan:enterprise`? Value: `[organization:cups]`.
3. Key: (`IC/RSWU`) What are the tuples where user `user:charles` has relationship `member` with objects of type `organization`? Value: `[organization:cups#member@user:charles]`.

### List Objects Iterator Cache

Same as the Check Iterator Cache (i.e., they share the same cache entries), but used in List Objects.

Note: If this is enabled along with the [cache controller](#cache-controller), the cache controller will trigger invalidation on every List Objects request.

#### Interaction between Check and List Objects iterator cache config

Because Check and List Objects share the same underlying cache entries (the cache key has no API-name component), the TTL and `maxResults` of a given entry are fixed by whichever request **first populates** it:

- The first writer's configured TTL is stamped on the entry at write time and does not change on subsequent reads. When the entry expires, the next request to populate it (Check or List Objects) stamps its own TTL.
- `maxResults` is a write-time cutoff: if the result set exceeds the writing request's `maxResults`, the result is **not cached at all**. Once cached, the stored tuple count is fixed; readers use the entry as-is regardless of their own `maxResults` setting.

The practical implication: if Check and List Objects iterator cache TTLs or `maxResults` are configured to very different values, the effective TTL/limit for a given cache entry depends non-deterministically on which API happened to populate it first.

#### Configuration

| Config File | Env Var | Flag Name | Type | Description | Default Value |
|-------------|---------|-----------|------|-------------|---------------|
| `checkCache.limit` | <div id="OPENFGA_CHECK_CACHE_LIMIT"><code>OPENFGA_CHECK_CACHE_LIMIT</code></div> | `check-cache-limit` | integer | the size limit (in items) of the cache for Check (queries and iterators) | `10000` |
| `listObjectsIteratorCache.enabled` | <div id="OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_ENABLED"><code>OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_ENABLED</code></div> | `list-objects-iterator-cache-enabled` | boolean | enable caching of datastore iterators in ListObjects. The key is a string representing a database query, and the value is a list of tuples. Each iterator is the result of a database query, for example usersets related to a specific object, or objects related to a specific user, up to a certain number of tuples per iterator. If the request's consistency is HIGHER_CONSISTENCY, this cache is not used. | `false` |
| `listObjectsIteratorCache.maxResults` | <div id="OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_MAX_RESULTS"><code>OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_MAX_RESULTS</code></div> | `list-objects-iterator-cache-max-results` | integer | if caching of datastore iterators of ListObjects requests is enabled, this is the limit of tuples to cache per key | `10000` |
| `listObjectsIteratorCache.ttl` | <div id="OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_TTL"><code>OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_TTL</code></div> | `list-objects-iterator-cache-ttl` | string (duration) | if caching of datastore iterators of ListObjects requests is enabled, this is the TTL of each value | `10s` |

## Cache Controller

- **What it does**: Invalidates stale cache entries based on recent writes to the store.
- **Benefits**: Allows using longer TTLs for cache entries to increase hit rate.

The cache controller periodically checks the store's changelog and compares recent tuple writes/deletes to the time cache entries were set. When the cache controller sees new writes to the store, all previous Check query cache entries are invalidated, while only relevant Check iterator cache entries are invalidated based on what tuples the writes affected.

### Invalidation Details

Invalidation runs asynchronously (eventually consistent) and is triggered on:

- Any Check request if cache controller is enabled, but not more than once every cache controller TTL.
- Any List Objects request if List Objects iterator cache is enabled, **irrespective** of cache controller TTL.
- Only one invalidation can be running at a time.

When the cache controller runs invalidation, it queries the database's changelog table for the most recent writes and saves the time of the most recent write. Then, it sets a cache entry of type `changelog` with key `CC + storeID` which contains the time of the latest write to the store. Now, invalidation is handled differently for query vs. iterator cache entries:

- **Query Cache**: Whenever Check finds a query cache entry, it will compare it to the time in the `changelog` cache entry (i.e., the time of the last write to the store). The query cache entry is only used if it was set after the latest write. If it was set before the latest write, it can no longer be trusted and is considered invalid, forcing Check to recompute the result from the datastore. I.e., any write to the store will invalidate all previous Check query cache entries.
- **Iterator Cache**: Invalidation will look at X latest store changes (currently 50) from the changelog and invalidate only the relevant iterator cache entries for each of those changes that are within the Check iterator cache TTL from now. For any changes outside of this window, any iterator cache entry that was set before would have expired by now. Three cases:
      1. If none of these changes are within the Check iterator cache TTL, no invalidation is necessary.
      2. If all of these changes are within the Check iterator cache TTL, all iterator cache entries are invalidated. This is done by setting a new cache entry of type `invalid_entity` with key `IQ + storeID`; iterator cache entries are only used if they were set after the time this `invalid_entity` cache entry was set.
      3. If only some of these changes are within the Check iterator cache TTL, only the iterator cache entries affected by those changes are invalidated. This is done by setting two cache entries of type `invalid_entity` per changed tuple - one with key `IQ + "OR" + storeID + object + relation` (for object-relation DB iterators) and one with key `IQ + "UOT" + storeID + user + objectType` (for user-objectType DB iterators); when the code needs to go to the datastore, it will only use the iterator cache if the associated `invalid_entity` cache keys are not set.

Note: since invalidation is done by setting new cache entries which are checked before using a regular cache entry, the metrics for the number of items in the cache and the number of cache removals may be inflated.

### How iterator invalidation keys map to iterator cache entries

A tuple write `(user, object, relation)` affects exactly two shapes of DB query:

1. **Queries that scan by object+relation** - `IC/READ` and `IC/RUT` entries both include `object` and `relation` in the clear in their key. The invalidation writes a marker at `IQ + "OR" + storeID + object + relation`. On read, both `IC/READ` and `IC/RUT` entries for that object+relation check this same marker and are discarded if their `LastModified` predates it.

2. **Queries that scan by user+objectType** - `IC/RSWU` entries include `objectType` and `relation` in their key, but the invalidation key is `IQ + "UOT" + storeID + user + objectType` - **relation is intentionally omitted**. A write to any `(user, objectType)` combination must conservatively invalidate all RSWU entries for that user and object type, regardless of relation, because a single change could affect multiple relation-scoped reverse lookups. Dropping the relation ensures one invalidation marker covers all of them.

Invalidation never deletes iterator entries; it writes timestamp markers. An iterator cache entry is discarded on read if its `LastModified` predates the relevant marker's `LastModified`. This means entries populated *after* a write are unaffected by that write's marker - they correctly reflect the post-write state.

### Configuration

| Config File | Env Var | Flag Name | Type | Description | Default Value |
|-------------|---------|-----------|------|-------------|---------------|
| `cacheController.enabled` | <div id="OPENFGA_CACHE_CONTROLLER_ENABLED"><code>OPENFGA_CACHE_CONTROLLER_ENABLED</code></div> | `cache-controller-enabled` | boolean | enable invalidation of check query cache and iterator cache based on recent tuple writes. Invalidation is triggered by Check and List Objects requests, which periodically check the datastore's changelog table for writes and invalidate cache entries earlier than recent writes. Invalidations from Check requests are rate-limited by cache-controller-ttl, whereas List Objects requests invalidate every time if list objects iterator cache is enabled. | `false` |
| `cacheController.ttl` | <div id="OPENFGA_CACHE_CONTROLLER_TTL"><code>OPENFGA_CACHE_CONTROLLER_TTL</code></div> | `cache-controller-ttl` | string (duration) | if cache controller is enabled, this is the minimum time interval for Check requests to trigger cache invalidation. List Objects requests may trigger invalidation even sooner if list objects iterator cache is enabled. | `10s` |

With the cache controller enabled, its TTL sets the staleness window for cache. Check and List Objects cache TTLs will now only affect hit rate and memory usage, not freshness. Choose:

- Controller TTL ≤ your acceptable staleness (e.g., 10s).
- Other cache entries' TTLs as long as your memory budget and workload allow.

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

#### Stale Query Cache Entry via Stale Iterator Cache

When the cache controller, iterator cache, and query cache are all enabled, there is an additional staleness window that can arise from the interaction of the three:

1. A Check request runs, reads tuples from the database, and populates iterator cache entries.
2. A tuple write/delete occurs, making those iterator entries stale. The cache controller has not yet run invalidation.
3. A subsequent Check request reads the still-present stale iterator entries, computes a stale result, and stores it in the query cache - stamped with the current time, which is *after* the write in step 2.
4. When the cache controller finally runs, it invalidates cache entries whose `LastModified` predates the write time. The query cache entry from step 3 was stamped *after* the write, so it passes validation and is **not** invalidated. It continues to be served until either a new write to the store advances the invalidation threshold past step 3's timestamp, or the query cache TTL expires.

This is an inherent limitation of timestamp-based invalidation: query cache validity is checked against the last write time only, with no visibility into whether the iterator entries used to compute the result were themselves stale.

**Mitigations:**
- Enable the cache controller with only one of the query or iterator cache (not both). The scenario requires all three to be active simultaneously.
- If using all three, keep the query cache TTL bounded to limit how long the stale entry can persist.
- `HIGHER_CONSISTENCY` requests bypass all caches entirely and are not affected.

## Authorization Model & Typesystem Cache

These two caches are always active and are independent of the caching flags described above.

### Authorization Model Cache

- **What it caches**: Validated `AuthorizationModel` objects, loaded once per `(storeID, modelID)` pair.
- **Cache key**: `MODEL + storeID + modelID`
- **TTL**: 7 days
- **Why a long TTL**: Authorization models are immutable - creating a new model always produces a new model ID, so existing entries can never go stale.
- **Size config**: `OPENFGA_DATASTORE_MAX_CACHE_SIZE` / `--datastore-max-cache-size` (default `100000`)

### Typesystem Cache

- **What it caches**: Compiled `TypeSystem` objects derived from each authorization model, loaded and validated once per `(storeID, modelID)` pair. A `singleflight` group ensures concurrent requests for the same model only trigger one compilation.
- **Cache key**: `TS + storeID + modelID`
- **TTL**: 7 days (same rationale as model cache - models are immutable)
- **Size config**: `OPENFGA_DATASTORE_MAX_TYPESYSTEM_CACHE_SIZE` / `--datastore-max-typesystem-cache-size` (default `100000`)

## TTL Jitter

OpenFGA supports adding random jitter to the TTL of query and iterator cache entries. When enabled, each entry's effective TTL is `TTL + random(0, TTL × jitterPercentage / 100)`. For example, with a base TTL of `10s` and jitter of `10%`, each entry expires somewhere between `10s` and `11s`.

**Why:** without jitter, all cache entries populated at roughly the same time expire at roughly the same time, causing a burst of simultaneous DB queries (thundering herd). Jitter spreads out expirations to smooth the repopulation load.

**Configuration:**

| Config File | Env Var | Flag Name | Type | Description | Default Value |
|-------------|---------|-----------|------|-------------|---------------|
| `cacheTTLJitterPercentage` | <div id="OPENFGA_CACHE_TTL_JITTER_PERCENTAGE"><code>OPENFGA_CACHE_TTL_JITTER_PERCENTAGE</code></div> | `cache-ttl-jitter-percentage` | integer (0–100) | percentage of the base TTL added as random jitter to each cache entry's TTL at write time | `0` (disabled) |

Note: jitter applies to the query cache and iterator caches only. The authorization model and typesystem caches use a fixed 7-day TTL.

## Observability

OpenFGA exposes the following metrics for caching:

Raw Metric Name                                           | Metric Name in Prometheus*                                | [Type](https://prometheus.io/docs/concepts/metric_types/) | Description
----------------------------------------------------------|-----------------------------------------------------------|-----------|------------------------------------------------------------
`openfga_cache_item_count`                                | `openfga_cache_item_count`                                | Gauge     | The current number of items stored in the cache
`openfga_cache_item_removed_count`                        | `openfga_cache_item_removed_count_total`                  | Counter   | The total number of items removed (evicted/expired/deleted) from the cache
`openfga_cachecontroller_cache_total_count`               | `openfga_cachecontroller_cache_count_total`               | Counter   | The total number of cache controller requests triggered by Check.
`openfga_cachecontroller_cache_hit_count`                 | `openfga_cachecontroller_cache_hit_count_total`           | Counter   | The total number of cache controller requests triggered by Check within the cache controller TTL (i.e., no invalidation).
`openfga_cachecontroller_cache_invalidation_count`        | `openfga_cachecontroller_cache_invalidation_count_total`  | Counter   | The total number of invalidation requests that invalidated iterator caches.
`openfga_cachecontroller_invalidation_duration_ms_bucket` | `openfga_cachecontroller_invalidation_duration_ms_bucket` | Histogram | The duration (in ms) required for cache controller to find changes and invalidate labeled by whether invalidation is required and buckets of changes size.
`openfga_cachecontroller_invalidation_duration_ms_count`  | `openfga_cachecontroller_invalidation_duration_ms_count`  | "         | "
`openfga_cachecontroller_invalidation_duration_ms_sum`    | `openfga_cachecontroller_invalidation_duration_ms_sum`    | "         | "
`openfga_check_cache_total_count`                         | `openfga_check_cache_count_total`                         | Counter   | The total number of calls to ResolveCheck with caching enabled (including any recursive calls).
`openfga_check_cache_hit_count`                           | `openfga_check_cache_hit_count_total`                     | Counter   | The total number of valid Check Query cache hits for ResolveCheck (including any recursive calls).
`openfga_check_cache_invalid_hit_count`                   | `openfga_check_cache_invalid_hit_count_total`             | Counter   | The total number of Check Query cache hits for ResolveCheck (including any recursive calls) that were discarded because they were invalidated.
`openfga_current_iterator_cache_count`                    | `openfga_current_iterator_cache_count`                    | Gauge     | The current number of cached iterator instances.
`openfga_tuples_cache_total_count`                        | `openfga_tuples_cache_count_total`                        | Counter   | The total number of created cached iterator instances.
`openfga_tuples_cache_discard_count`                      | `openfga_tuples_cache_discard_count_total`                | Counter   | The total number of discards from cached iterator instances.
`openfga_tuples_cache_hit_count`                          | `openfga_tuples_cache_hit_count_total`                    | Counter   | The total number of cache hits from cached iterator instances.
`openfga_tuples_cache_size_bucket`                        | `openfga_tuples_cache_size_bucket`                        | Histogram | The number of tuples cached from iterator cache entries.
`openfga_tuples_cache_size_count`                         | `openfga_tuples_cache_size_count`                         | "         | "
`openfga_tuples_cache_size_sum`                           | `openfga_tuples_cache_size_sum`                           | "         | "

\* Prometheus automatically applies [naming conventions](https://prometheus.io/docs/practices/naming/) to metric names when viewing metrics in its UI or other interfaces that pull from it (e.g., Grafana UI).

## Best Practices

### Enablement

The different caches must be explicitly enabled - see [OpenFGA Configuration Options](https://openfga.dev/docs/getting-started/setup-openfga/configuration) for more info.

### Production Deployment

1. **Gradual rollout**: Enable caching incrementally, starting with query cache
2. **Monitor memory usage**: Track cache memory consumption
3. **Set up alerting**: Alert on low cache hit rates or high eviction rates
   - Check Query Cache hit rate: `openfga_check_cache_hit_count_total / openfga_check_cache_count_total`*
   - Iterator Cache hit rate: `openfga_tuples_cache_hit_count_total / openfga_tuples_cache_count_total`*
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
