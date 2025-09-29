# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Try to keep listed changes to a concise bulleted list of simple explanations of changes. Aim for the amount of information needed so that readers can understand where they would look in the codebase to investigate the changes' implementation, or where they would look in the documentation to understand how to make use of the change in practice - better yet, link directly to the docs and provide detailed information there. Only elaborate if doing so is required to avoid breaking changes or experimental features from ruining someone's day.

## [Unreleased]
### Changed
- Bumped the version of `openfga/language/pkg` to a version of the weighted graph that includes recursive relation detection. [#2716](https://github.com/openfga/openfga/pull/2716)
- Log the reason on server failure start [#2703](https://github.com/openfga/openfga/pull/2703)
- Remove zap type conversion on request logger [#2717](https://github.com/openfga/openfga/pull/2717)

### Fixed
- Fixed a bug where experimental ReverseExpand constructed the underlying check relation incorrectly for intersection and exclusion in ListObjects. [#2721](https://github.com/openfga/openfga/pull/2721)

## [1.10.1] - 2025-09-22
### Fixed
- Revert spf13/viper back to v.1.20.1 to avoid bumping sourcegraph/conc to an unreleased version as it causes performance degradation. [#2706](https://github.com/openfga/openfga/pull/2706)

## [1.10.0] - 2025-09-11
### Added
- Make number of querying goroutines in experimental reverse_expand configurable via `resolveNodeBreadthLimit`. [#2652](https://github.com/openfga/openfga/pull/2652)
- Add microsecond latency numbers and datastore query count in shadow check resolver. [#2658](https://github.com/openfga/openfga/pull/2658)
- Add `NewWithDB` support for sqlite storage. [#2679](https://github.com/openfga/openfga/pull/2679)
- Add planner for selecting check resolution strategies based on runtime statistics, behind the `enable-check-optimization` flag. [#2624](https://github.com/openfga/openfga/pull/2624)
- Add `server.WithShadowCheckCacheEnabled` to enable creation of a separate cache for shadow check resolver. [#2683](https://github.com/openfga/openfga/pull/2683)
- Run weight 2 optimization for cases where there are more than 1 directly assignable userset. [#2684](https://github.com/openfga/openfga/pull/2684)

### Changed
- Make experimental reverse_expand behave the same as old reverse_expand in case of timeouts. [#2649](https://github.com/openfga/openfga/pull/2649)
- Breaking: Changes to storage interface

  > [!NOTE]
  > The following breaking changes are related to the storage interface. If you are not implementing a storage adapter, then these changes should not impact your usage of OpenFGA.

  - Changed the `RelationshipTupleWriter` datastore interface to accept variadic write options (`opts ...TupleWriteOption`) to customize behavior on duplicate inserts and missing deletes. [#2663](https://github.com/openfga/openfga/pull/2663)
    Implementers must update the `Write` method signature to include `opts ...TupleWriteOption`. Defaults preserve prior behavior (error on duplicates and on missing deletes). Example:
    `Write(ctx, store, deletes, writes, storage.WithOnDuplicateInsert(storage.OnDuplicateInsertIgnore))`

### Fixed
- Fixed bug in how experimental ReverseExpand support for ttus with multiple parents in the intersection and exclusion for list objects. [#2691](https://github.com/openfga/openfga/pull/2691)
- Improve performance by allowing weight 2 optimization if the directly assignable userset types are of different types. [#2645](https://github.com/openfga/openfga/pull/2645)
- Update ListObjects' check resolver to use correct environment variable. [#2653](https://github.com/openfga/openfga/pull/2653)
- !!REQUIRES MIGRATION!! Collation specification for queries dependent on sort order. [#2661](https://github.com/openfga/openfga/pull/2661)
    - PostgreSQL is non-disruptive.
    - MySQL requires a shared lock on the tuple table during the transaction.

## [1.9.5] - 2025-08-15
### Fixed
- Do not run weight 2 optimization for cases where there are more than 1 directly assignable userset. [#2643](https://github.com/openfga/openfga/pull/2643)

## [1.9.4] - 2025-08-13
### Fixed
- Fix breaking change introduced after upgrading the JWT dependency from v5.2.2 to v.5.3.0. [#2636](https://github.com/openfga/openfga/pull/2636)

## [1.9.3] - 2025-08-11
### Added
- Add `check_count` grpc tag to list objects requests. [#2515](https://github.com/openfga/openfga/pull/2515)
- Promote the Check fast path v2 implementations to no longer being behind the `enable-check-optimizations` config flag. [#2609](https://github.com/openfga/openfga/pull/2609)

### Changed
- Change ListObjectsResolutionMetadata fields to value types instead of pointers. [#2583](https://github.com/openfga/openfga/pull/2583)
- Instead of panic when encountering unknown parameters in hasEntrypoints, return internal error to allow graceful handling. [#2588](https://github.com/openfga/openfga/pull/2588)
- Shared iterators now rely entirely on a TTL for eviction from the pool. [#2590](https://github.com/openfga/openfga/pull/2590)
- Update go toolchain version to 1.24.6 - related: [CVE-2025-47907](https://nvd.nist.gov/vuln/detail/CVE-2025-47907)
- Revert min supported go version to 1.24.0
- Bump the base docker image to `cgr.dev/chainguard/static@sha256:6a4b683f4708f1f167ba218e31fcac0b7515d94c33c3acf223c36d5c6acd3783`

### Fixed
- Fixed bug in how experimental ReverseExpand is handling duplicate TTUs. [#2589](https://github.com/openfga/openfga/pull/2589)
- Fixed bug in how experimental ReverseExpand is handling duplicate edge traversals. [#2594](https://github.com/openfga/openfga/pull/2594)
- Fixed logs in ListObjects weighted graph to include `store_id` and `authorization_model_id` through the context. [#2581](https://github.com/openfga/openfga/pull/2581)
- Fixed bug where OpenFGA fail to start when both secondary DB and db metrics enabled. [#2598](https://github.com/openfga/openfga/pull/2598)

### Security
- Bumped up the `grpc-health-probe` dependency in the published Docker image to the latest release (v0.4.39) which fixes some vulnerabilities. [#2601](https://github.com/openfga/openfga/pull/2601)

## [1.9.2] - 2025-07-24
### Added
- Add `list_objects_optimization_count` metric to list objects requests. [#2524](https://github.com/openfga/openfga/pull/2524)

### Changed
- Update ReverseExpand to use a LinkedList to track its relation stack for performance. [#2542](https://github.com/openfga/openfga/pull/2542)
- Update ReverseExpand to use a intersection and exclusion handler to fast path check calls. [#2543](https://github.com/openfga/openfga/pull/2543)
- Deduplicate queries more effectively in ReverseExpand. [#2567](https://github.com/openfga/openfga/pull/2567)
- Update go version to 1.24.5 [#2577](https://github.com/openfga/openfga/pull/2577)

### Fixed
- Shared iterator race condition and deadlock. [#2544](https://github.com/openfga/openfga/pull/2544)
- Fixed bug in how experimental ReverseExpand is handling Intersection nodes. [#2556](https://github.com/openfga/openfga/pull/2556)
- Migration command now respects logging configuration options. [#2541](https://github.com/openfga/openfga/issues/2541)
- Fixed message in log and slight refactor in list objects intersection/exclusion. [#2566](https://github.com/openfga/openfga/pull/2566)
- Shadow list objects' check resolver should use its own cache. [#2574](https://github.com/openfga/openfga/pull/2574)
- Improve performance for list objects intersection/exclusion with `enable-list-objects-optimizations` flag. [#2569](https://github.com/openfga/openfga/pull/2569)

## [1.9.1] - 2025-07-22
### Changed
- **WARNING:** This is an incorrect version and should not be used. Please upgrade to version `v1.9.2` and do not use its companion docker image with the sha:
  `sha256:71212e9aa2f26cd74287babf7eb92743b8510960388ff0b5ac68d89a23728898`

## [1.9.0] - 2025-07-03
### Added
- Add separate reverse_expand path utilizing the weighted graph. Gated behind `enable-list-objects-optimizations` flag. [#2529](https://github.com/openfga/openfga/pull/2529)

### Changed
- SQLite based iterators will load tuples only when needed (lazy loading). [#2511](https://github.com/openfga/openfga/pull/2511)
- Shared iterator improvement to reduce lock contention when creating and cloning. [#2530](https://github.com/openfga/openfga/pull/2530)
- Enable experimental list object optimizations in shadow mode using flag `enable-list-objects-optimizations`. [#2509](https://github.com/openfga/openfga/pull/2509)
- Invalidated iterators will be removed from cache if an invalid entity entry is found allowing for less time to refresh. [#2536](https://github.com/openfga/openfga/pull/2536)
- Shared iterator cache map split into a single map per datastore operation. [#2549](https://github.com/openfga/openfga/pull/2549)
- Shared Iterator cloning performance improvement. [#2551](https://github.com/openfga/openfga/pull/2551)
- Shared iterator performance enhancements. [#2553](https://github.com/openfga/openfga/pull/2553)

### Fixed
- Cache Controller was always completely invalidating. [#2522](https://github.com/openfga/openfga/pull/2522)

## [1.8.16] - 2025-06-17
### Fixed
- Context cancelation was preventing database connections from being reused. [#2508](https://github.com/openfga/openfga/pull/2508)

## [1.8.15] - 2025-06-11
### Added
- Add support for separate read and write datastores for PostgreSQL. [#2479](https://github.com/openfga/openfga/pull/2479)

### Changed
- Shared iterator refactor to reduce lock contention. [#2478](https://github.com/openfga/openfga/pull/2478)

### Fixed
- Improve Check performance for models with recursion and `enable-check-optimizations` experiment flag is enabled. [#2492](https://github.com/openfga/openfga/pull/2492)
- Reverts the base docker image back to `cgr.dev/chainguard/static` [#2473](https://github.com/openfga/openfga/issues/2473)
- Fix for picking up env vars for `migrate` pkg [#2493](https://github.com/openfga/openfga/issues/2493)

## [1.8.14] - 2025-06-10
### Fixed
- Performance improve for SQL based datastore by reducing connection usage during IsReady check. [#2483](https://github.com/openfga/openfga/pull/2483)
- SQL drivers now respect zero value for MaxOpenConns, ConnMaxIdleTime, ConnMaxLifetime. [#2484](https://github.com/openfga/openfga/pull/2484)
- When `enable-check-optimizations` experiment flag is enabled, incorrect check for model with recursion and user is assigned to more than 100 groups due to iteratorToUserset not handling multiple messages incorrectly. [#2491](https://github.com/openfga/openfga/pull/2491)
- Correlate storage context with caller's context. [#2292](https://github.com/openfga/openfga/pull/2292)

## [1.8.13] - 2025-05-22
### Added
- New `DatastoreThrottle` configuration for Check, ListObjects, ListUsers. [#2452](https://github.com/openfga/openfga/pull/2452)
- Added pkg `migrate` to expose `.RunMigrations()` for programmatic use. [#2422](https://github.com/openfga/openfga/pull/2422)
- Performance optimization by allowing datastore query iterator to be shared by multiple consumers. This can be enabled via `OPENFGA_SHARED_ITERATOR_ENABLED`. [#2433](https://github.com/openfga/openfga/pull/2433), [#2410](https://github.com/openfga/openfga/pull/2410) and [#2423](https://github.com/openfga/openfga/pull/2423)
- Upgraded all references of Postgres to v17. [#2407](https://github.com/openfga/openfga/pull/2407)

### Fixed
- Ensure `fanin.Stop` and `fanin.Drain` are called for all clients which may create blocking goroutines. [#2441](https://github.com/openfga/openfga/pull/2441)
- Prevent throttled Go routines from "leaking" when a request context has been canceled or deadline exceeded. [#2450](https://github.com/openfga/openfga/pull/2450)
- Filter context tuples based on type restrictions for ReadUsersetTuples. [CVE-2025-48371](https://github.com/openfga/openfga/security/advisories/GHSA-c72g-53hw-82q7)

## [1.8.12] - 2025-05-12
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.11...v1.8.12)

### Changed
- `DefaultResolveNodeBreadthLimit` changed from 100 to 10 in order to reduce connection contention. [#2425](https://github.com/openfga/openfga/pull/2425)
- PostgreSQL and MySQL based iterators will load tuples only when needed (lazy loading). [#2425](https://github.com/openfga/openfga/pull/2425)

### Fixed
- Replace hardcoded Prometheus datasource UID (`PBFA97CFB590B2093`) with `${DS_PROMETHEUS}` in `telemetry/grafana/dashboards/openfga.json`. This allows the Grafana dashboard to correctly reference the dynamic Prometheus datasource, resolving issues with improper binding. [#2287](https://github.com/openfga/openfga/issues/2287)
- Handle case where iterator is stopped more than once for `current_iterator_cache_count`. [#2409](https://github.com/openfga/openfga/pull/2409)
- Fix deadlock when number of SQL datastore connections is less than Resolve Max Breadth. [#2425](https://github.com/openfga/openfga/pull/2425)
- Improved `panic()` handling from `go` routines: [#2379](https://github.com/openfga/openfga/pull/2379), [#2385](https://github.com/openfga/openfga/pull/2385), [#2405](https://github.com/openfga/openfga/pull/2405)

## [1.8.11] - 2025-04-29
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.10...v1.8.11)

### Changed
- Update go version to 1.24.2 [#2404](https://github.com/openfga/openfga/pull/2404)

### Fixed
- Do not save to check query cache when result indicates cycle. [CVE-2025-46331](https://github.com/openfga/openfga/security/advisories/GHSA-w222-m46c-mgh6)

## [1.8.10] - 2025-04-28
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.9...v1.8.10)

### Added
- Added "dispatch_count" attribute to batch-check request logs. [#2369](https://github.com/openfga/openfga/pull/2369)
- Added "dispatch_count" histogram metric to batch-check requests. [#2369](https://github.com/openfga/openfga/pull/2369)
- Added "request.throttled" boolean for check and batch-check request logs. [#2369](https://github.com/openfga/openfga/pull/2369)
- Added "throttled_requests_count" metric to batch-check requests. [#2369](https://github.com/openfga/openfga/pull/2369)
- Surface partial metrics on check resolutions [#2371](https://github.com/openfga/openfga/pull/2371)
- Adds `cache_item_count` gauge metric to track how many items are in the check query cache. [#2396](https://github.com/openfga/openfga/pull/2412)
- Adds `cache_item_removed_count` counter metric to track removed items from cache. [#2396](https://github.com/openfga/openfga/pull/2412)
- Added "current_iterator_cache_count" gauge metric to current number of iterator cache. [#2397](https://github.com/openfga/openfga/pull/2397)
- Adds cached iterators to ListObjects [#2388](https://github.com/openfga/openfga/pull/2388)

### Changed
- The serverconfig was moved from internal to pkg to make it available to external users of this package. [#2382](https://github.com/openfga/openfga/pull/2382)
- Panics in goroutines in check are converted to errors.

### Fixed
- Add limit to goroutine concurrency when processing iterator [#2386](https://github.com/openfga/openfga/pull/2386)
- Fixes case where cached_datastore.ReadStartingWithUser generated bad cache invalidation keys. [#2381](https://github.com/openfga/openfga/pull/2381)

## [1.8.9] - 2025-04-01
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.8...v1.8.9)

### Added
- Updated grpc logs for the healthcheck service to log at the Debug level instead of at the Info level. [#2340](https://github.com/openfga/openfga/pull/2340)
- Separate out experimental list objects optimization flag (`enable-list-objects-optimizations`) from experimental check optimization flag (`enable-check-optimizations`) to allow individual optimization. [#2341](https://github.com/openfga/openfga/pull/2341).

## [1.8.8] - 2025-03-18
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.7...v1.8.8)

### Added
- Added a new CheckResolver (`ShadowResolver`) to allow comparing changes across different CheckResolvers. [#2308](https://github.com/openfga/openfga/pull/2308).

### Changed
- Extend object_id VARCHAR in MySQL to 255 characters. [#2230](https://github.com/openfga/openfga/pull/2230).

## [1.8.7] - 2025-03-07
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.6...v1.8.7)

### Added
- Added `storage.ErrTransactionThrottled` for throttling errors applied at the datastore level. [#2304](https://github.com/openfga/openfga/pull/2304).

### Removed
- Removed recently-added `tuples_iterator_cache_invalid_hit_count` metric. The `cachecontroller_cache_invalidation_count` from 1.8.6 better accomplishes the same goal. [#2296)[https://github.com/openfga/openfga/pull/2296/]

### Fixed
- Fixed evaluation of certain recursive TTU cases behind the `enable-check-optimizations` flag. [#2281](https://github.com/openfga/openfga/pull/2281)

## [1.8.6] - 2025-02-20
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.5...v1.8.6)

### Added
- Added `cachecontroller_cache_invalidation_count` metric to track invalidation operations. [#2282](https://github.com/openfga/openfga/pull/2282)

## [1.8.5] - 2025-02-19
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.4...v1.8.5)

### Added
- Improve `Check` performance for sub-problems when caching is enabled [#2193](https://github.com/openfga/openfga/pull/2193).
- Improve `Check` performance for relations involving public wildcard. Enable via experimental flag `enable-check-optimizations`.  [#2180](https://github.com/openfga/openfga/pull/2180).
- Improve Check API performance when experimental flag `enable-check-optimizations` is turned on and contextual tuples are involved. [#2150](https://github.com/openfga/openfga/pull/2150)
- Added metrics to track invalid cache hits: `check_cache_invalid_hit_count` and `tuples_iterator_cache_invalid_hit_count` [#2222](https://github.com/openfga/openfga/pull/2222).
- Move Check performance optimizations out of experimental mode: shortcutting based on path, recursive userset fast path, and recursive TTU fast path. [#2236](https://github.com/openfga/openfga/pull/2236)
- Improve Check API performance when experimental flag `enable-check-optimizations` is turned on and the model contains union of a TTU and algebraic operations. [#2200](https://github.com/openfga/openfga/pull/2200)
- Implement dynamic TLS certificate reloading for HTTP and gRPC servers. [#2182](https://github.com/openfga/openfga/pull/2182)
- Publicize `check.RunMatrixTests` method to allow testing against any `ClientInterface`. [#2267](https://github.com/openfga/openfga/pull/2267).

### Changed
- Performance optimizations for string operations and memory allocations across the codebase [#2238](https://github.com/openfga/openfga/pull/2238) and [#2241](https://github.com/openfga/openfga/pull/2241)
- Update to Go 1.23 as the min supported version and bump the container image to go1.23.6
  We follow Go's version support policy and will only support the latest two major versions of Go. Now that [Go 1.24 is out](https://go.dev/blog/go1.24), we have dropped support for Go < 1.23.

### Fixed
- Optimized database dialect handling by setting it during initialization instead of per-call, fixing SQL syntax errors in MySQL [#2252](https://github.com/openfga/openfga/pull/2252)
- Fixed incorrect invalidation by cache controller on cache iterator. [#2190](https://github.com/openfga/openfga/pull/2190), [#2216](https://github.com/openfga/openfga/pull/2216)
- Fixed incorrect types in configuration JSON schema [#2217](https://github.com/openfga/openfga/pull/2217), [#2228](https://github.com/openfga/openfga/pull/2228).
- Fixed `BatchCheck` API to validate presence of the `tuple_key` property of a `BatchCheckItem` [#2242](https://github.com/openfga/openfga/issues/2242)
- Fixed incorrect check and list objects evaluation when model has a relation directly assignable to both public access AND userset with the same type and type bound public access tuple is assigned to the object.

## [1.8.4] - 2025-01-13
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.3...v1.8.4)

### Fixed
- Fixed missing binding between flags and environment variables for the cache controller feature [#2184](https://github.com/openfga/openfga/pull/2184)
- Fixed Read API to validate user field and assert presence of both type and value. [#2195](https://github.com/openfga/openfga/pull/2195)

### Security
- Address [CVE-2024-56323](https://github.com/openfga/openfga/security/advisories/GHSA-32q6-rr98-cjqv) - an issue affecting Check and ListObjects results for users using Conditions in Contextual Tuples. Please see the CVE report for more details.

## [1.8.3] - 2024-12-31
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.2...v1.8.3)

### Added
- Improve `Check` performance for Userset relationships that include set operations. Enable via experimental flag `enable-check-optimizations`. [#2140](https://github.com/openfga/openfga/pull/2140)
- Add `name` as a filter to `ListStores`. The name parameter instructs the API to only include results that match that name. [#2103](https://github.com/openfga/openfga/pull/2103)
- Additional guard against nil context at the time of server initialization [#2187](https://github.com/openfga/openfga/pull/2187)

### Fixed
- Ensure Check Cache Key considers `contextual_tuple` conditions and their contexts [#2160](https://github.com/openfga/openfga/pull/2160).

## [1.8.2] - 2024-12-13
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.1...v1.8.2)

### Added
- Add metrics `cachecontroller_find_changes_and_invalidate_histogram` on latency for cache controller in finding changes and invalidating.  [#2135](https://github.com/openfga/openfga/pull/2135)
- Improve `Check` performance when cache controller is enabled by invalidating iterator and sub-problem cache asynchronously when read changes API indicates there are recent writes/deletes for the store.  [#2124](https://github.com/openfga/openfga/pull/2124)
- Improve check cache key generation performance via `strings.Builder` [#2161](https://github.com/openfga/openfga/pull/2161).

### Fixed
- Labels of metrics that went past the `max` histogram bucket are now labelled "+Inf" instead of ">max". [#2146](https://github.com/openfga/openfga/pull/2146)
- Prevent possible data races by waiting for in-flight cached iterator goroutines during server shutdown [#2145](https://github.com/openfga/openfga/pull/2145)
- Correct incorrect check result returned when using experimental flag `enable-check-optimizations` and model has intersection or exclusion within a TTU or Userset. [#2157](https://github.com/openfga/openfga/pull/2157)

## [1.8.1] - 2024-12-05
[Full changelog](https://github.com/openfga/openfga/compare/v1.8.0...v1.8.1)

### Added
- New flag `OPENFGA_CHECK_ITERATOR_TTL`. Please see the flag description (`./openfga run --help`) for more details. [#2082](https://github.com/openfga/openfga/pull/2082)
- New flag `OPENFGA_CHECK_CACHE_LIMIT`. Please see the flag description (`./openfga run --help`) for more details. [#2082](https://github.com/openfga/openfga/pull/2082)
- Improve `Check` performance for TTU relationships that include set operations. Enable via experimental flag `enable-check-optimizations`. [#2075](https://github.com/openfga/openfga/pull/2075)
- Add a field in log entries when authz calls were made. [#2130](https://github.com/openfga/openfga/pull/2130)
- Add `Duration` to `ResolveCheckResponseMetadata` for use in metrics. [#2139](https://github.com/openfga/openfga/pull/2139)
- Add `check_duration_ms` metric to `server` package to enable measurement of check across different API methods. [#2139](https://github.com/openfga/openfga/pull/2139)
- Added deduplication logic to BatchCheck API. [#2102](https://github.com/openfga/openfga/pull/2102)

### Changed
- OIDC token validation will now exclusively throw error code 1004 for invalid tokens. [#1999](https://github.com/openfga/openfga/pull/1999)

### Removed
- Begin deprecation process for flag `OPENFGA_CHECK_QUERY_CACHE_LIMIT`, in favor of `OPENFGA_CHECK_CACHE_LIMIT`. [#2082](https://github.com/openfga/openfga/pull/2082)
- Removed flags with the `OPENFGA_DISPATCH_THROTTLING_*` name. [#2083](https://github.com/openfga/openfga/pull/2083)

### Fixed
- Improve `Check` performance in the case that the query involves types that cannot be reached from the source. Enable via experimental flag `enable-check-optimizations`. [#2104](https://github.com/openfga/openfga/pull/2104)
- Fix regression introduced in #2091: error message for invalid Writes. [#2110](https://github.com/openfga/openfga/pull/2110)
- Ensure `/read` and `/list-objects` respect the received `Consistency` values [#2113](https://github.com/openfga/openfga/pull/2113)
- Fix `access-control` to always return unauthorized errors, and add logging for authorization failures [2129](https://github.com/openfga/openfga/pull/2129)
- Fix composition of database decorators to fix some performance issues. [#2126](https://github.com/openfga/openfga/pull/2126)

## [1.8.0] - 2024-11-08
[Full changelog](https://github.com/openfga/openfga/compare/v1.7.0...v1.8.0)

### Added
- Added `start_time` parameter to `ReadChanges` API to allow filtering by specific time [#2020](https://github.com/openfga/openfga/pull/2020)
- Added support for Contextual Tuples in the `Expand` API. [#2045](https://github.com/openfga/openfga/pull/2045)
- Added a flag `OPENFGA_CONTEXT_PROPAGATION_TO_DATASTORE` to control propagation of a request's context to the datastore. [#1838](https://github.com/openfga/openfga/pull/1838)
- Added OTEL measurement for access control store check latency and write latency due to authorization [#2069](https://github.com/openfga/openfga/pull/2069)
- Added `BatchCheck` API which allows multiple check operations to be performed in a single request.
  It requires a unique `correlation_id` associated with each individual check to map each result to its associated tuple.
  For more details, see [batch check docs](https://openfga.dev/docs/interacting/relationship-queries#batch-check) [#2039](https://github.com/openfga/openfga/pull/2039).

### Changed
- The storage adapter `ReadChanges`'s parameter ReadChangesOptions allows filtering by `StartTime` [#2020](https://github.com/openfga/openfga/pull/2020).
  As a part of the implementation, a new server setting called `WithContinuationTokenSerializer` was introduced.
  If you are using OpenFGA as a library, you will need to pass in either `StringContinuationTokenSerializer`, or `SQLContinuationTokenSerializer`, or implement your own (if you also have your own storage adapter)
- The storage adapter `ReadPage` return parameters changed from `([]*openfgav1.Tuple, []byte, error)` to `([]*openfgav1.Tuple, string, error)` [#2064](https://github.com/openfga/openfga/pull/2064)
  If you are using a custom storage adapter or consume `ReadPage` func in your code, you will need to update the return type and/or handling of the `ReadPage` function.
- `ErrMismatchObjectType` error type removed from `openfga` package [#2064](https://github.com/openfga/openfga/pull/2064) as storage is not validating this anymore.
  Validation moved to `ReadChangesQuery` implementation.

### Fixed
- Improve `Check` performance in the case that the query involves resolving nested userset with type bound public access. Enable via experimental flag `enable-check-optimizations`. [#2063](https://github.com/openfga/openfga/pull/2063)

## [1.7.0] - 2024-10-29
### Added
- Add an experimental access control feature [#1913](https://github.com/openfga/openfga/pull/1913)
  Learn more about this feature and how to enable it [here](https://openfga.dev/docs/getting-started/setup-openfga/access-control)
  If you do try it out, please provide feedback in the [GitHub Discussion](https://github.com/orgs/openfga/discussions)
- Document OpenFGA release process [#1923](https://github.com/openfga/openfga/pull/1923)

### Changed
- Bump max number of contextual tuples in a single request to `100`. [#2040](https://github.com/openfga/openfga/pull/2040)
  Note: In assertions, they are still restricted to `20` per assertion
- The storage adapter `ListStores`'s parameter `ListStoresOptions` allows filtering by `IDs` [#1913](https://github.com/openfga/openfga/pull/1913)
  If you are using a custom storage adapter, `ListStores` now expects `ListStoresOptions` parameter that accepts passing in a list of IDs.
  See the following adapter [change](https://github.com/openfga/openfga/pull/1913/files#diff-8b98b331c5d4acbeb7274c68973d20900daaed47c8d8f3e62ba39284379166bbR86-R87) and the following [change](https://github.com/openfga/openfga/pull/1913/files#diff-087f50fca2d7eab21b8d342dbbf8fb0de6d405f85b51334b3801d2c34d810ff9L582-L587) for a sample storage adapter implementation.
  If you are not using OpenFGA as a library with a custom storage adapter, this will not affect you. (for example, if you are using OpenFGA through our published docker images, you are not affected).

### Fixed
- Improve `Check` performance in the case that the query involves resolving nested tuple to userset relations. Enable via experimental flag `enable-check-optimizations`. [#2025](https://github.com/openfga/openfga/pull/2025)
- Improve the sub-problem caching in `Check` [#2006](https://github.com/openfga/openfga/pull/2006), [#2035](https://github.com/openfga/openfga/pull/2035)
- Fixed internal error for `Check` where model has nested userset with publicly assignable wildcard. [#2049](https://github.com/openfga/openfga/pull/2049)
- Fixed goroutine leak when `ListObjects` or `StreamedListObjects` call cannot be completed within `REQUEST_TIMEOUT`. [#2030](https://github.com/openfga/openfga/pull/2030)
- Fixed incorrect dispatch counts in `ListObjects` used for observability [2013](https://github.com/openfga/openfga/pull/2013)
- Correct metrics label for `ListUsers` API calls [#2000](https://github.com/openfga/openfga/pull/2000)

## [1.6.2] - 2024-10-03
[Full changelog](https://github.com/openfga/openfga/compare/v1.6.1...v1.6.2)

### Added
- Improve tracing in Check API by enhancing discoverability of model ID. [#1964](https://github.com/openfga/openfga/pull/1964)
- Improve tracing in all APIs by adding the store ID to the span. [#1965](https://github.com/openfga/openfga/pull/1965)
- Add a cache for datastore iterators on Check API. [#1924](https://github.com/openfga/openfga/pull/1924).

  Can be configured via `OPENFGA_CHECK_ITERATOR_CACHE_ENABLED` and `OPENFGA_CHECK_ITERATOR_CACHE_MAX_RESULTS`.
- Improve check performance in the case that the query involves resolving nested userset. Enable via experimental flag `enable-check-optimizations`. [#1945](https://github.com/openfga/openfga/issues/1945)

### Changed
- `ReadChanges` now supports sorting. [#1976](https://github.com/openfga/openfga/pull/1976).

  This is a breaking change related to the storage interface. If you are not implementing a storage adaptor, then these changes should not impact you.

### Removed
- Removed deprecated opentelemetry-connector `memory_ballast` extension. [#1942](https://github.com/openfga/openfga/pull/1942).
- Removed experimental logging of cache hits for each subproblem in `Check` API calls. [#1960](https://github.com/openfga/openfga/pull/1960).

### Fixed
- Handle all permutations of SQLite busy / locked errors [#1936](https://github.com/openfga/openfga/pull/1936). Thanks @DanCech!
- Goroutine leak in Check API introduced in v1.6.1 [#1962](https://github.com/openfga/openfga/pull/1962).
- Broken migration from v.1.4.3 to v1.5.4 (https://github.com/openfga/openfga/issues/1668) [#1980](https://github.com/openfga/openfga/issues/1980) and [#1986](https://github.com/openfga/openfga/issues/1986).
- Upgrade go from 1.22.6 to 1.22.7 to address CVE-2024-34156 [#1987](https://github.com/openfga/openfga/pull/1987). Thanks @golanglemonade!

## [1.6.1] - 2024-09-12
### Added
- Stack trace when logging panics [#1904](https://github.com/openfga/openfga/pull/1904)
- Throttling metric `throttled_requests_count` for observing the number of throttled requests for a given throttling configuration [#1863](https://github.com/openfga/openfga/pull/1863)
- New metric on number of allowed vs. non-allowed Check responses [#1911](https://github.com/openfga/openfga/pull/1911)
- New datastore engine: SQLite (beta) [#1615](https://github.com/openfga/openfga/pull/1615) Thanks @DanCech!

  ```
  openfga migrate --datastore-engine sqlite --datastore-uri openfga.sqlite
  openfga run --datastore-engine sqlite --datastore-uri openfga.sqlite
  ```

### Changed
- Support context in assertions [#1907](https://github.com/openfga/openfga/pull/1907)

### Fixed
- When a request gets cancelled by a client, throw a 4xx, not a 5xx. [#1905](https://github.com/openfga/openfga/pull/1905)
- Makes the `pkg.logger.Logger.With` immutable by creating a child logger instead of mutating the delegate one to prevent side effects [1906](https://github.com/openfga/openfga/pull/1906)
- Extend request timeout to 10s for slow tests [1926](https://github.com/openfga/openfga/pull/1926)
- Improve performance of Check API in the case that the query involves resolving a tuple to userset and/or a userset, by streaming intermediate results. [#1888](https://github.com/openfga/openfga/pull/1888)

## [1.6.0] - 2024-08-30
[Full changelog](https://github.com/openfga/openfga/compare/v1.5.9...v1.6.0)

### Changed
- Consistency options experimental flag has been removed and is now enabled by default. Refer to the [consistency options documentation](https://openfga.dev/docs/interacting/consistency) for details. [#1889](https://github.com/openfga/openfga/pull/1889)
- Require at least Go 1.22.6 [#1831](https://github.com/openfga/openfga/pull/1831). Thanks @tranngoclam
- Add a "query_duration_ms" field on each log [#1807](https://github.com/openfga/openfga/pull/1831). Thanks @lalalalatt
- Default logging to stdout instead of stderr [#1830](https://github.com/openfga/openfga/pull/1830)
- Breaking: Set a maximum limit on bytes to the WriteAssertions API: 64 KB [#1847](https://github.com/openfga/openfga/pull/1847)

### Fixed
- Check API: internal fixes [#1843](https://github.com/openfga/openfga/pull/1843)
- Correct docker file syntax [#1852](https://github.com/openfga/openfga/pull/1852)
- Performance improvements for Check API:
  - introduce an optimization when the input request relation is pointing to a computed relation [#1793](https://github.com/openfga/openfga/pull/1793)
  - batch calls that compute membership checks and start processing them earlier [#1804](https://github.com/openfga/openfga/pull/1804)
  - performance improvement in wildcard scenarios [#1848](https://github.com/openfga/openfga/pull/1848)
- Performance improvement in tuple validation on reads [#1825](https://github.com/openfga/openfga/pull/1825)

## [1.5.9] - 2024-08-13
[Full changelog](https://github.com/openfga/openfga/compare/v1.5.8...v1.5.9)

### Security
- Address [CVE-2024-42473](https://github.com/openfga/openfga/security/advisories/GHSA-3f6g-m4hr-59h8) - a critical issue where Check API can return incorrect responses. Please see the CVE report for more details.

## [1.5.8] - 2024-08-07
[Full changelog](https://github.com/openfga/openfga/compare/v1.5.7...v1.5.8)

### Added
- Performance improvements for Check API:
  - introduce an optimization when the input request relation is pointing to a computed relation [#1793](https://github.com/openfga/openfga/pull/1793)
  - batch calls that compute membership checks and start processing them earlier [#1804](https://github.com/openfga/openfga/pull/1804)
- Logging number of cache hits for each subproblem of each authorization model for `Check` API calls. Enabled with the `OPENFGA_CHECK_TRACKER_ENABLED` flag. [#1785](https://github.com/openfga/openfga/pull/1785)
- Aliases for issuers and subject validation in OIDC AuthN mode using `OPENFGA_AUTHN_OIDC_ISSUER_ALIASES` and `OPENFGA_AUTHN_OIDC_SUBJECTS` respectively [#1784](https://github.com/openfga/openfga/pull/1784) Thanks @Code2Life!
- Dispatch Throttling for our `ListUsers` API. This can be enabled using `OPENFGA_LIST_USERS_DISPATCH_THROTTLING_ENABLED` and the env variables below. [#1658](https://github.com/openfga/openfga/pull/1658)
  - `OPENFGA_LIST_USERS_DISPATCH_THROTTLING_THRESHOLD` - The number of dispatches allowed before throttling is triggered
  - `OPENFGA_LIST_USERS_DISPATCH_THROTTLING_MAX_THRESHOLD` - The maximum number of dispatches allowed before the request is rejected
  - `OPENFGA_LIST_USERS_DISPATCH_THROTTLING_FREQUENCY` - The frequency at which the deprioritized throttling queue is processed
- Support sending contextual tuples in the Write Assertions API. [#1821](https://github.com/openfga/openfga/pull/1821)

### Fixed
- address `"expected exactly one terminal relation for fast path, received {num}"` error during `Check` for models with type restrictions with and without a condition or with multiple conditions. [#1814](https://github.com/openfga/openfga/pull/1814)

## [1.5.7] - 2024-07-25
### Added
- Support requesting a different consistency option per request in `Check`, `Expand`, `ListObjects`, `ListUsers`, and `Read` [#1764](https://github.com/openfga/openfga/pull/1764)
  - This is currently experimental and needs to be enabled by configuring `OPENFGA_EXPERIMENTALS=enable-consistency-params` or passing `--experimentals enable-consistency-params` to `openfga run`.
  - When `HIGHER_CONSISTENCY` is requested, OpenFGA will skip the check resolver cache. For storage implementors it is recommended to skip any caching and perform a stronger read if `HIGHER_CONSISTENCY` is requested. This can be accessed in the `Consistency` options provided to the relevant methods of the storage interface.
- Start publishing images to `ghcr.io/openfga/openfga` as alternative to DockerHub [#1775](https://github.com/openfga/openfga/pull/1775) - Thanks @JAORMX!
- Performance improvements for parent child relations in Check [#1765](https://github.com/openfga/openfga/pull/1765)
- Performance improvement in Check: computed relations don't consume from the resolution depth quota, don't trigger additional goroutines, and don't get cached [#1786](https://github.com/openfga/openfga/pull/1786)

### Changed
- Update to Go 1.22 in container image [#1776](https://github.com/openfga/openfga/pull/1776) - Thanks @tranngoclam!
- Breaking: Changes to storage interface

  > [!NOTE]
  > The following breaking changes are related to the storage interface. If you are not implementing a storage adaptor, then there are these changes should not impact your usage of OpenFGA.

  - Removal of `PaginationOptions` in favour of a per-method `Options` type [#1732](https://github.com/openfga/openfga/pull/1732)

    The options parameter of type `PaginationOptions` has been replaced with a per-method type that contains a `Pagination` field that contains this data in the following methods:

    - `ReadAuthorizationModels` - Type is `ReadAuthorizationModelsOptions`
    - `ListStores` - Type is `ListStoresOptions`
    - `ReadChanges` - Type is `ReadChangesOptions`
    - `ReadPage` - Type is `ReadPageOptions`

  #### Introduction of new `Options` types to certain methods in the storage interface to facilitate consistency data [#1750](https://github.com/openfga/openfga/pull/1750)

  The following methods have had an options parameter introduced to the method signature to include consistency data, or the existing options parameter has been expanded to hold consistency data.

  This consistency data should be used to help determine whether any form of caching should be used as part of the read performed by the storage adapter.
- `Read` - Added a new parameter of type `ReadOptions`
- `ReadPage` - Added `Consistency` to existing `ReadPageOptions` type
- `ReadUserSetTuples` - Added a new parameter of type `ReadUserSetTuplesOptions`
- `ReadStartingWithUser` - Added a new parameter of type `ReadStartingWithUserOptions`

## [1.5.6] - 2024-07-17
[Full changelog](https://github.com/openfga/openfga/compare/v1.5.5...v1.5.6)

### Added
- Performance improvements to userset subproblem resolutions in Check in certain scenarios [#1734](https://github.com/openfga/openfga/pull/1734)
- Performance improvements to tuple-to-userset subproblem resolutions in Check in certain scenarios [#1735](https://github.com/openfga/openfga/pull/1735)
- Warning when log level set to `none` [#1705](https://github.com/openfga/openfga/pull/1705) - thank you, @Siddhant-K-code!
- Minor performance improvement for queries when model ID not specified [#1754](https://github.com/openfga/openfga/pull/1754)

### Removed
- ListUsers experimental flag (will continue to work if passed) [#1730](https://github.com/openfga/openfga/pull/1730)

### Fixed
- Race condition in ListUsers which could erroneously swallow errors [#1755](https://github.com/openfga/openfga/pull/1755)
- "relation is undefined" error in Check and ListUsers [#1767](https://github.com/openfga/openfga/pull/1767)
- Request ID included with Streaming ListObjects responses [#1636](https://github.com/openfga/openfga/pull/1636)

## [1.5.5] - 2024-06-18
[Full changelog](https://github.com/openfga/openfga/compare/v1.5.4...v1.5.5)

### Added
- Configuring maximum cost for CEL evaluation via `OPENFGA_MAX_CONDITION_EVALUATION_COST` [#1631](https://github.com/openfga/openfga/pull/1631) - thank you, @cmmoran

### Removed
- `excluded_users` from ListUsers response. Further discovery required before being reintroduced. If impacted by this removal, please provide feedback in [issue #1692](https://github.com/openfga/openfga/issues/1692) [#1685](https://github.com/openfga/openfga/pull/1685)

### Fixed
- OTel trace context propagation to grpc-gateway [#1624](https://github.com/openfga/openfga/pull/1624) - thank you, @Zach-Johnson

## [1.5.4] - 2024-05-29
[Full changelog](https://github.com/openfga/openfga/compare/v1.5.3...v1.5.4)

### Added
- ListUsers API which answers the question "what users are related to a specific object?". This feature is experimental and can be enabled by configuring `OPENFGA_EXPERIMENTALS=enable-list-users`. Also see [Performing a ListUsers call](https://openfga.dev/docs/getting-started/perform-list-users) and [ListUsers API docs](https://openfga.dev/api/service#/Relationship%20Queries/ListUsers). **Known Limitation:** Child usersets that are negated from their parent are currently not returned as `excluded_users` [#1433](https://github.com/openfga/openfga/pull/1433)
- ListObjects throttling to manage resource usage of expensive queries. Throttling improves overall query performance by limiting the number of dispatches, which are the recursive sub-operations of a ListObjects query [#1571](https://github.com/openfga/openfga/pull/1571)
- Per-request dispatch throttling threshold configuration via context [#1546](https://github.com/openfga/openfga/pull/1546)
- Self-defining usersets for Check, ListObjects and ListUsers. These are implicit tuples that exist by virtue of set theory. For example, the userset `document:1#viewer` implicitly possess the `viewer` relation for `document:1` [#1521](https://github.com/openfga/openfga/pull/1521)
- Panic recovery handling for all APIs [#1557](https://github.com/openfga/openfga/pull/1557)
- Logging of non-sensitive server configuration on startup [#1609](https://github.com/openfga/openfga/pull/1609)
- Appropriate error codes for throttled requests indicating if a request should be retried [#1552](https://github.com/openfga/openfga/pull/1552)
- Minor performance improvements in Check API by reducing quantity of spans created [#1550](https://github.com/openfga/openfga/pull/1550), [#1589](https://github.com/openfga/openfga/pull/1589)

### Changed
- `request_id` is now same as `trace_id` (e.g. `1e20da43269fe07e3d2ac018c0aad2d1`) if tracing is enabled. Otherwise, remains an UUID (e.g. `38fee7ac-4bfe-4cf6-baa2-8b5ec296b485`) [#1576](https://github.com/openfga/openfga/pull/1576) - thank you, @00chorch

### Removed
- `request_duration_by_query_count_ms` metric [#1579](https://github.com/openfga/openfga/pull/1579)

### Fixed
- Goroutine leak occurring during initial server validation [#1617](https://github.com/openfga/openfga/pull/1617)
- Stricter filtering of invalid tuples with ListObjects [#1563](https://github.com/openfga/openfga/pull/1563)
- Panic on server close if caching is enabled [#1568](https://github.com/openfga/openfga/pull/1568)
- Prevent calling datastore if context has error [#1593](https://github.com/openfga/openfga/pull/1593)

## [1.5.3] - 2024-04-16
[Full changelog](https://github.com/openfga/openfga/compare/v1.5.2...v1.5.3)

### Added
- Apply tags to requests that have been intentionally throttled (https://github.com/openfga/openfga/pull/1531). This will add a new log field titled "throttled" to such requests.

### Changed
- [Modular Models (Schema 1.2)](https://openfga.dev/docs/modeling/modular-models) support is enabled by default and the experimental flag for it has been dropped (https://github.com/openfga/openfga/pull/1520)
- Bumped to Go 1.21.9 (https://github.com/openfga/openfga/pull/1523)

### Fixed
- Panic that occurred on Check API with some authorization models and tuples (https://github.com/openfga/openfga/pull/1517)

### Security
- Patch [CVE-2024-31452](https://github.com/openfga/openfga/security/advisories/GHSA-8cph-m685-6v6r) - a critical issue where Check and ListObjects APIs returns incorrect results for some models and tuples. See the CVE report for more details.

## [1.5.2] - 2024-04-03
[Full changelog](https://github.com/openfga/openfga/compare/v1.5.1...v1.5.2)

### Added
- Add homebrew release job by @chenrui333 ([#780](https://github.com/openfga/openfga/pull/780))

### Fixed
- Fix the count of datastore reads in the Check API ([#1452](https://github.com/openfga/openfga/pull/1452))
- Fix the correct default used for dispatch throttling ([#1479](https://github.com/openfga/openfga/pull/1479))

### Security
- Bumped up the `grpc-health-probe` dependency in the published Docker image to the latest release which fixes some vulnerabilities ([#1507](https://github.com/openfga/openfga/pull/1507))

## [1.5.1] - 2024-03-19
[Full changelog](https://github.com/openfga/openfga/compare/v1.5.0...v1.5.1)

### Added
- Include calls to ListObjects and StreamedListObjects methods in the `dispatch_count` histogram ([#1427](https://github.com/openfga/openfga/pull/1427))
- Added `request_duration_ms` histogram which has `datastore_query_count` and `dispatch_count` as dimensions ([#1444](https://github.com/openfga/openfga/pull/1444))
- Added new flag `OPENFGA_AUTHN_OIDC_ISSUER_ALIASES` to specify oidc issuer aliases ([#1354](https://github.com/openfga/openfga/pull/1354)) - Thanks @le-yams!
- Added experimental support for modular models via `OPENFGA_EXPERIMENTALS=enable-modular-models` ([#1443](https://github.com/openfga/openfga/pull/1443)). This will enable writing models that are split across multiple files.
- Added support for throttling dispatches ([#1440](https://github.com/openfga/openfga/pull/1440)). This will throttle Check requests that are overly complex. You can turn on this feature via OPENFGA_DISPATCH_THROTTLING_ENABLED and configured via OPENFGA_DISPATCH_THROTTLING_THRESHOLD and OPENFGA_DISPATCH_THROTTLING_FREQUENCY
- Thanks @lekaf974 for enhancing NewLogger with builder pattern options ([#1413](https://github.com/openfga/openfga/pull/1413))

### Deprecated
- Histogram `request_duration_by_query_count_ms` will be removed in the next release, in favour of `request_duration_ms` ([#1450](https://github.com/openfga/openfga/pull/1450))

### Fixed
- Throw HTTP 400 when tuple condition is invalid instead of HTTP 500 ([#1420](https://github.com/openfga/openfga/pull/1420))
- Fix model validation which threw error "no entrypoints defined" ([#1422](https://github.com/openfga/openfga/pull/1422))

## [1.5.0] - 2024-03-01
[Full changelog](https://github.com/openfga/openfga/compare/v1.4.3...v1.5.0)

### Added
- Override option for timestamp in JSON logs ([#1330](https://github.com/openfga/openfga/pull/1330)) - thank you, @raj-saxena!
- OpenTelemetry tracing and attributes to check algorithm ([#1331](https://github.com/openfga/openfga/pull/1331), [#1388](https://github.com/openfga/openfga/pull/1388))
- Dispatch count to check response metadata as a query complexity heuristic ([#1343](https://github.com/openfga/openfga/pull/1343))

### Changed
- Breaking: The `AuthorizationModelReadBackend` interface method `FindLatestAuthorizationModelID` has changed to `FindLatestAuthorizationModel` for performance improvements. [#1387](https://github.com/openfga/openfga/pull/1387)

  If you implement your own data store, you will need to make the following change:

  <table>
  <tr>
  <th>Before</th>
  <th>After</th>
  </tr>
  <tr>
  <td>

  ```go
  func (...) FindLatestAuthorizationModelID(ctx context.Context, storeID string) (string, error) {
    //...get model ID
    return modelID, nil
  }
  ```

  </td>
  <td>

  ```go
  func (...) FindLatestAuthorizationModel(ctx context.Context, storeID string) (*openfgav1.AuthorizationModel, error) {
    //...get model
    return model.(*openfgav1.AuthorizationModel), nil
  }
  ```

  </td>
  </tr>
  </table>

### Fixed
- Cycles detected during check now deterministically return with `{allowed:false}` ([#1371](https://github.com/openfga/openfga/pull/1371), [#1372](https://github.com/openfga/openfga/pull/1372))
- Fix incorrect path for gPRC health check ([#1321](https://github.com/openfga/openfga/pull/1321))

## [1.4.3] - 2024-01-26
[Full changelog](https://github.com/openfga/openfga/compare/v1.4.2...v1.4.3)

### Added
- Add ability to close all server resources through `server.Stop()` ([#1318](https://github.com/openfga/openfga/pull/1318))

### Changed
- Increase performance by removing redundant `map.Clone()` calls in model validation ([#1281](https://github.com/openfga/openfga/pull/1281))

### Fixed
- Fix the sorting of contextual tuples when generating a cache key during check ([#1299](https://github.com/openfga/openfga/pull/1299))

### Security
- Patch [CVE-2024-23820](https://github.com/openfga/openfga/security/advisories/GHSA-rxpw-85vw-fx87) - a critical issue
  where issuing many `ListObjects` API calls that hit the `--listObjects-deadline` setting can lead to an out of memory error.
  See the CVE report for more details

## [1.4.2] - 2024-01-10
[Full changelog](https://github.com/openfga/openfga/compare/v1.4.1...v1.4.2)

### Fixed
- Goroutine leak in ListObjects because of a leak in ReverseExpand ([#1297](https://github.com/openfga/openfga/pull/1297))

## [1.4.1] - 2024-01-04
[Full changelog](https://github.com/openfga/openfga/compare/v1.4.0...v1.4.1)

### Added
- Support for cancellation/timeouts when evaluating Conditions ([#1237](https://github.com/openfga/openfga/pull/1237))
- Tracing span info for Condition evaluation ([#1251](https://github.com/openfga/openfga/pull/1251))

### Changed
- Reduce goroutine overhead in ListObjects ([#1173](https://github.com/openfga/openfga/pull/1173))
- Added `openfga` prefix to custom exported Prometheus metrics

  > ⚠️ This change may impact existing deployments of OpenFGA if you're integrating with the metrics reported by OpenFGA.

  Custom metrics reported by the OpenFGA server are now prefixed with `openfga_`. For example, `request_duration_by_query_count_ms ` is now exported as `openfga_request_duration_by_query_count_ms`.

### Fixed
- Resolve rewrites involving exclusion (e.g. `but not`) more deterministically in Check ([#1239](https://github.com/openfga/openfga/pull/1239))
- Record span errors correctly in Check, ListObjects, and StreamedListObjects ([#1231](https://github.com/openfga/openfga/pull/1231))
- Log request validation errors correctly ([#1236](https://github.com/openfga/openfga/pull/1236))

## [1.4.0] - 2023-12-11
[Full changelog](https://github.com/openfga/openfga/compare/v1.3.10...v1.4.0)

### Changed
- Enable support for Conditional Relationship Tuples by default. ([#1220](https://github.com/openfga/openfga/pull/1220))
- Added stricter gRPC server max message size constraints ([#1222](https://github.com/openfga/openfga/pull/1222))

  We changed the default gRPC max message size (4MB) to a stricter 512KB to protect the server from excessively large request `context` fields. This shouldn't impact existing clients since our calculated max message size should be much smaller than 512KB given our other input constraints.

## [1.3.10] - 2023-12-08
[Full changelog](https://github.com/openfga/openfga/compare/v1.3.9...v1.3.10)

### Changed
- Bumped up to Go 1.21.5 ([#1219](https://github.com/openfga/openfga/pull/1219))

### Fixed
- Reorder protobuf fields for persisted Assertions ([#1217](https://github.com/openfga/openfga/pull/1217))

  Assertions written on or after v1.3.8 should be re-written to resolve some binary encoding issues that were introduced.
- Handle floating point conversion errors in conditions ([#1200](https://github.com/openfga/openfga/pull/1200))

## [1.3.9] - 2023-12-05
[Full changelog](https://github.com/openfga/openfga/compare/v1.3.8...v1.3.9)

### Fixed
- Avoid panic when processing a nil set of writes ([#1208](https://github.com/openfga/openfga/pull/1208)) - thanks @stgraber!
- Decoding of null conditions in SQL storage implementations ([#1212](https://github.com/openfga/openfga/pull/1212))

## [1.3.8] - 2023-12-04
[Full changelog](https://github.com/openfga/openfga/compare/v1.3.7...v1.3.8)

### Added
- Experimental support for ABAC Conditional Relationships.

  To enable experimental support for ABAC Conditional Relationships you can pass the `enable-conditions` experimental flag. For example, `openfga run --experimentals=enable-conditions`. The upcoming `v1.4.0` release will introduce official support for this new feature. For more information please see our [official blog post](https://openfga.dev/blog/conditional-tuples-announcement). The `v1.4.0` release will have more official documentation on [openfga.dev](https://openfga.dev/).

  > ⚠️ If you enable experimental support for ABAC and introduce models and/or relationship tuples into the system and then choose to rollback to a prior release, then you may experience unintended side-effects. Care should be taken!
  >
  > Read on for more information.

  If you introduce a model with a condition defined in a relation's type restriction(s) and then rollback to a prior OpenFGA release, then the model will be treated as though the conditioned type restriction did not exist.

  ```
  model
    schema 1.1

  type user

  type document
    relations
      define viewer: [user with somecondition]

  condition somecondition(x: int) {
    x < 100
  }
  ```

  and then you rollback to `v1.3.7` or earlier, then the model above will be treated equivalently to

  ```
  model
    schema 1.1

  type user

  type document
    relations
      define viewer: [user]
  ```

  Likewise, if you write a relationship tuple with a condition and then rollback to a prior release, then the tuple will be treated as an unconditioned tuple.

  ```
  - document:1#viewer@user:jon, {condition: "somecondition"}
  ```

  will be treated equivalently to `document:1#viewer@user:jon` in `v1.3.7` or earlier. That is, `Check(document:1#viewer@user:jon)` would return `{allowed: true}` even though at the tuple was introduced it was conditioned.
- Minimum datastore schema revision check in the server's health check ([#1166](https://github.com/openfga/openfga/pull/1166))

  Each OpenFGA release from here forward will explicitly reference a minimum datastore schema version that is required to run that specific release of OpenFGA. If OpenFGA operators have not migrated up to that revision then the server's health checks will fail.
- Username/password configuration overrides for the `openfga migrate` entrypoint ([#1133](https://github.com/openfga/openfga/pull/1133)). Thanks for the contribution @martin31821!

  Similar to the server's main entrypoint `openfga run`, you can now override the datastore username and password with environment variables. when running the `openfga migrate` utility.
- Healthcheck definitions in Dockerfile ([#1134](https://github.com/openfga/openfga/pull/1134)). Thanks @Siddhant-K-code!

### Changed
- Database iterators yielded by the RelationshipTupleReader storage interface now accept a `context` parameter which allows iteration to be promptly terminated ([#1055](https://github.com/openfga/openfga/pull/1055))

  We have noticed improvements in query performance by adding this because once a resolution path has been found we more quickly cancel any further evaluation by terminating the iterators promptly.
- Improved tuple validation performance with precomputation of TTUs ([#1171](https://github.com/openfga/openfga/pull/1171))
- Refactored the commands in the `pkg/server/commands` package to uniformly use the Options builder pattern ([#1142](https://github.com/openfga/openfga/pull/1142)). Thanks for the contribution @ilaleksin!
- Upgraded to Go `1.21.4` ([#1143](https://github.com/openfga/openfga/pull/1143)). Thanks @tranngoclam!

### Fixed
- If two requests were made with the same request body and contextual tuples but the order of the contextual tuples differed, then the cache key that is produced is now the same.([#1187](https://github.com/openfga/openfga/pull/1187))
- Use `NoOp` TracerProvider if tracing is disabled ([#1139](https://github.com/openfga/openfga/pull/1139) and [#1196](https://github.com/openfga/openfga/pull/1196))

## [1.3.7] - 2023-11-06
[Full changelog](https://github.com/openfga/openfga/compare/v1.3.6...v1.3.7)

### Security
- Bumped up the `grpc-health-probe` dependency to the latest release which fixed some vulnerabilities.

## [1.3.6] - 2023-11-06
[Full changelog](https://github.com/openfga/openfga/compare/v1.3.5...v1.3.6)

### Added
- Provenance manifests generation (`openfga.intoto.jsonl``) for verification of release artifacts with SLSA attestations.

### Changed
- Removed the experimental flag `check-query-cache`. If you wish to enable the Check query cache you no longer need the experimental flag.

## [1.3.5] - 2023-10-27
[Full changelog](https://github.com/openfga/openfga/compare/v1.3.4...v1.3.5)

### Added
- Export metrics from MySQL and Postgres ([#1023](https://github.com/openfga/openfga/pull/1023))

  To export datastore metrics, set `OPENFGA_METRICS_ENABLED=true` and `OPENFGA_DATASTORE_METRICS_ENABLED=true`.

### Changed
- Write Authorization Models in a single database row ([#1030](https://github.com/openfga/openfga/pull/1030))

  :warning: In order to avoid downtime, we recommend upgrading to at least v1.3.3 _before_ upgrading to v1.3.5.

  This is the second of a series of releases that will progressively introduce changes via code and database migrations that will allow authorization models to be stored in a single database row.

  See [here for more details](https://github.com/openfga/openfga/issues/1025).

### Fixed
- Return all results when `OPENFGA_LIST_OBJECTS_MAX_RESULTS=0` ([#1067](https://github.com/openfga/openfga/pull/1067))
- Promptly return if max results are met before deadline in ListObjects ([#1064](https://github.com/openfga/openfga/pull/1064))
- Fix sort order on ReadChanges ([#1079](https://github.com/openfga/openfga/pull/1079))

## [1.3.4] - 2023-10-17
[Full changelog](https://github.com/openfga/openfga/compare/v1.3.3...v1.3.4)

### Changed
- Bumped up to Go 1.21.3 ([#1060](https://github.com/openfga/openfga/pull/1060))

### Fixed
- Incorrect string in model validation error message ([#1057](https://github.com/openfga/openfga/pull/1057))
- Incorrect results can be returned by Check API when passing in contextual tuples and the `check-query-cache` experimental flag is turned on ([#1059](https://github.com/openfga/openfga/pull/1059))

### Security
- Patches [CVE-2023-45810](https://github.com/openfga/openfga/security/advisories/GHSA-hr4f-6jh8-f2vq). See the CVE for more details

## [1.3.3] - 2023-10-04
[Full changelog](https://github.com/openfga/openfga/compare/v1.3.2...v1.3.3)

### Added
- Configurable size limit for Authorization Models ([#1032](https://github.com/openfga/openfga/pull/1032))

  We've introduced a new size limit for authorization models, provided a consistent behavior across datastores, which defaults to `256KB`. This can be configured by using the `--max-authorization-model-size-in-bytes` flag.

### Changed
- Move standalone server config defaults ([#1036](https://github.com/openfga/openfga/pull/1036))
- Persist Authorization Models serialized protobuf in the database ([#1028](https://github.com/openfga/openfga/pull/1028))

  In the next series of releases will progressively introduce changes via code and database migrations that will allow authorization models to be stored in a single database row.

  See [here for more details](https://github.com/openfga/openfga/issues/1025).

### Fixed
- Reduce use of GOB in encoded cache key ([#1029](https://github.com/openfga/openfga/pull/1029))

## [1.3.2] - 2023-08-25
### Added
- Support TLS for OTLP trace endpoint ([#885](https://github.com/openfga/openfga/pull/885)) - thanks @matoous
- Configurable limits to database reads per ListObjects query ([#967](https://github.com/openfga/openfga/pull/967))
- Datastore query count labels to traces and query latency histogram in ListObjects ([#959](https://github.com/openfga/openfga/pull/959))
- GitHub workflow to check Markdown links ([#1016](https://github.com/openfga/openfga/pull/1016)) - thanks @sanketrai1

### Changed
- Use slices and maps packages from go1.21 ([#969](https://github.com/openfga/openfga/pull/969)) - thanks @tranngoclam
- Moved request validations to RPC handlers so library integrations benefit ([#975](https://github.com/openfga/openfga/pull/975), [#998](https://github.com/openfga/openfga/pull/998))
- Refactored internal usages of ConnectedObjects to ReverseExpand ([#968](https://github.com/openfga/openfga/pull/968))
- Expose validation middleware ([#1005](https://github.com/openfga/openfga/pull/1005))
- Upgrade grpc validator middleware to the latest v2 package ([#1019](https://github.com/openfga/openfga/pull/1019)) - thanks @tranngoclam

### Fixed
- Change response code to internal error for concurrency conflicts ([#1011](https://github.com/openfga/openfga/pull/1011))

### Security
- Patches [CVE-2023-43645](https://github.com/openfga/openfga/security/advisories/GHSA-2hm9-h873-pgqh) - see the CVE for more details

  **[BREAKING]** If your model contained cycles or a relation definition that has the relation itself in its evaluation path, then Checks and queries that require evaluation will no longer be evaluated on v1.3.2+ and will return errors instead. You will need to update your models to remove the cycles.

## [1.3.1] - 2023-08-23
### Added
- Count datastore queries involved in Check resolution metadata ([#880](https://github.com/openfga/openfga/pull/880))

  OpenFGA request logs and traces will now include a field `datastore_query_count` that shows how many queries were involved in a single Check resolution.
- Histogram metric to report the `datastore_query_count` per Check ([#924](https://github.com/openfga/openfga/pull/932))

  This new metric can be used to report percentiles of the number of database queries required to resolve Check requests.
- Check request duration histogram labeled by method and datastore query count ([#950](https://github.com/openfga/openfga/pull/950))

  The `request_duration_by_query_count_ms` metric reports the total request duration (in ms) labelled by the RPC method and ranges of observations for the `datastore_query_count`. This metrics allows operators of an OpenFGA server to report request duration percentiles for Check requests based on the number of database queries that were required to resolve the query.
- Optimize Check to avoid database lookups in some scenarios ([#932](https://github.com/openfga/openfga/pull/932))
- CachedCheckResolver for caching Check subproblems ([#891](https://github.com/openfga/openfga/pull/891))

  This experimental feature adds new caching capabilities to the OpenFGA server. It is an "opt-in" feature and thus must be enabled. To enable this feature you must specify the experimental flag `check-query-cache` and set the `--check-query-cache-enabled=true` flag.

  ```shell
  openfga run --experimentals check-query-cache --check-query-cache-enabled=true
  ```
- Server request logs now include the `user-agent` ([#943](https://github.com/openfga/openfga/pull/943))

### Changed
- Default Check and ListObjects concurrency read limits ([#916](https://github.com/openfga/openfga/pull/916))

  In our last release [v1.3.0](https://github.com/openfga/openfga/releases/tag/v1.3.0) we modified the default behavior of Check and ListObjects such that it limits/restricts the degree of concurrency that is allowed for a single request. This change was unintended. This release reverts the default behavior back to unbounded concurrency limits (the prior default). The change mostly affects those using OpenFGA as a library.
- Bumped up to Go 1.21 ([#952](https://github.com/openfga/openfga/pull/952))

### Security
- Patches [CVE-2023-40579](https://github.com/openfga/openfga/security/advisories/GHSA-jcf2-mxr2-gmqp) - see the CVE for more details

## [1.3.0] - 2023-08-01
[Full changelog](https://github.com/openfga/openfga/compare/v1.2.0...v1.3.0)

### Added
- Bounded concurrency limiter for Check and ListObjects queries ([#860](https://github.com/openfga/openfga/pull/860), [#887](https://github.com/openfga/openfga/pull/887))
  New server configurations can be provided to limit/bound the amount of concurrency that is allowed during query evaluation. These settings can help reduce the impact/burden that a single query (e.g. Check, ListObjects, etc..) can have on the underlying database and OpenFGA server.

  - `--maxConcurrentReadsForListObjects` - The maximum allowed number of concurrent reads in a single ListObjects query.

  - `--maxConcurrentReadsForCheck` - The maximum allowed number of concurrent reads in a single Check query.

  - `--resolveNodeBreadthLimit` - Defines how many nodes on a given level can be evaluated concurrently in a Check resolution tree.
- Jaeger persistent storage for traces in `docker-compose.yaml` ([#888](https://github.com/openfga/openfga/pull/888)) - thanks @Azanul

### Changed
- [BREAKING] Imports for OpenFGA protobuf API dependencies ([#898](https://github.com/openfga/openfga/pull/898))

  - **Problem** - Previously we depended on [Buf remote generated packages](https://buf.build/docs/bsr/remote-packages/overview), but they recently deprecated protobuf imports served from the `go.buf.build` domain (see [Migrate from remote generation alpha](https://buf.build/docs/migration-guides/migrate-remote-generation-alpha)). OpenFGA builds are currently broken as a result of this.
  - **Change** - We switched our protobuf API dependency from `go.buf.build/openfga/go/openfga/api/openfga/v1` to `github.com/openfga/api/proto/openfga/v1`. So we no longer use Buf remote generated packages in favor of packages we managed in the [`openfga/api`](https://github.com/openfga/api) repository. This fixes existing build issues.
  - **Impact** - Developers using the OpenFGA as a library or the gRPC API must change their protobuf dependency from `go.buf.build/openfga/go/openfga/api/openfga/v1` to `github.com/openfga/api/proto/openfga/v1`. A global find/replace and package dependency update should fix it. Here's a diff demonstrating the changes for a Go app, for example:

    ```go
    import (
      ...
    - openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
    + openfgav1 "github.com/openfga/api/proto/openfga/v1"
    )
    ```
- Refactor the `Server` constructor to use the options builder pattern ([#833](https://github.com/openfga/openfga/pull/833))

  ```go
  import (
    openfga "github.com/openfga/openfga/pkg/server"
  )

  s := openfga.New(
    &server.Dependencies{...},
    &server.Config{...},
  )
  ```

  becomes

  ```go
  import (
    openfga "github.com/openfga/openfga/pkg/server"
  )

  var opts []openfga.OpenFGAServiceV1Option
  s := openfga.MustNewServerWithOpts(opts...)
  ```

### Fixed
- Disable default debug level-logging in `retryablehttp` client ([#882](https://github.com/openfga/openfga/pull/882)) - thanks @KlausVii

## [1.2.0] - 2023-06-30
[Full changelog](https://github.com/openfga/openfga/compare/v1.1.1...v1.2.0)

### Added
- Optimizations for [ListObjects](https://openfga.dev/api/service#/Relationship%20Queries/ListObjects) and [StreamedListObjects](https://openfga.dev/api/service#/Relationship%20Queries/StreamedListObjects) for models involving intersection (`and`) and exclusion (`but not`) ([#797](https://github.com/openfga/openfga/pull/797))

### Changed
- Cache model validation results on first model load ([#831](https://github.com/openfga/openfga/pull/831))
- Cache inflight requests when looking up any authorization model ([#831](https://github.com/openfga/openfga/pull/831))
- Update postgres max connections in docker compose file ([#829](https://github.com/openfga/openfga/pull/829))

## [1.1.1] - 2023-06-26
[Full changelog](https://github.com/openfga/openfga/compare/v1.1.0...v1.1.1)

### Added
- Official Homebrew installation instructions ([#781](https://github.com/openfga/openfga/pull/781)) - thanks @chenrui333
- The `--verbose` flag has been added to the `openfga migrate` command ([#776](https://github.com/openfga/openfga/pull/776))
- The `openfga validate-models` CLI command has been introduced to validate all models across all stores ([#817](https://github.com/openfga/openfga/pull/817))

### Changed
- Updated the version of the `grpc-health-probe` binary included in OpenFGA builds ([#784](https://github.com/openfga/openfga/pull/784))
- Cache inflight requests when looking up the latest authorization model ([#820](https://github.com/openfga/openfga/pull/820))

### Fixed
- Validation of models with non-zero entrypoints ([#802](https://github.com/openfga/openfga/pull/802))
- Remove unintended newlines in model validation error messages ([#816](https://github.com/openfga/openfga/pull/816)) - thanks @Galzzly

### Security
- Patches [CVE-2023-35933](https://github.com/openfga/openfga/security/advisories/GHSA-hr9r-8phq-5x8j) - additional model validations are now applied to models that can lead to the vulnerability. See the CVE report for more details, and don't hesitate to reach out if you have questions.

## [1.1.0] - 2023-05-15
[Full changelog](https://github.com/openfga/openfga/compare/v1.0.1...v1.1.0)

### Added
- Streaming ListObjects has no limit in number of results returned ([#733](https://github.com/openfga/openfga/pull/733))
- Add Homebrew release stage to goreleaser's release process ([#716](https://github.com/openfga/openfga/pull/716))

### Changed
- [BREAKING] The flags to turn on writing and evaluation of `v1.0` models have been dropped ([#763](https://github.com/openfga/openfga/pull/763))

### Fixed
- Avoid DB connection churning in unoptimized ListObjects ([#711](https://github.com/openfga/openfga/pull/711))
- Ensure ListObjects respects configurable ListObjectsDeadline ([#704](https://github.com/openfga/openfga/pull/704))
- In Write, throw 400 instead of 500 error if auth model ID not found ([#725](https://github.com/openfga/openfga/pull/725))
- Performance improvements when loading the authorization model ([#726](https://github.com/openfga/openfga/pull/726))
- Ensure Check evaluates deterministically on the eval boundary case ([#732](https://github.com/openfga/openfga/pull/732))

## [1.0.1] - 2023-04-18
[Full changelog](https://github.com/openfga/openfga/compare/v1.0.0...v1.0.1)

### Fixed
- Correct permission and location for gRPC health probe in Docker image (#697)

## [1.0.0] - 2023-04-14
[Full changelog](https://github.com/openfga/openfga/compare/v0.4.3...v1.0.0)

### Fixed
- MySQL migration script errors during downgrade (#664)

## [0.4.3] - 2023-04-12
[Full changelog](https://github.com/openfga/openfga/compare/v0.4.2...v0.4.3)

### Added
- OpenFGA with Postgres is now considered stable and ready for production usage.
- Release artifacts are now signed and include a Software Bill of Materials (SBOM) ([#683](https://github.com/openfga/openfga/pull/683))

  The SBOM (Software Bill of Materials) is included in each GitHub release using [Syft](https://github.com/anchore/syft) and is exported in [SPDX](https://spdx.dev) format.

  Developers will be able to verify the signature of the release artifacts with the following workflow(s):

  ```shell
  wget https://github.com/openfga/openfga/releases/download/<tag>/checksums.txt

  cosign verify-blob \
    --certificate-identity 'https://github.com/openfga/openfga/.github/workflows/release.yml@refs/tags/<tag>' \
    --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
    --cert https://github.com/openfga/openfga/releases/download/<tag>/checksums.txt.pem \
    --signature https://github.com/openfga/openfga/releases/download/<tag>/checksums.txt.sig \
    ./checksums.txt
  ```

  If the `checksums.txt` validation succeeds, it means the checksums included in the release were not tampered with, so we can use it to verify the hashes of other files using the `sha256sum` utility. You can then download any file you want from the release, and verify it with, for example:

  ```shell
  wget https://github.com/openfga/openfga/releases/download/<tag>/openfga_<version>_linux_amd64.tar.gz.sbom
  wget https://github.com/openfga/openfga/releases/download/<tag>/openfga_<version>_linux_amd64.tar.gz

  sha256sum --ignore-missing -c checksums.txt
  ```

  And both should say "OK".

  You can then inspect the .sbom file to see the entire dependency tree of the binary.

  Developers can also verify the Docker image signature. Cosign actually embeds the signature in the image manifest, so we only need the public key used to sign it in order to verify its authenticity:

  ```shell
  cosign verify -key cosign.pub openfga/openfga:<tag>
  ```
- `openfga migrate` now accepts reading configuration from a config file and environment variables like the `openfga run` command ([#655](https://github.com/openfga/openfga/pull/655)) - thanks @suttod!
- The `--trace-service-name` command-line flag has been added to allow for customizing the service name in traces ([#652](https://github.com/openfga/openfga/pull/652)) - thanks @jmiettinen

### Changed
- Bumped up to Go version 1.20 ([#664](https://github.com/openfga/openfga/pull/664))
- Default model schema versions to 1.1 ([#669](https://github.com/openfga/openfga/pull/669))

  In preparation for sunsetting support for models with schema version 1.0, the [WriteAuthorizationModel API](https://openfga.dev/api/service#/Authorization%20Models/WriteAuthorizationModel) will now interpret any model provided to it as a 1.1 model if the `schema_version` field is omitted in the request. This shouldn't affect default behavior since 1.0 model support is enabled by default.

### Fixed
- Postgres and MySQL implementations have been fixed to avoid ordering relationship tuple queries by `ulid` when it is not needed. This can improve read query performance on larger OpenFGA stores ([#677](https://github.com/openfga/openfga/pull/677))
- Synchronize concurrent access to in-memory storage iterators ([#587](https://github.com/openfga/openfga/pull/587))
- Improve error logging in the `openfga migrate` command ([#663](https://github.com/openfga/openfga/pull/663))
- Fix middleware ordering so that `requestid` middleware is registered earlier ([#662](https://github.com/openfga/openfga/pull/662))

## [0.4.2] - 2023-03-17
[Full changelog](https://github.com/openfga/openfga/compare/v0.4.1...v0.4.2)

### Fixed
- Correct migration path for mysql in `openfga migrate` ([#644](https://github.com/openfga/openfga/pull/664))

## [0.4.1] - 2023-03-16
[Full changelog](https://github.com/openfga/openfga/compare/v0.4.0...v0.4.1)

The `v0.4.1` release includes everything in `v0.4.0` which includes breaking changes, please read the [`v0.4.0` changelog entry](#040---2023-03-15) for more details.

### Fixed
- Fix ListObjects not returning objects a user has access to in some cases (openfga/openfga#637)

## [0.4.0] - 2023-03-15
[Full changelog](https://github.com/openfga/openfga/compare/v0.3.7...v0.4.0)

> Note: the 0.4.0 release was held due to issues discovered after the release was cut.

### Added
- Add OpenFGA version command to the CLI ([#625](https://github.com/openfga/openfga/pull/625))
- Add `timeout` flag to `migrate` command ([#634](https://github.com/openfga/openfga/pull/634))

### Removed
- [BREAKING] Disable schema 1.0 support, except if appropriate flags are set (openfga/openfga#613)
  - As of this release, OpenFGA no longer allows writing or evaluating schema `v1.0` models by default. If you need support for it for now, you can use the:
    - `OPENFGA_ALLOW_WRITING_1_0_MODELS`: set to `true` to allow `WriteAuthorizationModel` to accept schema `v1.0` models.
    - `OPENFGA_ALLOW_EVALUATING_1_0_MODELS`: set to `true` to allow `Check`, `Expand`, `ListObjects`, `Write` and `WriteAssertions` that target schema `v1.0` models.
    - `ReadAuthorizationModel`, `ReadAuthorizationModels` and `ReadAssertions` are unaffected and will continue to work regardless of the target model schema version.
  - Note that these flags will be removed and support fully dropped in a future release. Read the [Schema v1.0 Deprecation Timeline](https://openfga.dev/docs/modeling/migrating/migrating-schema-1-1#deprecation-timeline) for more details.

### Fixed
- Improve the speed of Check for 1.1 models by using type restrictions (([#545](https://github.com/openfga/openfga/pull/545), ([#596](https://github.com/openfga/openfga/pull/596))
- Various important fixes to the experimental ListObjects endpoint
  - Improve readUsersets query by dropping unnecessary sorting ([#631](https://github.com/openfga/openfga/pull/631),([#633](https://github.com/openfga/openfga/pull/633))
  - Fix null pointer exception if computed userset does not exist ([#572](https://github.com/openfga/openfga/pull/572))
  - Fix race condition in memory store ([#585](https://github.com/openfga/openfga/pull/585))
  - Ensure no objects returned that would not have been allowed in Checks ([#577](https://github.com/openfga/openfga/pull/577))
  - Reverse expansion with indirect computed userset relationship ([#611](https://github.com/openfga/openfga/pull/611))
  - Improved tests ([#582](https://github.com/openfga/openfga/pull/582), [#599](https://github.com/openfga/openfga/pull/599), [#601](https://github.com/openfga/openfga/pull/601), [#620](https://github.com/openfga/openfga/pull/620))
- Tuning of OTEL parameters ([#570](https://github.com/openfga/openfga/pull/570))
- Fix tracing in Check API ([#627](https://github.com/openfga/openfga/pull/627))
- Use chainguard images in Dockerfile ([#628](https://github.com/openfga/openfga/pull/628))

## [0.3.7] - 2023-02-21
[Full changelog](https://github.com/openfga/openfga/compare/v0.3.6...v0.3.7)

### Fixed
- Contextual tuple propagation in the unoptimized ListObjects implementation ([#565](https://github.com/openfga/openfga/pull/565))

## [0.3.6] - 2023-02-16
[Full changelog](https://github.com/openfga/openfga/compare/v0.3.5...v0.3.6)

Re-release of `v0.3.5` because the go module proxy cached a prior commit of the `v0.3.5` tag.

## [0.3.5] - 2023-02-14
[Full changelog](https://github.com/openfga/openfga/compare/v0.3.4...v0.3.5)

### Added
- [`grpc-health-probe`](https://github.com/grpc-ecosystem/grpc-health-probe) for Health Checks ([#520](https://github.com/openfga/openfga/pull/520))

  OpenFGA containers now include an embedded `grpc_health_probe` binary that can be used to probe the Health Check endpoints of OpenFGA servers. Take a look at the [docker-compose.yaml](https://github.com/openfga/openfga/blob/main/docker-compose.yaml) file for an example.
- Improvements to telemetry: logging, tracing, and metrics ([#468](https://github.com/openfga/openfga/pull/468), [#514](https://github.com/openfga/openfga/pull/514), [#517](https://github.com/openfga/openfga/pull/517), [#522](https://github.com/openfga/openfga/pull/522))

  - We have added Prometheus as the standard metrics provided for OpenFGA and provide a way to launch Grafana to view the metrics locally. See [docker-compose.yaml](https://github.com/openfga/openfga/blob/main/docker-compose.yaml) for more information.

  - We've improved the attributes of various trace spans and made sure that trace span names align with the functions they decorate.

  - Our logging has been enhanced with more logged fields including request level logging which includes a `request_id` and `store_id` field in the log message.

  These features will allow operators of OpenFGA to improve their monitoring and observability processes.
- Nightly releases ([#508](https://github.com/openfga/openfga/pull/508)) - thanks @Siddhant-K-code!

  You should now be able to run nightly releases of OpenFGA using `docker pull openfga/openfga:nightly`

### Fixed
- Undefined computed relations on tuplesets now behave properly ([#532](https://github.com/openfga/openfga/pull/532))

  If you had a model involving two different computed relations on the same tupleset, then it's possible you may have received an internal server error if one of the computed relations was undefined. For example,

  ```
  type document
    relations
      define parent as self
      define viewer as x from parent or y from parent

  type folder
    relations
      define x as self

  type org
    relations
      define y as self
  ```

  Given the tuple `{ user: "org:contoso", relation: "parent", object: "document:1" }`, then `Check({ user: "jon", relation: "viewer", object: "document:1" })` would return an error prior to this fix because the `x` computed relation on the `document#parent` tupleset relation is not defined for the `org` object type.
- Eliminate duplicate objects in ListObjects response ([#528](https://github.com/openfga/openfga/pull/528))

## [0.3.4] - 2023-02-02
[Full changelog](https://github.com/openfga/openfga/compare/v0.3.3...v0.3.4)

### Added
- Added OpenTelemetry tracing ([#499](https://github.com/openfga/openfga/pull/499))

### Removed
- The ReadTuples endpoint has been removed ([#495](https://github.com/openfga/openfga/pull/495)). Please use [Read](https://openfga.dev/api/service#/Relationship%20Tuples/Read) with no tuple key instead (e.g. `POST /stores/<store_id>/read` with `{}` as the body).

### Fixed
- Fixed the environment variable mapping ([#498](https://github.com/openfga/openfga/pull/498)). For the full list of environment variables see [.config-schema.json](https://github.com/openfga/openfga/blob/main/.config-schema.json).
- Fix for stack overflow error in ListObjects ([#506](https://github.com/openfga/openfga/pull/506)). Thank you for reporting the issue @wonderbeyond!

## [0.3.3] - 2023-01-31
[Full changelog](https://github.com/openfga/openfga/compare/v0.3.2...v0.3.3)

### Added
- Environment variable names have been updated ([#472](https://github.com/openfga/openfga/pull/472)).

  For example, `OPENFGA_MAX_TUPLES_PER_WRITE` instead of `OPENFGA_MAXTUPLESPERWRITE`.

  For the full list please see [.config-schema.json](https://github.com/openfga/openfga/blob/main/.config-schema.json).

  The old form still works but is considered deprecated and should not be used anymore.
- Optimized ListObjects is now on by default ([#489](https://github.com/openfga/openfga/pull/489)) (`--experimentals="list-objects-optimized"` is no longer needed)
- Avoid connection churn in our datastore implementations ([#474](https://github.com/openfga/openfga/pull/474))
- The default values for `OPENFGA_DATASTORE_MAX_OPEN_CONNS` and `OPENFGA_DATASTORE_MAX_IDLE_CONNS` have been set to 30 and 10 respectively ([#492](https://github.com/openfga/openfga/pull/492))

### Fixed
- ListObjects should no longer return duplicates ([#475](https://github.com/openfga/openfga/pull/475))

## [0.3.2] - 2023-01-18
[Full changelog](https://github.com/openfga/openfga/compare/v0.3.1...v0.3.2)

### Added
- OpenTelemetry metrics integration with an `otlp` exporter ([#360](https://github.com/openfga/openfga/pull/360)) - thanks @AlexandreBrg!

  To export OpenTelemetry metrics from an OpenFGA instance you can now provide the `otel-metrics` experimental flag along with the `--otel-telemetry-endpoint` and `--otel-telemetry-protocol` flags. For example,

  ```
  ./openfga run --experimentals=otel-metrics --otel-telemetry-endpoint=127.0.0.1:4317 --otel-telemetry-protocol=http
  ```

  For more information see the official documentation on [Experimental Features](https://openfga.dev/docs/getting-started/setup-openfga/docker#experimental-features) and [Telemetry](https://openfga.dev/docs/getting-started/setup-openfga/docker#telemetry).
- Type-bound public access support in the optimized ListObjects implementation (when the `list-objects-optimized` experimental feature is enabled) ([#444](https://github.com/openfga/openfga/pull/444))

### Fixed
- Tuple validations for models with schema version 1.1 ([#446](https://github.com/openfga/openfga/pull/446), [#457](https://github.com/openfga/openfga/pull/457))
- Evaluate rewrites on nested usersets in the optimized ListObjects implementation ([#432](https://github.com/openfga/openfga/pull/432))

## [0.3.1] - 2022-12-19
[Full changelog](https://github.com/openfga/openfga/compare/v0.3.0...v0.3.1)

### Added
- Datastore configuration flags to control connection pool settings
  `--datastore-max-open-conns`
  `--datastore-max-idle-conns`
  `--datastore-conn-max-idle-time`
  `--datastore-conn-max-lifetime`
  These flags can be used to fine-tune database connections for your specific deployment of OpenFGA.
- Log level configuration flags
  `--log-level` (can be one of ['none', 'debug', 'info', 'warn', 'error', 'panic', 'fatal'])
- Support for Experimental Feature flags
  A new flag `--experimentals` has been added to enable certain experimental features in OpenFGA. For more information see [Experimental Features](https://openfga.dev/docs/getting-started/setup-openfga/docker#experimental-features).

### Security
- Patches [CVE-2022-23542](https://github.com/openfga/openfga/security/advisories/GHSA-m3q4-7qmj-657m) - relationship reads now respect type restrictions from prior models ([#422](https://github.com/openfga/openfga/pull/422)).

## [0.3.0] - 2022-12-12
[Full changelog](https://github.com/openfga/openfga/compare/v0.2.5...v0.3.0)

### Added
- Support for [v1.1 JSON Schema](https://github.com/openfga/rfcs/blob/feat/add-type-restrictions-to-json-syntax/20220831-add-type-restrictions-to-json-syntax.md)

  - You can now write your models in the [new DSL](https://github.com/openfga/rfcs/blob/type-restriction-dsl/20221012-add-type-restrictions-to-dsl-syntax.md)
    which the Playground and the [syntax transformer](https://github.com/openfga/syntax-transformer) can convert to the
    JSON syntax. Schema v1.1 allows for adding type restrictions to each assignable relation, and it can be used to
    indicate cases such as "The folder's parent must be a folder" (and so not a user or a document).
    - This change also comes with breaking changes to how `*` and `<type>:*` are treated:
    - `<type>:*` is interpreted differently according to the model version. v1.0 will interpret it as a object of type
      `<type>` and id `*`, whereas v1.1 will interpret is as all objects of type `<type>`.
    - `*` is still supported in v1.0 models, but not supported in v1.1 models. A validation error will be thrown when
      used in checks or writes and it will be ignored when evaluating.
  - Additionally, the change to v1.1 models allows us to provide more consistent validation when writing the model
    instead of when issuing checks.

  :warning: Note that with this release **models with schema version 1.0 are now considered deprecated**, with the plan to
  drop support for them over the next couple of months, please migrate to version 1.1 when you can. Read more about
  [migrating to the new syntax](https://openfga.dev/docs/modeling/migrating/migrating-schema-1-1).

### Changed
- ListObjects changes

  The response has changed to include the object type, for example:

  ```json
  { "object_ids": ["a", "b", "c"] }
  ```

  to

  ```json
  { "objects": ["document:a", "document:b", "document:c"] }
  ```

  We have also improved validation and fixed support for Contextual Tuples that were causing inaccurate responses to be
  returned.

### Deprecated
- The ReadTuples API endpoint is now marked as deprecated, and support for it will be dropped shortly. Please use Read with no tuple key instead.

## [0.2.5] - 2022-11-07
### Added
- Multi-platform container build manifests to releases ([#323](https://github.com/openfga/openfga/pull/323))

### Fixed
- Read RPC returns correct error when authorization model id is not found ([#312](https://github.com/openfga/openfga/pull/312))
- Throw error if `http.upstreamTimeout` config is less than `listObjectsDeadline` ([#315](https://github.com/openfga/openfga/pull/315))

### Security
- Patches [CVE-2022-39352](https://github.com/openfga/openfga/security/advisories/GHSA-3gfj-fxx4-f22w)

## [0.2.4] - 2022-10-24
### Added
- Update Go to 1.19

### Fixed
- TLS certificate config path mappings ([#285](https://github.com/openfga/openfga/pull/285))
- Error message when a `user` field is invalid ([#278](https://github.com/openfga/openfga/pull/278))
- host:port mapping with unspecified host ([#275](https://github.com/openfga/openfga/pull/275))
- Wait for connection to postgres before starting ([#270](https://github.com/openfga/openfga/pull/270))

### Security
- Patches [CVE-2022-39340](https://github.com/openfga/openfga/security/advisories/GHSA-95x7-mh78-7w2r), [CVE-2022-39341](https://github.com/openfga/openfga/security/advisories/GHSA-vj4m-83m8-xpw5), and [CVE-2022-39342](https://github.com/openfga/openfga/security/advisories/GHSA-f4mm-2r69-mg5f)

## [0.2.3] - 2022-10-05
### Added
- Support for MySQL storage backend ([#210](https://github.com/openfga/openfga/pull/210)). Thank you @MidasLamb!
- Allow specification of type restrictions in authorization models ([#223](https://github.com/openfga/openfga/pull/223)). Note: Type restriction is not enforced yet, this just allows storing them.
- Tuple validation against type restrictions in Write API ([#232](https://github.com/openfga/openfga/pull/232))
- Upgraded the Postgres storage backend to use pgx v5 ([#225](https://github.com/openfga/openfga/pull/225))

### Fixed
- Close database connections after migration ([#252](https://github.com/openfga/openfga/pull/252))
- Race condition in streaming ListObjects ([#255](https://github.com/openfga/openfga/pull/255), [#256](https://github.com/openfga/openfga/pull/256))

## [0.2.2] - 2022-09-15
### Fixed
- Reject direct writes if only indirect relationship allowed ([#114](https://github.com/openfga/openfga/pull/114)). Thanks @dblclik!
- Log internal errors at the grpc layer ([#222](https://github.com/openfga/openfga/pull/222))
- Authorization model validation ([#224](https://github.com/openfga/openfga/pull/224))
- Bug in `migrate` command ([#236](https://github.com/openfga/openfga/pull/236))
- Skip malformed tuples involving tuple to userset definitions ([#234](https://github.com/openfga/openfga/pull/234))

## [0.2.1] - 2022-08-30
### Added
- Support Check API calls on userset types of users ([#146](https://github.com/openfga/openfga/pull/146))
- Add backoff when connecting to Postgres ([#188](https://github.com/openfga/openfga/pull/188))

### Fixed
- Improve logging of internal server errors ([#193](https://github.com/openfga/openfga/pull/193))
- Use Postgres in the sample Docker Compose file ([#195](https://github.com/openfga/openfga/pull/195))
- Emit authorization errors ([#144](https://github.com/openfga/openfga/pull/144))
- Telemetry in Check and ListObjects APIs ([#177](https://github.com/openfga/openfga/pull/177))
- ListObjects API: respect the value of ListObjectsMaxResults ([#181](https://github.com/openfga/openfga/pull/181))

## [0.2.0] - 2022-08-12
### Added
- [ListObjects API](https://openfga.dev/api/service#/Relationship%20Queries/ListObjects)

  The ListObjects API provides a way to list all of the objects (of a particular type) that a user has a relationship with. It provides a solution to the [Search with Permissions (Option 3)](https://openfga.dev/docs/interacting/search-with-permissions#option-3-build-a-list-of-ids-then-search) use case for access-aware filtering on smaller object collections. It implements the [ListObjects RFC](https://github.com/openfga/rfcs/blob/main/20220714-listObjects-api.md).

  This addition brings with it two new server configuration options `--listObjects-deadline` and `--listObjects-max-results`. These configurations help protect the server from excessively long lived and large responses.

  > ⚠️ If `--listObjects-deadline` or `--listObjects-max-results` are provided, the endpoint may only return a subset of the data. If you provide the deadline but returning all of the results would take longer than the deadline, then you may not get all of the results. If you limit the max results to 1, then you'll get at most 1 result.
- Support for presharedkey authentication in the Playground ([#141](https://github.com/openfga/openfga/pull/141))

  The embedded Playground now works if you run OpenFGA using one or more preshared keys for authentication. OIDC authentication remains unsupported for the Playground at this time.

## [0.1.7] - 2022-07-29
### Added
- `migrate` CLI command ([#56](https://github.com/openfga/openfga/pull/56))

  The `migrate` command has been added to the OpenFGA CLI to assist with bootstrapping and managing database schema migrations. See the usage for more info.

  ```
  ➜ openfga migrate -h
  The migrate command is used to migrate the database schema needed for OpenFGA.

  Usage:
    openfga migrate [flags]

  Flags:
        --datastore-engine string   (required) the database engine to run the migrations for
        --datastore-uri string      (required) the connection uri of the database to run the migrations against (e.g. 'postgres://postgres:password@localhost:5432/postgres')
    -h, --help                      help for migrate
        --version uint              the version to migrate to (if omitted the latest schema will be used)
  ```

## [0.1.6] - 2022-07-27
### Fixed
- Issue with embedded Playground assets found in the `v0.1.5` released docker image ([#129](https://github.com/openfga/openfga/pull/129))

## [0.1.5] - 2022-07-27
### Added
- Support for defining server configuration in `config.yaml`, CLI flags, or env variables ([#63](https://github.com/openfga/openfga/pull/63), [#92](https://github.com/openfga/openfga/pull/92), [#100](https://github.com/openfga/openfga/pull/100))

  `v0.1.5` introduces multiple ways to support a variety of server configuration strategies. You can configure the server with CLI flags, env variables, or a `config.yaml` file.

  Server config will be loaded in the following order of precedence:

  - CLI flags (e.g. `--datastore-engine`)
  - env variables (e.g. `OPENFGA_DATASTORE_ENGINE`)
  - `config.yaml`

  If a `config.yaml` file is provided, the OpenFGA server will look for it in `"/etc/openfga"`, `"$HOME/.openfga"`, or `"."` (the current working directory), in that order.
- Support for grpc health checks ([#86](https://github.com/openfga/openfga/pull/86))

  `v0.1.5` introduces support for the [GRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md). The server's health can be checked with the grpc or HTTP health check endpoints (the `/healthz` endpoint is just a proxy to the grpc health check RPC).

  For example,

  ```
  grpcurl -plaintext \
    -d '{"service":"openfga.v1.OpenFGAService"}' \
    localhost:8081 grpc.health.v1.Health/Check
  ```

  or, if the HTTP server is enabled, with the `/healthz` endpoint:

  ```
  curl --request GET -d '{"service":"openfga.v1.OpenFGAService"}' http://localhost:8080/healthz
  ```
- Profiling support (pprof) ([#111](https://github.com/openfga/openfga/pull/111))

  You can now profile the OpenFGA server while it's running using the [pprof](https://github.com/google/pprof/blob/main/doc/README.md) profiler. To enable the pprof profiler set `profiler.enabled=true`. It is served on the `/debug/pprof` endpoint and port `3001` by default.
- Configuration to enable/disable the HTTP server ([#84](https://github.com/openfga/openfga/pull/84))

  You can now enable/disable the HTTP server by setting `http.enabled=true/false`. It is enabled by default.

### Changed
- Env variables have a new mappings.

  Please refer to the [`.config-schema.json`](https://github.com/openfga/openfga/blob/main/.config-schema.json) file for a description of the new configurations or `openfga run -h` for the CLI flags. Env variables are mapped by prefixing `OPENFGA` and converting dot notation into underscores (e.g. `datastore.uri` becomes `OPENFGA_DATASTORE_URI`).

### Fixed
- goroutine leaks in Check resolution. ([#113](https://github.com/openfga/openfga/pull/113))

## [0.1.4] - 2022-06-27
### Added
- OpenFGA Playground support ([#68](https://github.com/openfga/openfga/pull/68))
- CORS policy configuration ([#65](https://github.com/openfga/openfga/pull/65))

## [0.1.2] - 2022-06-20
### Added
- Request validation middleware
- Postgres startup script

## [0.1.1] - 2022-06-16
### Added
- TLS support for both the grpc and HTTP servers
- Configurable logging formats including `text` and `json` formats
- OpenFGA CLI with a preliminary `run` command to run the server

## [0.1.0] - 2022-06-08
### Added
- Initial working implementation of OpenFGA APIs (Check, Expand, Write, Read, Authorization Models, etc..)
- Postgres storage adapter implementation
- Memory storage adapter implementation
- Early support for preshared key or OIDC authentication methods

[Unreleased]: https://github.com/openfga/openfga/compare/v1.10.1...HEAD
[1.10.1]: https://github.com/openfga/openfga/compare/v1.10.0...v1.10.1
[1.10.0]: https://github.com/openfga/openfga/compare/v1.9.5...v1.10.0
[1.9.5]: https://github.com/openfga/openfga/compare/v1.9.4...v1.9.5
[1.9.4]: https://github.com/openfga/openfga/compare/v1.9.3...v1.9.4
[1.9.3]: https://github.com/openfga/openfga/compare/v1.9.2...v1.9.3
[1.9.2]: https://github.com/openfga/openfga/compare/v1.9.1...v1.9.2
[1.9.1]: https://github.com/openfga/openfga/compare/v1.9.0...v1.9.1
[1.9.0]: https://github.com/openfga/openfga/compare/v1.8.16...v1.9.0
[1.8.16]: https://github.com/openfga/openfga/compare/v1.8.15...v1.8.16
[1.8.15]: https://github.com/openfga/openfga/compare/v1.8.14...v1.8.15
[1.8.14]: https://github.com/openfga/openfga/compare/v1.8.13...v1.8.14
[1.8.13]: https://github.com/openfga/openfga/compare/v1.8.12...v1.8.13
[1.8.12]: https://github.com/openfga/openfga/compare/v1.8.11...v1.8.12
[1.8.11]: https://github.com/openfga/openfga/compare/v1.8.10...v1.8.11
[1.8.10]: https://github.com/openfga/openfga/compare/v1.8.9...v1.8.10
[1.8.9]: https://github.com/openfga/openfga/compare/v1.8.8...v1.8.9
[1.8.8]: https://github.com/openfga/openfga/compare/v1.8.7...v1.8.8
[1.8.7]: https://github.com/openfga/openfga/compare/v1.8.6...v1.8.7
[1.8.6]: https://github.com/openfga/openfga/compare/v1.8.5...v1.8.6
[1.8.5]: https://github.com/openfga/openfga/compare/v1.8.4...v1.8.5
[1.8.4]: https://github.com/openfga/openfga/compare/v1.8.3...v1.8.4
[1.8.3]: https://github.com/openfga/openfga/compare/v1.8.2...v1.8.3
[1.8.2]: https://github.com/openfga/openfga/compare/v1.8.1...v1.8.2
[1.8.1]: https://github.com/openfga/openfga/compare/v1.8.0...v1.8.1
[1.8.0]: https://github.com/openfga/openfga/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/openfga/openfga/compare/v1.6.2...v1.7.0
[1.6.2]: https://github.com/openfga/openfga/compare/v1.6.1...v1.6.2
[1.6.1]: https://github.com/openfga/openfga/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/openfga/openfga/compare/v1.5.9...v1.6.0
[1.5.9]: https://github.com/openfga/openfga/compare/v1.5.8...v1.5.9
[1.5.8]: https://github.com/openfga/openfga/compare/v1.5.7...v1.5.8
[1.5.7]: https://github.com/openfga/openfga/compare/v1.5.6...v1.5.7
[1.5.6]: https://github.com/openfga/openfga/compare/v1.5.5...v1.5.6
[1.5.5]: https://github.com/openfga/openfga/compare/v1.5.4...v1.5.5
[1.5.4]: https://github.com/openfga/openfga/compare/v1.5.3...v1.5.4
[1.5.3]: https://github.com/openfga/openfga/compare/v1.5.2...v1.5.3
[1.5.2]: https://github.com/openfga/openfga/compare/v1.5.1...v1.5.2
[1.5.1]: https://github.com/openfga/openfga/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/openfga/openfga/compare/v1.4.3...v1.5.0
[1.4.3]: https://github.com/openfga/openfga/compare/v1.4.2...v1.4.3
[1.4.2]: https://github.com/openfga/openfga/compare/v1.4.1...v1.4.2
[1.4.1]: https://github.com/openfga/openfga/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/openfga/openfga/compare/v1.3.10...v1.4.0
[1.3.10]: https://github.com/openfga/openfga/compare/v1.3.9...v1.3.10
[1.3.9]: https://github.com/openfga/openfga/compare/v1.3.8...v1.3.9
[1.3.8]: https://github.com/openfga/openfga/compare/v1.3.7...v1.3.8
[1.3.7]: https://github.com/openfga/openfga/compare/v1.3.6...v1.3.7
[1.3.6]: https://github.com/openfga/openfga/compare/v1.3.5...v1.3.6
[1.3.5]: https://github.com/openfga/openfga/compare/v1.3.4...v1.3.5
[1.3.4]: https://github.com/openfga/openfga/compare/v1.3.3...v1.3.4
[1.3.3]: https://github.com/openfga/openfga/compare/v1.3.2...v1.3.3
[1.3.2]: https://github.com/openfga/openfga/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/openfga/openfga/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/openfga/openfga/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/openfga/openfga/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/openfga/openfga/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/openfga/openfga/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/openfga/openfga/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/openfga/openfga/compare/v0.4.3...v1.0.0
[0.4.3]: https://github.com/openfga/openfga/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/openfga/openfga/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/openfga/openfga/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/openfga/openfga/compare/v0.3.7...v0.4.0
[0.3.7]: https://github.com/openfga/openfga/compare/v0.3.6...v0.3.7
[0.3.6]: https://github.com/openfga/openfga/compare/v0.3.5...v0.3.6
[0.3.5]: https://github.com/openfga/openfga/compare/v0.3.4...v0.3.5
[0.3.4]: https://github.com/openfga/openfga/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/openfga/openfga/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/openfga/openfga/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/openfga/openfga/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/openfga/openfga/compare/v0.2.5...v0.3.0
[0.2.5]: https://github.com/openfga/openfga/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/openfga/openfga/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/openfga/openfga/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/openfga/openfga/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/openfga/openfga/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/openfga/openfga/compare/v0.1.7...v0.2.0
[0.1.7]: https://github.com/openfga/openfga/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/openfga/openfga/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/openfga/openfga/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/openfga/openfga/compare/v0.1.2...v0.1.4
[0.1.2]: https://github.com/openfga/openfga/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/openfga/openfga/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/openfga/openfga/releases/tag/v0.1.0
