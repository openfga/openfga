# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Try to keep listed changes to a concise bulleted list of simple explanations of changes. Aim for the amount of information needed so that readers can understand where they would look in the codebase to investigate the changes' implementation, or where they would look in the documentation to understand how to make use of the change in practice - better yet, link directly to the docs and provide detailed information there. Only elaborate if doing so is required to avoid breaking changes or experimental features from ruining someone's day.

## [Unreleased]

## [1.3.5] - 2023-10-27

[Full changelog](https://github.com/openfga/openfga/compare/v1.3.4...v1.3.5)

### Added

* Export metrics from MySQL and Postgres ([#1023](https://github.com/openfga/openfga/pull/1023))

### Fixed

* Return all results when `OPENFGA_LIST_OBJECTS_MAX_RESULTS=0` ([#1067](https://github.com/openfga/openfga/pull/1067))
* Promptly return if max results are met before deadline in ListObjects ([#1064](https://github.com/openfga/openfga/pull/1064))
* Fix sort order on ReadChanges ([#1079](https://github.com/openfga/openfga/pull/1079))

### Changed

* Write Authorization Models in a single database row ([#1030](https://github.com/openfga/openfga/pull/1030))

  :warning: In order to avoid downtime, we recommend upgrading to at least v1.3.3 _before_ upgrading to v1.3.5.

  This is the second of a series of releases that will progressively introduce changes via code and database migrations that will allow authorization models to be stored in a single database row.

  See [here for more details](https://github.com/openfga/openfga/issues/1025).

## [1.3.4] - 2023-10-17

[Full changelog](https://github.com/openfga/openfga/compare/v1.3.3...v1.3.4)

### Fixed

* Incorrect string in model validation error message ([#1057](https://github.com/openfga/openfga/pull/1057))
* Incorrect results can be returned by Check API when passing in contextual tuples and the `check-query-cache` experimental flag is turned on ([#1059](https://github.com/openfga/openfga/pull/1059))

### Changed

* Bumped up to Go 1.21.3 ([#1060](https://github.com/openfga/openfga/pull/1060))

### Security

* Patches [CVE-2023-45810](https://github.com/openfga/openfga/security/advisories/GHSA-hr4f-6jh8-f2vq). See the CVE for more details

## [1.3.3] - 2023-10-04

[Full changelog](https://github.com/openfga/openfga/compare/v1.3.2...v1.3.3)

### Added

* Configurable size limit for Authorization Models ([#1032](https://github.com/openfga/openfga/pull/1032))

  We've introduced a new size limit for authorization models, provided a consistent behavior across datastores, which defaults to `256KB`. This can be configured by using the `--max-authorization-model-size-in-bytes` flag.

### Fixed

* Reduce use of GOB in encoded cache key ([#1029](https://github.com/openfga/openfga/pull/1029))

### Changed

* Move standalone server config defaults ([#1036](https://github.com/openfga/openfga/pull/1036))

* Persist Authorization Models serialized protobuf in the database ([#1028](https://github.com/openfga/openfga/pull/1028))

  In the next series of releases will progressively introduce changes via code and database migrations that will allow authorization models to be stored in a single database row.

  See [here for more details](https://github.com/openfga/openfga/issues/1025).


## [1.3.2] - 2023-08-25
### Added
* Support TLS for OTLP trace endpoint ([#885](https://github.com/openfga/openfga/pull/885)) - thanks @matoous
* Configurable limits to database reads per ListObjects query ([#967](https://github.com/openfga/openfga/pull/967))
* Datastore query count labels to traces and query latency histogram in ListObjects ([#959](https://github.com/openfga/openfga/pull/959))
* GitHub workflow to check Markdown links ([#1016](https://github.com/openfga/openfga/pull/1016)) - thanks @sanketrai1

### Fixed
* Change response code to internal error for concurrency conflicts ([#1011](https://github.com/openfga/openfga/pull/1011))

### Changed
* Use slices and maps packages from go1.21 ([#969](https://github.com/openfga/openfga/pull/969)) - thanks @tranngoclam
* Moved request validations to RPC handlers so library integrations benefit ([#975](https://github.com/openfga/openfga/pull/975), [#998](https://github.com/openfga/openfga/pull/998))
* Refactored internal usages of ConnectedObjects to ReverseExpand ([#968](https://github.com/openfga/openfga/pull/968))
* Expose validation middleware ([#1005](https://github.com/openfga/openfga/pull/1005))
* Upgrade grpc validator middleware to the latest v2 package ([#1019](https://github.com/openfga/openfga/pull/1019)) - thanks @tranngoclam

### Security
* Patches [CVE-2023-43645](https://github.com/openfga/openfga/security/advisories/GHSA-2hm9-h873-pgqh) - see the CVE for more details

  **[BREAKING]** If your model contained cycles or a relation definition that has the relation itself in its evaluation path, then Checks and queries that require evaluation will no longer be evaluated on v1.3.2+ and will return errors instead. You will need to update your models to remove the cycles.

## [1.3.1] - 2023-08-23

### Added
* Count datastore queries involved in Check resolution metadata ([#880](https://github.com/openfga/openfga/pull/880))

  OpenFGA request logs and traces will now include a field `datastore_query_count` that shows how many queries were involved in a single Check resolution.

* Histogram metric to report the `datastore_query_count` per Check ([#924](https://github.com/openfga/openfga/pull/932))

  This new metric can be used to report percentiles of the number of database queries required to resolve Check requests.

* Check request duration histogram labeled by method and datastore query count ([#950](https://github.com/openfga/openfga/pull/950))

  The `request_duration_by_query_count_ms` metric reports the total request duration (in ms) labelled by the RPC method and ranges of observations for the `datastore_query_count`. This metrics allows operators of an OpenFGA server to report request duration percentiles for Check requests based on the number of database queries that were required to resolve the query.

* Optimize Check to avoid database lookups in some scenarios ([#932](https://github.com/openfga/openfga/pull/932))

* CachedCheckResolver for caching Check subproblems ([#891](https://github.com/openfga/openfga/pull/891))

  This experimental feature adds new caching capabilities to the OpenFGA server. It is an "opt-in" feature and thus must be enabled. To enable this feature you must specify the experimental flag `check-query-cache` and set the  `--check-query-cache-enabled=true` flag.

  ```shell
  openfga run --experimentals check-query-cache --check-query-cache-enabled=true
  ```

* Server request logs now include the `user-agent` ([#943](https://github.com/openfga/openfga/pull/943))

### Changed
* Default Check and ListObjects concurrency read limits ([#916](https://github.com/openfga/openfga/pull/916))

  In our last release [v1.3.0](https://github.com/openfga/openfga/releases/tag/v1.3.0) we modified the default behavior of Check and ListObjects such that it limits/restricts the degree of concurrency that is allowed for a single request. This change was unintended. This release reverts the default behavior back to unbounded concurrency limits (the prior default). The change mostly affects those using OpenFGA as a library.

* Bumped up to Go 1.21 ([#952](https://github.com/openfga/openfga/pull/952))

### Security
* Patches [CVE-2023-40579](https://github.com/openfga/openfga/security/advisories/GHSA-jcf2-mxr2-gmqp) - see the CVE for more details

## [1.3.0] - 2023-08-01

[Full changelog](https://github.com/openfga/openfga/compare/v1.2.0...v1.3.0)

### Added
* Bounded concurrency limiter for Check and ListObjects queries ([#860](https://github.com/openfga/openfga/pull/860), [#887](https://github.com/openfga/openfga/pull/887))
  New server configurations can be provided to limit/bound the amount of concurrency that is allowed during query evaluation. These settings can help reduce the impact/burden that a single query (e.g. Check, ListObjects, etc..) can have on the underlying database and OpenFGA server.

  * `--maxConcurrentReadsForListObjects` - The maximum allowed number of concurrent reads in a single ListObjects query.

  * `--maxConcurrentReadsForCheck` - The maximum allowed number of concurrent reads in a single Check query.

  * `--resolveNodeBreadthLimit` - Defines how many nodes on a given level can be evaluated concurrently in a Check resolution tree.

* Jaeger persistent storage for traces in `docker-compose.yaml` ([#888](https://github.com/openfga/openfga/pull/888)) - thanks @Azanul

### Fixed
* Disable default debug level-logging in `retryablehttp` client ([#882](https://github.com/openfga/openfga/pull/882)) - thanks @KlausVii

### Changed
* [BREAKING] Imports for OpenFGA protobuf API dependencies ([#898](https://github.com/openfga/openfga/pull/898))
  * **Problem** - Previously we depended on [Buf remote generated packages](https://buf.build/docs/bsr/remote-packages/overview), but they recently deprecated protobuf imports served from the `go.buf.build` domain (see [Migrate from remote generation alpha](https://buf.build/docs/migration-guides/migrate-remote-generation-alpha)). OpenFGA builds are currently broken as a result of this.
  * **Change** - We switched our protobuf API dependency from `go.buf.build/openfga/go/openfga/api/openfga/v1` to `github.com/openfga/api/proto/openfga/v1`. So we no longer use Buf remote generated packages in favor of packages we managed in the [`openfga/api`](https://github.com/openfga/api) repository. This fixes existing build issues.
  * **Impact** - Developers using the OpenFGA as a library or the gRPC API must change their protobuf dependency from `go.buf.build/openfga/go/openfga/api/openfga/v1` to `github.com/openfga/api/proto/openfga/v1`. A global find/replace and package dependency update should fix it. Here's a diff demonstrating the changes for a Go app, for example:

    ```go
    import (
      ...
    - openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
    + openfgav1 "github.com/openfga/api/proto/openfga/v1"
    )
    ```

* Refactor the `Server` constructor to use the options builder pattern ([#833](https://github.com/openfga/openfga/pull/833))

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
## [1.2.0] - 2023-06-30

[Full changelog](https://github.com/openfga/openfga/compare/v1.1.1...v1.2.0)

### Added
* Optimizations for [ListObjects](https://openfga.dev/api/service#/Relationship%20Queries/ListObjects) and [StreamedListObjects](https://openfga.dev/api/service#/Relationship%20Queries/StreamedListObjects) for models involving intersection (`and`) and exclusion (`but not`) ([#797](https://github.com/openfga/openfga/pull/797))

### Changed
* Cache model validation results on first model load ([#831](https://github.com/openfga/openfga/pull/831))
* Cache inflight requests when looking up any authorization model ([#831](https://github.com/openfga/openfga/pull/831))
* Update postgres max connections in docker compose file ([#829](https://github.com/openfga/openfga/pull/829))

## [1.1.1] - 2023-06-26

[Full changelog](https://github.com/openfga/openfga/compare/v1.1.0...v1.1.1)

### Added
* Official Homebrew installation instructions ([#781](https://github.com/openfga/openfga/pull/781)) - thanks @chenrui333
* The `--verbose` flag has been added to the `openfga migrate` command ([#776](https://github.com/openfga/openfga/pull/776))
* The `openfga validate-models` CLI command has been introduced to validate all models across all stores ([#817](https://github.com/openfga/openfga/pull/817))

### Changed
* Updated the version of the `grpc-health-probe` binary included in OpenFGA builds ([#784](https://github.com/openfga/openfga/pull/784))
* Cache inflight requests when looking up the latest authorization model ([#820](https://github.com/openfga/openfga/pull/820))

### Fixed
* Validation of models with non-zero entrypoints ([#802](https://github.com/openfga/openfga/pull/802))
* Remove unintended newlines in model validation error messages ([#816](https://github.com/openfga/openfga/pull/816)) - thanks @Galzzly

### Security
* Patches [CVE-2023-35933](https://github.com/openfga/openfga/security/advisories/GHSA-hr9r-8phq-5x8j) - additional model validations are now applied to models that can lead to the vulnerability. See the CVE report for more details, and don't hesitate to reach out if you have questions.

## [1.1.0] - 2023-05-15

[Full changelog](https://github.com/openfga/openfga/compare/v1.0.1...v1.1.0)

## Added
* Streaming ListObjects has no limit in number of results returned ([#733](https://github.com/openfga/openfga/pull/733))
* Add Homebrew release stage to goreleaser's release process ([#716](https://github.com/openfga/openfga/pull/716))

## Fixed
* Avoid DB connection churning in unoptimized ListObjects ([#711](https://github.com/openfga/openfga/pull/711))
* Ensure ListObjects respects configurable ListObjectsDeadline ([#704](https://github.com/openfga/openfga/pull/704))
* In Write, throw 400 instead of 500 error if auth model ID not found ([#725](https://github.com/openfga/openfga/pull/725))
* Performance improvements when loading the authorization model ([#726](https://github.com/openfga/openfga/pull/726))
* Ensure Check evaluates deterministically on the eval boundary case ([#732](https://github.com/openfga/openfga/pull/732))

## Changed
* [BREAKING] The flags to turn on writing and evaluation of `v1.0` models have been dropped ([#763](https://github.com/openfga/openfga/pull/763))

## [1.0.1] - 2023-04-18

[Full changelog](https://github.com/openfga/openfga/compare/v1.0.0...v1.0.1)

## Fixed
* Correct permission and location for gRPC health probe in Docker image (#697)

## [1.0.0] - 2023-04-14

[Full changelog](https://github.com/openfga/openfga/compare/v0.4.3...v1.0.0)

## Ready for Production with Postgres
OpenFGA with Postgres is now considered stable and ready for production usage.

## Fixed
* MySQL migration script errors during downgrade (#664)

## [0.4.3] - 2023-04-12

[Full changelog](https://github.com/openfga/openfga/compare/v0.4.2...v0.4.3)

## Added
* Release artifacts are now signed and include a Software Bill of Materials (SBOM) ([#683](https://github.com/openfga/openfga/pull/683))

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

* `openfga migrate` now accepts reading configuration from a config file and environment variables like the `openfga run` command ([#655](https://github.com/openfga/openfga/pull/655)) - thanks @suttod!

* The `--trace-service-name` command-line flag has been added to allow for customizing the service name in traces ([#652](https://github.com/openfga/openfga/pull/652)) - thanks @jmiettinen

## Fixed
* Postgres and MySQL implementations have been fixed to avoid ordering relationship tuple queries by `ulid` when it is not needed. This can improve read query performance on larger OpenFGA stores ([#677](https://github.com/openfga/openfga/pull/677))
* Synchronize concurrent access to in-memory storage iterators ([#587](https://github.com/openfga/openfga/pull/587))
* Improve error logging in the `openfga migrate` command ([#663](https://github.com/openfga/openfga/pull/663))
* Fix middleware ordering so that `requestid` middleware is registered earlier ([#662](https://github.com/openfga/openfga/pull/662))

## Changed
* Bumped up to Go version 1.20 ([#664](https://github.com/openfga/openfga/pull/664))
* Default model schema versions to 1.1 ([#669](https://github.com/openfga/openfga/pull/669))

  In preparation for sunsetting support for models with schema version 1.0, the [WriteAuthorizationModel API](https://openfga.dev/api/service#/Authorization%20Models/WriteAuthorizationModel) will now interpret any model provided to it as a 1.1 model if the `schema_version` field is omitted in the request. This shouldn't affect default behavior since 1.0 model support is enabled by default.

## [0.4.2] - 2023-03-17

[Full changelog](https://github.com/openfga/openfga/compare/v0.4.1...v0.4.2)

### Fixed
* Correct migration path for mysql in `openfga migrate` ([#644](https://github.com/openfga/openfga/pull/664))

## [0.4.1] - 2023-03-16

[Full changelog](https://github.com/openfga/openfga/compare/v0.4.0...v0.4.1)


The `v0.4.1` release includes everything in `v0.4.0` which includes breaking changes, please read the [`v0.4.0` changelog entry](#040---2023-03-15) for more details.

### Fixed

* Fix ListObjects not returning objects a user has access to in some cases (openfga/openfga#637)

## [0.4.0] - 2023-03-15

[Full changelog](https://github.com/openfga/openfga/compare/v0.3.7...v0.4.0)

> Note: the 0.4.0 release was held due to issues discovered after the release was cut.

### Removed

* [BREAKING] Disable schema 1.0 support, except if appropriate flags are set (openfga/openfga#613)
  * As of this release, OpenFGA no longer allows writing or evaluating schema `v1.0` models by default. If you need support for it for now, you can use the:
    * `OPENFGA_ALLOW_WRITING_1_0_MODELS`: set to `true` to allow `WriteAuthorizationModel` to accept schema `v1.0` models.
    * `OPENFGA_ALLOW_EVALUATING_1_0_MODELS`: set to `true` to allow `Check`, `Expand`, `ListObjects`, `Write` and `WriteAssertions` that target schema `v1.0` models.
    * `ReadAuthorizationModel`, `ReadAuthorizationModels` and `ReadAssertions` are unaffected and will continue to work regardless of the target model schema version.
  * Note that these flags will be removed and support fully dropped in a future release. Read the [Schema v1.0 Deprecation Timeline](https://openfga.dev/docs/modeling/migrating/migrating-schema-1-1#deprecation-timeline) for more details.

### Added
* Add OpenFGA version command to the CLI ([#625](https://github.com/openfga/openfga/pull/625))
* Add `timeout` flag to `migrate` command ([#634](https://github.com/openfga/openfga/pull/634))

### Fixed

* Improve the speed of Check for 1.1 models by using type restrictions (([#545](https://github.com/openfga/openfga/pull/545), ([#596](https://github.com/openfga/openfga/pull/596))
* Various important fixes to the experimental ListObjects endpoint
  * Improve readUsersets query by dropping unnecessary sorting ([#631](https://github.com/openfga/openfga/pull/631),([#633](https://github.com/openfga/openfga/pull/633))
  * Fix null pointer exception if computed userset does not exist ([#572](https://github.com/openfga/openfga/pull/572))
  * Fix race condition in memory store ([#585](https://github.com/openfga/openfga/pull/585))
  * Ensure no objects returned that would not have been allowed in Checks ([#577](https://github.com/openfga/openfga/pull/577))
  * Reverse expansion with indirect computed userset relationship ([#611](https://github.com/openfga/openfga/pull/611))
  * Improved tests ([#582](https://github.com/openfga/openfga/pull/582), [#599](https://github.com/openfga/openfga/pull/599), [#601](https://github.com/openfga/openfga/pull/601), [#620](https://github.com/openfga/openfga/pull/620))
* Tuning of OTEL parameters ([#570](https://github.com/openfga/openfga/pull/570))
* Fix tracing in Check API ([#627](https://github.com/openfga/openfga/pull/627))
* Use chainguard images in Dockerfile ([#628](https://github.com/openfga/openfga/pull/628))


## [0.3.7] - 2023-02-21

[Full changelog](https://github.com/openfga/openfga/compare/v0.3.6...v0.3.7)

### Fixed
* Contextual tuple propagation in the unoptimized ListObjects implementation ([#565](https://github.com/openfga/openfga/pull/565))

## [0.3.6] - 2023-02-16

[Full changelog](https://github.com/openfga/openfga/compare/v0.3.5...v0.3.6)

Re-release of `v0.3.5` because the go module proxy cached a prior commit of the `v0.3.5` tag.

## [0.3.5] - 2023-02-14

[Full changelog](https://github.com/openfga/openfga/compare/v0.3.4...v0.3.5)

### Added
* [`grpc-health-probe`](https://github.com/grpc-ecosystem/grpc-health-probe) for Health Checks ([#520](https://github.com/openfga/openfga/pull/520))

  OpenFGA containers now include an embedded `grpc_health_probe` binary that can be used to probe the Health Check endpoints of OpenFGA servers. Take a look at the [docker-compose.yaml](https://github.com/openfga/openfga/blob/main/docker-compose.yaml) file for an example.

* Improvements to telemetry: logging, tracing, and metrics ([#468](https://github.com/openfga/openfga/pull/468), [#514](https://github.com/openfga/openfga/pull/514), [#517](https://github.com/openfga/openfga/pull/517), [#522](https://github.com/openfga/openfga/pull/522))

  * We have added Prometheus as the standard metrics provided for OpenFGA and provide a way to launch Grafana to view the metrics locally. See [docker-compose.yaml](https://github.com/openfga/openfga/blob/main/docker-compose.yaml) for more information.

  * We've improved the attributes of various trace spans and made sure that trace span names align with the functions they decorate.

  * Our logging has been enhanced with more logged fields including request level logging which includes a `request_id` and `store_id` field in the log message.

  These features will allow operators of OpenFGA to improve their monitoring and observability processes.

* Nightly releases ([#508](https://github.com/openfga/openfga/pull/508)) - thanks @Siddhant-K-code!

  You should now be able to run nightly releases of OpenFGA using `docker pull openfga/openfga:nightly`

### Fixed
* Undefined computed relations on tuplesets now behave properly ([#532](https://github.com/openfga/openfga/pull/532))

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

* Eliminate duplicate objects in ListObjects response ([#528](https://github.com/openfga/openfga/pull/528))

## [0.3.4] - 2023-02-02

[Full changelog](https://github.com/openfga/openfga/compare/v0.3.3...v0.3.4)

### Fixed

* Fixed the environment variable mapping ([#498](https://github.com/openfga/openfga/pull/498)). For the full list of environment variables see [.config-schema.json](https://github.com/openfga/openfga/blob/main/.config-schema.json).
* Fix for stack overflow error in ListObjects ([#506](https://github.com/openfga/openfga/pull/506)). Thank you for reporting the issue @wonderbeyond!

### Added

* Added OpenTelemetry tracing ([#499](https://github.com/openfga/openfga/pull/499))

### Removed

* The ReadTuples endpoint has been removed ([#495](https://github.com/openfga/openfga/pull/495)). Please use [Read](https://openfga.dev/api/service#/Relationship%20Tuples/Read) with no tuple key instead (e.g. `POST /stores/<store_id>/read` with `{}` as the body).

## [0.3.3] - 2023-01-31

[Full changelog](https://github.com/openfga/openfga/compare/v0.3.2...v0.3.3)

### Added

* Environment variable names have been updated ([#472](https://github.com/openfga/openfga/pull/472)).

  For example, `OPENFGA_MAX_TUPLES_PER_WRITE` instead of `OPENFGA_MAXTUPLESPERWRITE`.

  For the full list please see [.config-schema.json](https://github.com/openfga/openfga/blob/main/.config-schema.json).

  The old form still works but is considered deprecated and should not be used anymore.

* Optimized ListObjects is now on by default ([#489](https://github.com/openfga/openfga/pull/489)) (`--experimentals="list-objects-optimized"` is no longer needed)

* Avoid connection churn in our datastore implementations ([#474](https://github.com/openfga/openfga/pull/474))

* The default values for `OPENFGA_DATASTORE_MAX_OPEN_CONNS` and `OPENFGA_DATASTORE_MAX_IDLE_CONNS` have been set to 30 and 10 respectively ([#492](https://github.com/openfga/openfga/pull/492))

### Fixed

* ListObjects should no longer return duplicates ([#475](https://github.com/openfga/openfga/pull/475))

## [0.3.2] - 2023-01-18

[Full changelog](https://github.com/openfga/openfga/compare/v0.3.1...v0.3.2)


### Added
* OpenTelemetry metrics integration with an `otlp` exporter ([#360](https://github.com/openfga/openfga/pull/360)) - thanks @AlexandreBrg!

  To export OpenTelemetry metrics from an OpenFGA instance you can now provide the `otel-metrics` experimental flag along with the `--otel-telemetry-endpoint` and `--otel-telemetry-protocol` flags. For example,

  ```
  ./openfga run --experimentals=otel-metrics --otel-telemetry-endpoint=127.0.0.1:4317 --otel-telemetry-protocol=http
  ```

  For more information see the official documentation on [Experimental Features](https://openfga.dev/docs/getting-started/setup-openfga/docker#experimental-features) and [Telemetry](https://openfga.dev/docs/getting-started/setup-openfga/docker#telemetry).

* Type-bound public access support in the optimized ListObjects implementation (when the `list-objects-optimized` experimental feature is enabled) ([#444](https://github.com/openfga/openfga/pull/444))

### Fixed
* Tuple validations for models with schema version 1.1 ([#446](https://github.com/openfga/openfga/pull/446), [#457](https://github.com/openfga/openfga/pull/457))
* Evaluate rewrites on nested usersets in the optimized ListObjects implementation ([#432](https://github.com/openfga/openfga/pull/432))

## [0.3.1] - 2022-12-19

[Full changelog](https://github.com/openfga/openfga/compare/v0.3.0...v0.3.1)

### Added
* Datastore configuration flags to control connection pool settings
  `--datastore-max-open-conns`
  `--datastore-max-idle-conns`
  `--datastore-conn-max-idle-time`
  `--datastore-conn-max-lifetime`
  These flags can be used to fine-tune database connections for your specific deployment of OpenFGA.

* Log level configuration flags
  `--log-level` (can be one of ['none', 'debug', 'info', 'warn', 'error', 'panic', 'fatal'])

* Support for Experimental Feature flags
  A new flag `--experimentals` has been added to enable certain experimental features in OpenFGA. For more information see [Experimental Features](https://openfga.dev/docs/getting-started/setup-openfga/docker#experimental-features).

### Security
* Patches [CVE-2022-23542](https://github.com/openfga/openfga/security/advisories/GHSA-m3q4-7qmj-657m) - relationship reads now respect type restrictions from prior models ([#422](https://github.com/openfga/openfga/pull/422)).

## [0.3.0] - 2022-12-12

[Full changelog](https://github.com/openfga/openfga/compare/v0.2.5...v0.3.0)

This release comes with a few big changes:

### Support for [v1.1 JSON Schema](https://github.com/openfga/rfcs/blob/feat/add-type-restrictions-to-json-syntax/20220831-add-type-restrictions-to-json-syntax.md)

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

### ListObjects changes

The response has changed to include the object type, for example:
```json
{ "object_ids": [ "a", "b", "c" ] }
```
to
```json
{ "objects": [ "document:a", "document:b", "document:c" ] }
```

We have also improved validation and fixed support for Contextual Tuples that were causing inaccurate responses to be
returned.

### ReadTuples deprecation

:warning:This endpoint is now marked as deprecated, and support for it will be dropped shortly. Please use Read with
no tuple key instead.


## [0.2.5] - 2022-11-07
### Security
* Patches [CVE-2022-39352](https://github.com/openfga/openfga/security/advisories/GHSA-3gfj-fxx4-f22w)

### Added
* Multi-platform container build manifests to releases ([#323](https://github.com/openfga/openfga/pull/323))

### Fixed
* Read RPC returns correct error when authorization model id is not found ([#312](https://github.com/openfga/openfga/pull/312))
* Throw error if `http.upstreamTimeout` config is less than `listObjectsDeadline` ([#315](https://github.com/openfga/openfga/pull/315))

## [0.2.4] - 2022-10-24
### Security
* Patches [CVE-2022-39340](https://github.com/openfga/openfga/security/advisories/GHSA-95x7-mh78-7w2r), [CVE-2022-39341](https://github.com/openfga/openfga/security/advisories/GHSA-vj4m-83m8-xpw5), and [CVE-2022-39342](https://github.com/openfga/openfga/security/advisories/GHSA-f4mm-2r69-mg5f)

### Fixed
* TLS certificate config path mappings ([#285](https://github.com/openfga/openfga/pull/285))
* Error message when a `user` field is invalid ([#278](https://github.com/openfga/openfga/pull/278))
* host:port mapping with unspecified host ([#275](https://github.com/openfga/openfga/pull/275))
* Wait for connection to postgres before starting ([#270](https://github.com/openfga/openfga/pull/270))


### Added
* Update Go to 1.19

## [0.2.3] - 2022-10-05
### Added
* Support for MySQL storage backend ([#210](https://github.com/openfga/openfga/pull/210)). Thank you @MidasLamb!
* Allow specification of type restrictions in authorization models ([#223](https://github.com/openfga/openfga/pull/223)). Note: Type restriction is not enforced yet, this just allows storing them.
* Tuple validation against type restrictions in Write API ([#232](https://github.com/openfga/openfga/pull/232))
* Upgraded the Postgres storage backend to use pgx v5 ([#225](https://github.com/openfga/openfga/pull/225))

### Fixed
* Close database connections after migration ([#252](https://github.com/openfga/openfga/pull/252))
* Race condition in streaming ListObjects ([#255](https://github.com/openfga/openfga/pull/255), [#256](https://github.com/openfga/openfga/pull/256))


## [0.2.2] - 2022-09-15
### Fixed
* Reject direct writes if only indirect relationship allowed ([#114](https://github.com/openfga/openfga/pull/114)). Thanks @dblclik!
* Log internal errors at the grpc layer ([#222](https://github.com/openfga/openfga/pull/222))
* Authorization model validation ([#224](https://github.com/openfga/openfga/pull/224))
* Bug in `migrate` command ([#236](https://github.com/openfga/openfga/pull/236))
* Skip malformed tuples involving tuple to userset definitions ([#234](https://github.com/openfga/openfga/pull/234))

## [0.2.1] - 2022-08-30
### Added
* Support Check API calls on userset types of users ([#146](https://github.com/openfga/openfga/pull/146))
* Add backoff when connecting to Postgres ([#188](https://github.com/openfga/openfga/pull/188))

### Fixed
* Improve logging of internal server errors ([#193](https://github.com/openfga/openfga/pull/193))
* Use Postgres in the sample Docker Compose file ([#195](https://github.com/openfga/openfga/pull/195))
* Emit authorization errors ([#144](https://github.com/openfga/openfga/pull/144))
* Telemetry in Check and ListObjects APIs ([#177](https://github.com/openfga/openfga/pull/177))
* ListObjects API: respect the value of ListObjectsMaxResults ([#181](https://github.com/openfga/openfga/pull/181))


## [0.2.0] - 2022-08-12
### Added
* [ListObjects API](https://openfga.dev/api/service#/Relationship%20Queries/ListObjects)

  The ListObjects API provides a way to list all of the objects (of a particular type) that a user has a relationship with. It provides a solution to the [Search with Permissions (Option 3)](https://openfga.dev/docs/interacting/search-with-permissions#option-3-build-a-list-of-ids-then-search) use case for access-aware filtering on smaller object collections. It implements the [ListObjects RFC](https://github.com/openfga/rfcs/blob/main/20220714-listObjects-api.md).

  This addition brings with it two new server configuration options `--listObjects-deadline` and `--listObjects-max-results`. These configurations help protect the server from excessively long lived and large responses.

  > ⚠️ If `--listObjects-deadline` or `--listObjects-max-results` are provided, the endpoint may only return a subset of the data. If you provide the deadline but returning all of the results would take longer than the deadline, then you may not get all of the results. If you limit the max results to 1, then you'll get at most 1 result.

* Support for presharedkey authentication in the Playground ([#141](https://github.com/openfga/openfga/pull/141))

  The embedded Playground now works if you run OpenFGA using one or more preshared keys for authentication. OIDC authentication remains unsupported for the Playground at this time.


## [0.1.7] - 2022-07-29
### Added
* `migrate` CLI command ([#56](https://github.com/openfga/openfga/pull/56))

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
* Issue with embedded Playground assets found in the `v0.1.5` released docker image ([#129](https://github.com/openfga/openfga/pull/129))

## [0.1.5] - 2022-07-27
### Added
* Support for defining server configuration in `config.yaml`, CLI flags, or env variables ([#63](https://github.com/openfga/openfga/pull/63), [#92](https://github.com/openfga/openfga/pull/92), [#100](https://github.com/openfga/openfga/pull/100))

  `v0.1.5` introduces multiple ways to support a variety of server configuration strategies. You can configure the server with CLI flags, env variables, or a `config.yaml` file.

  Server config will be loaded in the following order of precedence:

    * CLI flags (e.g. `--datastore-engine`)
    * env variables (e.g. `OPENFGA_DATASTORE_ENGINE`)
    * `config.yaml`

  If a `config.yaml` file is provided, the OpenFGA server will look for it in `"/etc/openfga"`, `"$HOME/.openfga"`, or `"."` (the current working directory), in that order.

* Support for grpc health checks ([#86](https://github.com/openfga/openfga/pull/86))

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

* Profiling support (pprof) ([#111](https://github.com/openfga/openfga/pull/111))

  You can now profile the OpenFGA server while it's running using the [pprof](https://github.com/google/pprof/blob/main/doc/README.md) profiler. To enable the pprof profiler set `profiler.enabled=true`. It is served on the `/debug/pprof` endpoint and port `3001` by default.

* Configuration to enable/disable the HTTP server ([#84](https://github.com/openfga/openfga/pull/84))

  You can now enable/disable the HTTP server by setting `http.enabled=true/false`. It is enabled by default.

### Changed
* Env variables have a new mappings.

  Please refer to the [`.config-schema.json`](https://github.com/openfga/openfga/blob/main/.config-schema.json) file for a description of the new configurations or `openfga run -h` for the CLI flags. Env variables are mapped by prefixing `OPENFGA` and converting dot notation into underscores (e.g. `datastore.uri` becomes `OPENFGA_DATASTORE_URI`).

### Fixed
* goroutine leaks in Check resolution. ([#113](https://github.com/openfga/openfga/pull/113))

## [0.1.4] - 2022-06-27
### Added
* OpenFGA Playground support ([#68](https://github.com/openfga/openfga/pull/68))
* CORS policy configuration ([#65](https://github.com/openfga/openfga/pull/65))

## [0.1.2] - 2022-06-20
### Added
* Request validation middleware
* Postgres startup script

## [0.1.1] - 2022-06-16
### Added
* TLS support for both the grpc and HTTP servers
* Configurable logging formats including `text` and `json` formats
* OpenFGA CLI with a preliminary `run` command to run the server

## [0.1.0] - 2022-06-08
### Added
* Initial working implementation of OpenFGA APIs (Check, Expand, Write, Read, Authorization Models, etc..)
* Postgres storage adapter implementation
* Memory storage adapter implementation
* Early support for preshared key or OIDC authentication methods

[Unreleased]: https://github.com/openfga/openfga/compare/v1.3.5...HEAD
[1.3.5]: https://github.com/openfga/openfga/releases/tag/v1.3.5
[1.3.4]: https://github.com/openfga/openfga/releases/tag/v1.3.4
[1.3.3]: https://github.com/openfga/openfga/releases/tag/v1.3.3
[1.3.2]: https://github.com/openfga/openfga/releases/tag/v1.3.2
[1.3.1]: https://github.com/openfga/openfga/releases/tag/v1.3.1
[1.3.0]: https://github.com/openfga/openfga/releases/tag/v1.3.0
[1.2.0]: https://github.com/openfga/openfga/releases/tag/v1.2.0
[1.1.1]: https://github.com/openfga/openfga/releases/tag/v1.1.1
[1.1.0]: https://github.com/openfga/openfga/releases/tag/v1.1.0
[1.0.1]: https://github.com/openfga/openfga/releases/tag/v1.0.1
[1.0.0]: https://github.com/openfga/openfga/releases/tag/v1.0.0
[0.4.3]: https://github.com/openfga/openfga/releases/tag/v0.4.3
[0.4.2]: https://github.com/openfga/openfga/releases/tag/v0.4.2
[0.4.1]: https://github.com/openfga/openfga/releases/tag/v0.4.1
[0.4.0]: https://github.com/openfga/openfga/releases/tag/v0.4.0
[0.3.7]: https://github.com/openfga/openfga/releases/tag/v0.3.7
[0.3.6]: https://github.com/openfga/openfga/releases/tag/v0.3.6
[0.3.5]: https://github.com/openfga/openfga/releases/tag/v0.3.5
[0.3.4]: https://github.com/openfga/openfga/releases/tag/v0.3.4
[0.3.3]: https://github.com/openfga/openfga/releases/tag/v0.3.3
[0.3.2]: https://github.com/openfga/openfga/releases/tag/v0.3.2
[0.3.1]: https://github.com/openfga/openfga/releases/tag/v0.3.1
[0.3.0]: https://github.com/openfga/openfga/releases/tag/v0.3.0
[0.2.5]: https://github.com/openfga/openfga/releases/tag/v0.2.5
[0.2.4]: https://github.com/openfga/openfga/releases/tag/v0.2.4
[0.2.3]: https://github.com/openfga/openfga/releases/tag/v0.2.3
[0.2.2]: https://github.com/openfga/openfga/releases/tag/v0.2.2
[0.2.1]: https://github.com/openfga/openfga/releases/tag/v0.2.1
[0.2.0]: https://github.com/openfga/openfga/releases/tag/v0.2.0
[0.1.7]: https://github.com/openfga/openfga/releases/tag/v0.1.7
[0.1.6]: https://github.com/openfga/openfga/releases/tag/v0.1.6
[0.1.5]: https://github.com/openfga/openfga/releases/tag/v0.1.5
[0.1.4]: https://github.com/openfga/openfga/releases/tag/v0.1.4
[0.1.2]: https://github.com/openfga/openfga/releases/tag/v0.1.2
[0.1.1]: https://github.com/openfga/openfga/releases/tag/v0.1.1
[0.1.0]: https://github.com/openfga/openfga/releases/tag/v0.1.0
