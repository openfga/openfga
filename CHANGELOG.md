# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Try to keep listed changes to a concise bulleted list of simple explanations of changes. Aim for the amount of information needed so that readers can understand where they would look in the codebase to investigate the changes' implementation, or where they would look in the documentation to understand how to make use of the change in practice - better yet, link directly to the docs and provide detailed information there. Only elaborate if doing so is required to avoid breaking changes or experimental features from ruining someone's day.

## [Unreleased]

## [0.2.2] - 2022-09-15
### Fixed
* Reject direct writes if only indirect relationship allowed (#114). Thanks @dblclik!
* Log internal errors at the grpc layer (#222)
* Authorization model validation (#224)
* Bug in `migrate` command (#236)
* Skip malformed tuples involving tuple to userset definitions (#234)

## [0.2.1] - 2022-08-30
### Added
* Support Check API calls on userset types of users (#146)
* Add backoff when connecting to Postgres (#188)

### Fixed
* Improve logging of internal server errors (#193)
* Use Postgres in the sample Docker Compose file (#195)
* Emit authorization errors (#144)
* Telemetry in Check and ListObjects APIs (#177)
* ListObjects API: respect the value of ListObjectsMaxResults (#181)


## [0.2.0] - 2022-08-12
### Added
* [ListObjects API](https://openfga.dev/api/service#/Relationship%20Queries/ListObjects)

  The ListObjects API provides a way to list all of the objects (of a particular type) that a user has a relationship with. It provides a solution to the [Search with Permissions (Option 3)](https://openfga.dev/docs/interacting/search-with-permissions#option-3-build-a-list-of-ids-then-search) use case for access-aware filtering on smaller object collections. It implements the [ListObjects RFC](https://github.com/openfga/rfcs/blob/main/20220714-listObjects-api.md).

  This addition brings with it two new server configuration options `--listObjects-deadline` and `--listObjects-max-results`. These configurations help protect the server from excessively long lived and large responses.

  > ⚠️ If `--listObjects-deadline` or `--listObjects-max-results` are provided, the endpoint may only return a subset of the data. If you provide the deadline but returning all of the results would take longer than the deadline, then you may not get all of the results. If you limit the max results to 1, then you'll get at most 1 result.

* Support for presharedkey authentication in the Playground (#141)

  The embedded Playground now works if you run OpenFGA using one or more preshared keys for authentication. OIDC authentication remains unsupported for the Playground at this time.


## [0.1.7] - 2022-07-29
### Added
* `migrate` CLI command (#56)

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
        --version uint              the version to migrate to. If omitted, the latest version of the schema will be used
  ```

## [0.1.6] - 2022-07-27
### Fixed
* Issue with embedded Playground assets found in the `v0.1.5` released docker image (#129)

## [0.1.5] - 2022-07-27
### Added
* Support for defining server configuration in `config.yaml`, CLI flags, or env variables (#63 #92 #100)

  `v0.1.5` introduces multiple ways to support a variety of server configuration strategies. You can configure the server with CLI flags, env variables, or a `config.yaml` file.

  Server config will be loaded in the following order of precedence:

    * CLI flags (e.g. `--datastore-engine`)
    * env variables (e.g. `OPENFGA_DATASTORE_ENGINE`)
    * `config.yaml`

  If a `config.yaml` file is provided, the OpenFGA server will look for it in `"/etc/openfga"`, `"$HOME/.openfga"`, or `"."` (the current working directory), in that order.

* Support for grpc health checks (#86)

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

* Profiling support (pprof) (#111)

  You can now profile the OpenFGA server while it's running using the [pprof](https://github.com/google/pprof/blob/main/doc/README.md) profiler. To enable the pprof profiler set `profiler.enabled=true`. It is served on the `/debug/pprof` endpoint and port `3001` by default.

* Configuration to enable/disable the HTTP server (#84)

  You can now enable/disable the HTTP server by setting `http.enabled=true/false`. It is enabled by default.

### Changed
* Env variables have a new mappings.

  Please refer to the [`.config-schema.json`](https://github.com/openfga/openfga/blob/main/.config-schema.json) file for a description of the new configurations or `openfga run -h` for the CLI flags. Env variables are   mapped by prefixing `OPENFGA` and converting dot notation into underscores (e.g. `datastore.uri` becomes `OPENFGA_DATASTORE_URI`). 

### Fixed
* goroutine leaks in Check resolution. (#113)

## [0.1.4] - 2022-06-27
### Added
* OpenFGA Playground support (#68)
* CORS policy configuration (#65)

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

[Unreleased]: https://github.com/openfga/openfga/compare/v0.2.2...HEAD
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
