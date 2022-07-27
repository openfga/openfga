# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/openfga/openfga/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/openfga/openfga/releases/tag/v0.1.5
[0.1.4]: https://github.com/openfga/openfga/releases/tag/v0.1.4
[0.1.2]: https://github.com/openfga/openfga/releases/tag/v0.1.2
[0.1.1]: https://github.com/openfga/openfga/releases/tag/v0.1.1
[0.1.0]: https://github.com/openfga/openfga/releases/tag/v0.1.0
