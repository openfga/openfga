## [0.1.5] - 2022-07-15
## Added
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

* Configuration to enable/disable the HTTP server (#84)

  You can now enable/disable the HTTP server by setting `http.enabled=true/false`. It is enabled by default.