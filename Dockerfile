FROM ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.39@sha256:bec245175713ded08cf7e9620f5c189bbd4bdbf2203eac5cfb01f82960c87d62 AS grpc_health_probe
FROM cgr.dev/chainguard/go:1.24.5@sha256:894edfc7e7e6d2a2cd3cf53025da0c6a3a6a8769c30dfe0f2b5bcff7229f195e AS builder

WORKDIR /app

# install and cache dependencies
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    --mount=type=bind,source=go.mod,target=go.mod \
    go mod download -x

# build with cache
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=bind,target=. \
    CGO_ENABLED=0 go build -o /bin/openfga ./cmd/openfga

FROM cgr.dev/chainguard/static@sha256:853bfd4495abb4b65ede8fc9332513ca2626235589c2cef59b4fce5082d0836d

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000

COPY --from=grpc_health_probe /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=builder /bin/openfga /openfga

# Healthcheck configuration for the container using grpc_health_probe
# The container will be considered healthy if the gRPC health probe returns a successful response.
HEALTHCHECK --interval=5s --timeout=30s --retries=3 CMD ["/usr/local/bin/grpc_health_probe", "-addr=:8081"]

ENTRYPOINT ["/openfga"]
