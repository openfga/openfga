FROM ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.26@sha256:a04a96bd92126604f5bc10998004d5274fa9e6a232dba0a3ef3eb18a318990ff as grpc_health_probe
FROM cgr.dev/chainguard/go:1.21@sha256:40fb38b3f61d1ecdecd2bfe3d14fa621ab5e9ecb4c9ebba590276c2992f3e282 AS builder

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

FROM cgr.dev/chainguard/static@sha256:873e9709e2a83acc995ff24e71c100480f9c0368e0d86eaee9c3c7cb8fb5f0e0

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000

COPY --from=grpc_health_probe /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=builder /bin/openfga /openfga

# Healthcheck configuration for the container using grpc_health_probe
# The container will be considered healthy if the gRPC health probe returns a successful response.
HEALTHCHECK --interval=5s --timeout=30s --retries=3 CMD ["/usr/local/bin/grpc_health_probe", "-addr=:8081"]

ENTRYPOINT ["/openfga"]
