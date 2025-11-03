FROM ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.41@sha256:3b7280dd5f61907bb7e06f436fdea471a58685cfdd7ef18c75e85626a1eb0ade AS grpc_health_probe
FROM cgr.dev/chainguard/go:1.25.3@sha256:c95baa2517f33a3d9ec60536f3241275f765be3ca079e75bded303b62d1bd679 AS builder

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

FROM cgr.dev/chainguard/go:1.25.3@sha256:c95baa2517f33a3d9ec60536f3241275f765be3ca079e75bded303b62d1bd679

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000

COPY --from=grpc_health_probe /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=builder /bin/openfga /openfga

# Healthcheck configuration for the container using grpc_health_probe
# The container will be considered healthy if the gRPC health probe returns a successful response.
HEALTHCHECK --interval=5s --timeout=30s --retries=3 CMD ["/usr/local/bin/grpc_health_probe", "-addr=:8081"]

ENTRYPOINT ["/openfga"]
