FROM ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.24@sha256:ba223f05f047e15d169bb87e0b312646668e874c8207066d964bdfb6757c10f6 as grpc_health_probe
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

FROM cgr.dev/chainguard/static@sha256:5ef2713be4309954b594d0b575b746b44b25786ab924894b6b156e73ce48583b

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000

COPY --from=grpc_health_probe /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=builder /bin/openfga /openfga

# Healthcheck configuration for the container using grpc_health_probe
# The container will be considered healthy if the gRPC health probe returns a successful response.
HEALTHCHECK --interval=5s --timeout=30s --retries=3 CMD ["/usr/local/bin/grpc_health_probe", "-addr=:8081"]

ENTRYPOINT ["/openfga"]
