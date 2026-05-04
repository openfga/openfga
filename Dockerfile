FROM ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.48@sha256:b615f8b80a6796490b91bfe0f7f4d59cf73767d4921968495cb8b4024090e151 AS grpc_health_probe
# Please manually update the Dockerfile.goreleaser whenever the grpc health probe is updated
FROM cgr.dev/chainguard/go:1.26.2@sha256:e797f1db0ae44e2d2ea4618a8be724f5a3c05f842af2ba47eadc0e573dad78ce AS builder

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

FROM cgr.dev/chainguard/static@sha256:1f14279403150757d801f6308bb0f4b816b162fddce10b9bd342f10adc3cf7fa

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000

COPY --from=grpc_health_probe /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=builder /bin/openfga /openfga

# Healthcheck configuration for the container using grpc_health_probe
# The container will be considered healthy if the gRPC health probe returns a successful response.
HEALTHCHECK --interval=5s --timeout=30s --retries=3 CMD ["/usr/local/bin/grpc_health_probe", "-addr=:8081"]

ENTRYPOINT ["/openfga"]
