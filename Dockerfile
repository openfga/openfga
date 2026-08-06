FROM cgr.dev/chainguard/go:1.26.5@sha256:fd4cfadccffc600948b4d9b3dedb2f447748c5743b58aa66701076a47892c289 AS builder

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

FROM cgr.dev/chainguard/static@sha256:60582b2ae6074f641094af0f370d4ab241aab271858a66223dcde7eee9f51638

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000

COPY --from=builder /bin/openfga /openfga

# Healthcheck uses the built-in `openfga healthcheck` command, which probes the
# gRPC Health Checking Protocol endpoint. The container is healthy when the
# server reports SERVING.
HEALTHCHECK --interval=5s --timeout=30s --retries=3 CMD ["/openfga", "healthcheck"]

ENTRYPOINT ["/openfga"]
