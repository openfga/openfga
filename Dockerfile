FROM cgr.dev/chainguard/go:1.27.4@sha256:6bcd39e3c9c79778784d9210b24a2f5f257e4f7817a4e9b9983d19aedae772d8 AS builder

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

FROM cgr.dev/chainguard/static@sha256:fc42af33e643f20d08baf03cddcc1c02ba57052dfb02edb0d79f3fc47bcea8d9

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000

COPY --from=builder /bin/openfga /openfga

# The container is healthy when the gRPC health check of the server returns SERVING.
HEALTHCHECK --interval=5s --timeout=30s --retries=3 CMD ["/openfga", "healthcheck"]

ENTRYPOINT ["/openfga"]
