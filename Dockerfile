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

COPY --from=builder /bin/openfga /openfga

ENTRYPOINT ["/openfga"]
