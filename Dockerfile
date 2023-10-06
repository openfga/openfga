FROM cgr.dev/chainguard/go:1.21@sha256:eb2d1372867b054146bcd0035a83772f486f01f7f0a05ac35f0866fe87264741 AS builder

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

FROM cgr.dev/chainguard/static@sha256:a432665213f109d5e48111316030eecc5191654cf02a5b66ac6c5d6b310a5511

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000

COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.19 /ko-app/grpc-health-probe /user/local/bin/grpc_health_probe
COPY --from=builder /bin/openfga /openfga

ENTRYPOINT ["/openfga"]
