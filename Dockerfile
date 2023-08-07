FROM cgr.dev/chainguard/go:1.20@sha256:8864ff1cd54f5819063ea23133a9f03d7626cec7e9fba8efa7004c71176adc48 AS builder

WORKDIR /app

# install and cache dependencies
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,source=go.sum,target=go.sum \
    --mount=type=bind,source=go.mod,target=go.mod \
      go mod download -x

# build
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
     CGO_ENABLED=0 go build -o openfga ./cmd/openfga

FROM cgr.dev/chainguard/static@sha256:6b35c7e7084349b3a71e70219f61ea49b22d663b89b0ea07474e5b44cbc70860

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.19 /ko-app/grpc-health-probe /user/local/bin/grpc_health_probe
COPY --from=builder /app/openfga /openfga
COPY --from=builder /app/assets /assets
ENTRYPOINT ["/openfga"]
