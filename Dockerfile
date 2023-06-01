FROM cgr.dev/chainguard/go:1.20@sha256:8454bbbb4061bd8fed6ce0b6de0d08c0a6037fe33e136b3f16dba31a68b9b3b6 AS builder

WORKDIR /app

COPY . .
RUN CGO_ENABLED=0 go build -o openfga ./cmd/openfga

FROM cgr.dev/chainguard/static@sha256:d1f247050de27feffaedfd47e71c15795a9887d30c76e6d64de9f079765c37a3

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.18 /ko-app/grpc-health-probe /user/local/bin/grpc_health_probe
COPY --from=builder /app/openfga /openfga
COPY --from=builder /app/assets /assets
ENTRYPOINT ["/openfga"]
