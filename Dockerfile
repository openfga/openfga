FROM cgr.dev/chainguard/go:1.20 AS builder

WORKDIR /app

COPY . .
RUN CGO_ENABLED=0 go build -o openfga ./cmd/openfga

FROM cgr.dev/chainguard/static:latest

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.16 /ko-app/grpc-health-probe /user/local/bin/grpc_health_probe
COPY --from=builder /app/openfga /openfga
COPY --from=builder /app/assets /assets
ENTRYPOINT ["/openfga"]
