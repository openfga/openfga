FROM cgr.dev/chainguard/go:1.20@sha256:8ed3fdc8f6375a3fd84b4b8b696a2366c3a639931aab492d6f92ca917e726ad6 AS builder

WORKDIR /app

COPY . .
RUN CGO_ENABLED=0 go build -o openfga ./cmd/openfga

FROM cgr.dev/chainguard/static@sha256:54b589146d4dbc80a094fcbcd6b09414f3df94cde8ea6d31c44fd02692c58203

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.19 /ko-app/grpc-health-probe /user/local/bin/grpc_health_probe
COPY --from=builder /app/openfga /openfga
COPY --from=builder /app/assets /assets
ENTRYPOINT ["/openfga"]
