FROM cgr.dev/chainguard/go:1.20@sha256:3689133fb91a85ff48a8a1910a906f77c61a39aad8b514c73e3bf09d6022d892 AS builder

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
