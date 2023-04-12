FROM cgr.dev/chainguard/go:1.20@sha256:bcd60e546369b2396398a46a9267cce8b8638252b1d7687a8a84aee1b4b50743 AS builder

WORKDIR /app

COPY . .
RUN CGO_ENABLED=0 go build -o openfga ./cmd/openfga

FROM cgr.dev/chainguard/static:latest@sha256:289ab2b147f431117275a519a2bcd3b1ceffb54bf386e562596b09cd44be426f

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000
COPY --chmod=0755 --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.16 /ko-app/grpc-health-probe /bin/grpc_health_probe
COPY --from=builder /app/openfga /openfga
COPY --from=builder /app/assets /assets
ENTRYPOINT ["/openfga"]
