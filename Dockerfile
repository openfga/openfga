FROM golang:1.20-alpine AS builder
ARG OS
ARG ARCH

WORKDIR /app

COPY . .
RUN go build -o ./openfga ./cmd/openfga
RUN wget -q -O grpc_health_probe "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.13/grpc_health_probe-${OS}-${ARCH}"

FROM alpine as final
EXPOSE 8081
EXPOSE 8080
EXPOSE 3000
COPY --from=builder /app/openfga /app/openfga
COPY --from=builder /app/assets /app/assets
COPY --from=builder /app/grpc_health_probe /bin/grpc_health_probe
RUN chmod +x /bin/grpc_health_probe
WORKDIR /app
ENTRYPOINT ["./openfga"]
