FROM golang:1.18-alpine AS builder

WORKDIR /app

COPY . .
RUN go build -o ./openfga ./cmd/openfga

FROM alpine as final
EXPOSE 8081
EXPOSE 8080
EXPOSE 3000
COPY --from=builder /app/openfga /app/openfga
COPY --from=builder /app/static /app/static
WORKDIR /app
ENTRYPOINT ["./openfga"]
