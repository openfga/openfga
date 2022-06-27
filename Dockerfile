FROM golang:1.18-alpine AS builder

WORKDIR /app

COPY . .
RUN go build -o ./openfga ./cmd/openfga

FROM alpine as final
COPY --from=builder /app/openfga /app/openfga
COPY --from=builder /app/static /app/static
WORKDIR /app
ENTRYPOINT ["./openfga"]
