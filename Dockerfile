FROM golang:1.18-alpine AS builder

WORKDIR /app

COPY . .
RUN go build -o ./openfga ./cmd/openfga

FROM alpine as final
EXPOSE 8080
COPY --from=builder /app/openfga /app/openfga
ENTRYPOINT ["/app/openfga"]