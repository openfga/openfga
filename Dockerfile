FROM golang:1.18-alpine AS builder

RUN apk update && apk --no-cache add make

RUN mkdir /app
WORKDIR /app

COPY . .
RUN make build
RUN cp ./bin/openfga /app

FROM alpine as final
EXPOSE 8080
RUN mkdir /app
COPY --from=builder /app/openfga /app
ENTRYPOINT ["/app/openfga"]