FROM golang:1.18-alpine AS builder

RUN mkdir /app && mkdir /app/bin
RUN apk update && apk --no-cache add make curl git gcc build-base

WORKDIR $GOPATH/src/github.com/openfga/openfga

COPY . .
RUN make build
COPY ./static/playground /app/bin/static/playground
RUN cp ./bin/openfga /app/bin/

FROM alpine as final
EXPOSE 8080
RUN mkdir /app && mkdir /app/bin
WORKDIR /app/bin
COPY --from=builder /app/bin /app/bin
ENTRYPOINT ["./openfga"]