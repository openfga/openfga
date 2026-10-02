FROM cgr.dev/chainguard/go:1.26.8@sha256:a29506323609aeee20bc963ea6d998bc76a6c05728358147e2e334d9d8a3c6f4 AS builder

WORKDIR /app

# install and cache dependencies
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    --mount=type=bind,source=go.mod,target=go.mod \
    go mod download -x

# build with cache
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=bind,target=. \
    CGO_ENABLED=0 go build -o /bin/openfga ./cmd/openfga

FROM cgr.dev/chainguard/static@sha256:f51c2493951313c3ad4069080b2814ffb6ed6fe3909dabeb84a9482f42d5600b

EXPOSE 8081
EXPOSE 8080
EXPOSE 3000

COPY --from=builder /bin/openfga /openfga

ENTRYPOINT ["/openfga"]
