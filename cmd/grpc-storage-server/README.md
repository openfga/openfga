# gRPC Storage Server

A standalone gRPC storage server backed by PostgreSQL. This server exposes the OpenFGA datastore interface over gRPC, which can be used to test the gRPC datastore client implementation.

## Building

```bash
go build -o grpc-storage-server ./cmd/grpc-storage-server
```

## Usage

### TCP Connection (default)

```bash
./grpc-storage-server \
  --port 50051 \
  --datastore-uri "postgres://user:pass@localhost:5432/openfga?sslmode=disable"
```

### Unix Domain Socket

For sidecar or same-machine deployments, Unix domain sockets provide lower latency and reduced overhead by avoiding network stack traversal:

```bash
./grpc-storage-server \
  --socket /var/run/openfga.sock \
  --datastore-uri "postgres://user:pass@localhost:5432/openfga?sslmode=disable"
```

When using Unix sockets, the client should connect using the `unix://` scheme:

```bash
# Example: Configure OpenFGA server to connect via Unix socket
openfga run \
  --datastore-engine grpc \
  --datastore-grpc-addr "unix:///var/run/openfga.sock"
```

## Flags

- `--host`: Host address to bind for TCP (default: `0.0.0.0`, ignored if `--socket` is set)
- `--port`: Port to bind for TCP (default: `50051`, ignored if `--socket` is set)
- `--socket`: Unix domain socket path to bind (e.g., `/var/run/openfga.sock`)
- `--datastore-uri`: PostgreSQL connection URI (required)
- `--log-level`: Log level - debug, info, warn, error (default: `info`)

Connection pool settings use sensible defaults appropriate for testing.

## Quick Start with Docker Compose

The easiest way to run the gRPC storage server with PostgreSQL is using Docker Compose:

```bash
cd cmd/grpc-storage-server
docker compose up
```

This will:
1. Start PostgreSQL on port 5432
2. Run database migrations automatically
3. Start the gRPC storage server on port 50051

All migrations are handled automatically, so the server is ready to use immediately.

To run in the background:
```bash
docker compose up -d
```

To stop and remove volumes (clean slate):
```bash
docker compose down -v
```