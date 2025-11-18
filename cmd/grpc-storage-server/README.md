# gRPC Storage Server

A standalone gRPC storage server backed by PostgreSQL. This server exposes the OpenFGA datastore interface over gRPC, which can be used to test the gRPC datastore client implementation.

## Building

```bash
go build -o grpc-storage-server ./cmd/grpc-storage-server
```

## Usage

```bash
./grpc-storage-server \
  --port 50051 \
  --datastore-uri "postgres://user:pass@localhost:5432/openfga?sslmode=disable"
```

## Flags

- `--host`: Host address to bind (default: `0.0.0.0`)
- `--port`: Port to bind (default: `50051`)
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