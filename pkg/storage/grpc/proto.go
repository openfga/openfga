// Package grpc provides a gRPC interface for OpenFGA storage backends.
// This file triggers proto code generation via buf.
//
// To regenerate proto files, run:
//   go generate ./pkg/storage/grpc
// or
//   make generate-mocks
//
// The buf configuration is in buf.yaml and buf.gen.yaml.

//go:generate sh -c "cd $PWD && buf generate"

package grpc
