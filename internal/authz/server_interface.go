package authz

//go:generate mockgen -source server_interface.go -destination ../mocks/mock_authz_server.go -package mocks

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type ServerInterface interface {
	Check(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error)
	ListObjects(ctx context.Context, req *openfgav1.ListObjectsRequest) (*openfgav1.ListObjectsResponse, error)
}
