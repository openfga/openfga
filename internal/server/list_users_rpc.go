package server

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/commands/listusers"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func ListUsers(
	typesys *typesystem.TypeSystem,
	ds storage.OpenFGADatastore,
	ctx context.Context,
	req *openfgav1.ListUsersRequest,
) (*openfgav1.ListUsersResponse, error) {
	err := listusers.ValidateListUsersRequest(req, typesys)
	if err != nil {
		return nil, err
	}

	ctx = typesystem.ContextWithTypesystem(ctx, typesys)

	listUsersQuery := listusers.NewListUsersQuery(ds)
	return listUsersQuery.ListUsers(ctx, req)
}
