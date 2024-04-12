package server

import (
	"context"
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/server/commands/listusers"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/typesystem"
)

// ListUsers returns all subjects (users) of a specified terminal type
// that are relate via specific relation to a specific object
func (s *Server) ListUsers(
	ctx context.Context,
	req *openfgav1.ListUsersRequest,
) (*openfgav1.ListUsersResponse, error) {
	if !s.IsExperimentallyEnabled(ExperimentalEnableListUsers) {
		return nil, status.Error(codes.Unimplemented, "ListUsers is not enabled. It can be enabled for experimental use by passing the `--experimentals enable-list-users` configuration option when running OpenFGA server")
	}

	typesys, err := s.resolveTypesystem(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	err = listusers.ValidateListUsersRequest(req, typesys)
	if err != nil {
		return nil, err
	}

	ctx = typesystem.ContextWithTypesystem(ctx, typesys)

	listUsersQuery := listusers.NewListUsersQuery(s.datastore)

	resp, err := listUsersQuery.ListUsers(ctx, &listusers.ListUsersRequest{
		ListUsersRequest: req,
		Depth:            s.resolveNodeLimit,
	})
	if err != nil {
		if errors.Is(err, graph.ErrResolutionDepthExceeded) {
			return nil, serverErrors.AuthorizationModelResolutionTooComplex
		}
	}
	return resp, err
}
