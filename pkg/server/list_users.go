package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/telemetry"

	"github.com/openfga/openfga/pkg/server/commands/listusers"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/typesystem"
)

// ListUsers returns all subjects (users) of a specified terminal type
// that are relate via specific relation to a specific object.
func (s *Server) ListUsers(
	ctx context.Context,
	req *openfgav1.ListUsersRequest,
) (*openfgav1.ListUsersResponse, error) {
	ctx, span := tracer.Start(ctx, "ListUsers", trace.WithAttributes(
		attribute.String("object", fmt.Sprintf("%s:%s", req.GetObject().GetType(), req.GetObject().GetId())),
		attribute.String("relation", req.GetRelation()),
		attribute.String("user_filters", userFiltersToString(req.GetUserFilters())),
	))
	defer span.End()
	if !s.IsExperimentallyEnabled(ExperimentalEnableListUsers) {
		return nil, status.Error(codes.Unimplemented, "ListUsers is not enabled. It can be enabled for experimental use by passing the `--experimentals enable-list-users` configuration option when running OpenFGA server")
	}

	typesys, err := s.resolveTypesystem(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	err = listusers.ValidateListUsersRequest(ctx, req, typesys)
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	ctx = typesystem.ContextWithTypesystem(ctx, typesys)

	listUsersQuery := listusers.NewListUsersQuery(s.datastore, listusers.WithResolveNodeLimit(s.resolveNodeLimit), listusers.WithListUsersQueryLogger(s.logger))

	resp, err := listUsersQuery.ListUsers(ctx, req)
	if err != nil {
		telemetry.TraceError(span, err)
		if errors.Is(err, graph.ErrResolutionDepthExceeded) {
			return nil, serverErrors.AuthorizationModelResolutionTooComplex
		}
		if errors.Is(err, condition.ErrEvaluationFailed) {
			return nil, serverErrors.ValidationError(err)
		}
		return nil, serverErrors.HandleError("", err)
	}
	return &openfgav1.ListUsersResponse{
		Users: resp.GetUsers(),
	}, err
}

func userFiltersToString(filter []*openfgav1.UserTypeFilter) string {
	var s strings.Builder
	for _, f := range filter {
		s.WriteString(f.GetType())
		if f.GetRelation() != "" {
			s.WriteString("#" + f.GetRelation())
		}
	}
	return s.String()
}
