package server

import (
	"context"
	"net/http"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/authz"
	httpmiddleware "github.com/openfga/openfga/pkg/middleware/http"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/telemetry"
)

func (s *Server) WriteAssertions(ctx context.Context, req *openfgav1.WriteAssertionsRequest) (*openfgav1.WriteAssertionsResponse, error) {
	ctx, span := tracer.Start(ctx, authz.WriteAssertions, trace.WithAttributes(
		attribute.String("store_id", req.GetStoreId()),
	))
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  authz.WriteAssertions,
	})

	err := s.checkAuthz(ctx, req.GetStoreId(), authz.WriteAssertions)
	if err != nil {
		return nil, err
	}

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	c := commands.NewWriteAssertionsCommand(s.datastore, commands.WithWriteAssertCmdLogger(s.logger))
	res, err := c.Execute(ctx, &openfgav1.WriteAssertionsRequest{
		StoreId:              storeID,
		AuthorizationModelId: typesys.GetAuthorizationModelID(), // the resolved model id
		Assertions:           req.GetAssertions(),
	})
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusNoContent))

	return res, nil
}

func (s *Server) ReadAssertions(ctx context.Context, req *openfgav1.ReadAssertionsRequest) (*openfgav1.ReadAssertionsResponse, error) {
	ctx, span := tracer.Start(ctx, authz.ReadAssertions, trace.WithAttributes(
		attribute.String("store_id", req.GetStoreId()),
	))
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  authz.ReadAssertions,
	})

	err := s.checkAuthz(ctx, req.GetStoreId(), authz.ReadAssertions)
	if err != nil {
		return nil, err
	}

	typesys, err := s.resolveTypesystem(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	q := commands.NewReadAssertionsQuery(s.datastore, commands.WithReadAssertionsQueryLogger(s.logger))
	return q.Execute(ctx, req.GetStoreId(), typesys.GetAuthorizationModelID())
}
