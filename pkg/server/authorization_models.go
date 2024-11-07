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

func (s *Server) ReadAuthorizationModel(ctx context.Context, req *openfgav1.ReadAuthorizationModelRequest) (*openfgav1.ReadAuthorizationModelResponse, error) {
	ctx, span := tracer.Start(ctx, authz.ReadAuthorizationModel, trace.WithAttributes(
		attribute.String("store_id", req.GetStoreId()),
		attribute.KeyValue{Key: authorizationModelIDKey, Value: attribute.StringValue(req.GetId())},
	))
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  authz.ReadAuthorizationModel,
	})

	err := s.checkAuthz(ctx, req.GetStoreId(), authz.ReadAuthorizationModel)
	if err != nil {
		return nil, err
	}

	q := commands.NewReadAuthorizationModelQuery(s.datastore, commands.WithReadAuthModelQueryLogger(s.logger))
	return q.Execute(ctx, req)
}

func (s *Server) WriteAuthorizationModel(ctx context.Context, req *openfgav1.WriteAuthorizationModelRequest) (*openfgav1.WriteAuthorizationModelResponse, error) {
	ctx, span := tracer.Start(ctx, authz.WriteAuthorizationModel, trace.WithAttributes(
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
		Method:  authz.WriteAuthorizationModel,
	})

	err := s.checkAuthz(ctx, req.GetStoreId(), authz.WriteAuthorizationModel)
	if err != nil {
		return nil, err
	}

	c := commands.NewWriteAuthorizationModelCommand(s.datastore,
		commands.WithWriteAuthModelLogger(s.logger),
		commands.WithWriteAuthModelMaxSizeInBytes(s.maxAuthorizationModelSizeInBytes),
	)
	res, err := c.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusCreated))

	return res, nil
}

func (s *Server) ReadAuthorizationModels(ctx context.Context, req *openfgav1.ReadAuthorizationModelsRequest) (*openfgav1.ReadAuthorizationModelsResponse, error) {
	ctx, span := tracer.Start(ctx, authz.ReadAuthorizationModels, trace.WithAttributes(
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
		Method:  authz.ReadAuthorizationModels,
	})

	err := s.checkAuthz(ctx, req.GetStoreId(), authz.ReadAuthorizationModels)
	if err != nil {
		return nil, err
	}

	c := commands.NewReadAuthorizationModelsQuery(s.datastore,
		commands.WithReadAuthModelsQueryLogger(s.logger),
		commands.WithReadAuthModelsQueryEncoder(s.encoder),
	)
	return c.Execute(ctx, req)
}
