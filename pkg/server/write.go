package server

import (
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/authz"
	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/telemetry"
)

func (s *Server) Write(ctx context.Context, req *openfgav1.WriteRequest) (*openfgav1.WriteResponse, error) {
	start := time.Now()

	ctx, span := tracer.Start(ctx, authz.Write, trace.WithAttributes(
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
		Method:  authz.Write,
	})

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())

	s.logger.Error("typesys is: " + typesys.GetAuthorizationModelID())

	if err != nil {
		return nil, err
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  authz.Write,
	})

	err = s.checkWriteAuthz(ctx, req, typesys)
	if err != nil {
		return nil, err
	}

	s.logger.Error("about to new write command")
	cmd := commands.NewWriteCommand(
		s.datastore,
		commands.WithWriteCmdLogger(s.logger),
	)
	s.logger.Error("about to cmd.Execute")
	resp, err := cmd.Execute(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: typesys.GetAuthorizationModelID(), // the resolved model id
		Writes:               req.GetWrites(),
		Deletes:              req.GetDeletes(),
	})

	// For now, we only measure the duration if it passes the authz step to make the comparison
	// apple to apple.
	s.logger.Error("about to WithLabelValues")
	writeDurationHistogram.WithLabelValues(
		strconv.FormatBool(s.IsAccessControlEnabled() && !authclaims.SkipAuthzCheckFromContext(ctx)),
	).Observe(float64(time.Since(start).Milliseconds()))

	s.logger.Error("about to return resp...")
	return resp, err
}
