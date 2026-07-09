package server

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/telemetry"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/middleware/requestid"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/server/commands/v2breaking"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func (s *Server) Expand(ctx context.Context, req *openfgav1.ExpandRequest) (*openfgav1.ExpandResponse, error) {
	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, apimethod.Expand.String(), trace.WithAttributes(
		attribute.KeyValue{Key: "store_id", Value: attribute.StringValue(req.GetStoreId())},
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "consistency", Value: attribute.StringValue(req.GetConsistency().String())},
	))
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  apimethod.Expand.String(),
	})

	err := s.checkAuthz(ctx, req.GetStoreId(), apimethod.Expand)
	if err != nil {
		return nil, err
	}

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}
	req.AuthorizationModelId = typesys.GetAuthorizationModelID() // the resolved model id

	q := commands.NewExpandQuery(s.datastore, commands.WithExpandQueryLogger(s.logger))
	resp, err := q.Execute(
		typesystem.ContextWithTypesystem(ctx, typesys),
		&openfgav1.ExpandRequest{
			StoreId:          storeID,
			TupleKey:         tk,
			Consistency:      req.GetConsistency(),
			ContextualTuples: req.GetContextualTuples(),
		})
	if err != nil {
		return nil, err
	}

	// Flag potential v2 (weighted-graph) resolution breaking changes for this
	// request shape. Shape predicates may over-report; where possible we also
	// confirm the response contains evidence of the v1 behavior.
	// See v2breaking.ExpandReason / ExpandResponseConfirmsReason.
	targetObjectType := tuple.GetType(tk.GetObject())
	if reason := v2breaking.ExpandReason(typesys, targetObjectType, tk.GetRelation()); reason != "" {
		if v2breaking.ExpandResponseConfirmsReason(reason, typesys, targetObjectType, tk.GetRelation(), resp.GetTree()) {
			s.logger.WarnWithContext(ctx, "potential v2 Expand resolution breaking change",
				zap.String("store_id", storeID),
				zap.String("model_id", req.GetAuthorizationModelId()),
				zap.String("request_id", requestid.GetRequestIDFromContext(ctx)),
				zap.String("reason", reason),
			)
		}
	}

	return resp, nil
}
