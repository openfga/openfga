package server

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/authz"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *Server) BatchCheck(ctx context.Context, req *openfgav1.BatchCheckRequest) (*openfgav1.BatchCheckResponse, error) {
	ctx, span := tracer.Start(ctx, authz.Read, trace.WithAttributes(
		attribute.KeyValue{Key: "store_id", Value: attribute.StringValue(req.GetStoreId())},
		attribute.KeyValue{Key: "batch_size", Value: attribute.IntValue(len(req.GetChecks()))},
		attribute.KeyValue{Key: "consistency", Value: attribute.StringValue(req.GetConsistency().String())},
	))
	defer span.End()

	err := req.Validate()
	if err != nil {
		return nil, err
	}

	// TODO what do i need to do for authz, just add it to the constants in authz.go?
	err = s.checkAuthz(ctx, req.GetStoreId(), authz.BatchCheck)
	if err != nil {
		return nil, err
	}

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  authz.BatchCheck,
	})

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	// call a batch check command
	// that command will just manage concurrency, fan out the checks, and run the timer
	commands.NewBatchCheckCommand(
		s.checkDatastore,
		s.checkResolver,
		typesys,
		commands.WithBatchCheckCommandCacheController(s.cacheController),
		//commands.WithBatchCheckMaxConcurrentReadsPerCheck()
		//commands.WithBatchCheckResolveNodeLimit(),
		// TODO config variables somewhere
		// or does this belong as an execute param?
		commands.WithBatchCheckCommandMaxConcurrentChecks(50),
		commands.WithBatchCheckCommandLogger(s.logger),
	)
}
