package server

import (
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/internal/authz"
	"github.com/openfga/openfga/internal/throttler/threshold"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/typesystem"
)

func (s *Server) StreamedListObjects(req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) error {
	start := time.Now()

	ctx := srv.Context()
	ctx, span := tracer.Start(ctx, authz.StreamedListObjects, trace.WithAttributes(
		attribute.String("store_id", req.GetStoreId()),
		attribute.String("object_type", req.GetType()),
		attribute.String("relation", req.GetRelation()),
		attribute.String("user", req.GetUser()),
		attribute.String("consistency", req.GetConsistency().String()),
	))
	defer span.End()

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}

	const methodName = "streamedlistobjects"

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  methodName,
	})

	err := s.checkAuthz(ctx, req.GetStoreId(), authz.StreamedListObjects)
	if err != nil {
		return err
	}

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	q, err := commands.NewListObjectsQuery(
		s.datastore,
		s.checkResolver,
		commands.WithLogger(s.logger),
		commands.WithListObjectsDeadline(s.listObjectsDeadline),
		commands.WithDispatchThrottlerConfig(threshold.Config{
			Throttler:    s.listObjectsDispatchThrottler,
			Enabled:      s.listObjectsDispatchThrottlingEnabled,
			Threshold:    s.listObjectsDispatchDefaultThreshold,
			MaxThreshold: s.listObjectsDispatchThrottlingMaxThreshold,
		}),
		commands.WithListObjectsMaxResults(s.listObjectsMaxResults),
		commands.WithResolveNodeLimit(s.resolveNodeLimit),
		commands.WithResolveNodeBreadthLimit(s.resolveNodeBreadthLimit),
		commands.WithMaxConcurrentReads(s.maxConcurrentReadsForListObjects),
	)
	if err != nil {
		return serverErrors.NewInternalError("", err)
	}

	req.AuthorizationModelId = typesys.GetAuthorizationModelID() // the resolved model id

	resolutionMetadata, err := q.ExecuteStreamed(
		typesystem.ContextWithTypesystem(ctx, typesys),
		req,
		srv,
	)
	if err != nil {
		telemetry.TraceError(span, err)
		return err
	}
	datastoreQueryCount := float64(resolutionMetadata.DatastoreQueryCount.Load())

	grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, datastoreQueryCount)
	span.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, datastoreQueryCount))
	datastoreQueryCountHistogram.WithLabelValues(
		s.serviceName,
		methodName,
	).Observe(datastoreQueryCount)

	dispatchCount := float64(resolutionMetadata.DispatchCounter.Load())

	grpc_ctxtags.Extract(ctx).Set(dispatchCountHistogramName, dispatchCount)
	span.SetAttributes(attribute.Float64(dispatchCountHistogramName, dispatchCount))
	dispatchCountHistogram.WithLabelValues(
		s.serviceName,
		methodName,
	).Observe(dispatchCount)

	requestDurationHistogram.WithLabelValues(
		s.serviceName,
		methodName,
		utils.Bucketize(uint(datastoreQueryCount), s.requestDurationByQueryHistogramBuckets),
		utils.Bucketize(uint(resolutionMetadata.DispatchCounter.Load()), s.requestDurationByDispatchCountHistogramBuckets),
		req.GetConsistency().String(),
	).Observe(float64(time.Since(start).Milliseconds()))

	wasRequestThrottled := resolutionMetadata.WasThrottled.Load()
	if wasRequestThrottled {
		throttledRequestCounter.WithLabelValues(s.serviceName, methodName).Inc()
	}

	return nil
}
