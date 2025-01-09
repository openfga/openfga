package server

import (
	"context"
	"errors"
	"strconv"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/authz"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/telemetry"
)

func (s *Server) Check(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	start := time.Now()

	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, authz.Check, trace.WithAttributes(
		attribute.KeyValue{Key: "store_id", Value: attribute.StringValue(req.GetStoreId())},
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
		attribute.KeyValue{Key: "consistency", Value: attribute.StringValue(req.GetConsistency().String())},
	))
	defer span.End()

	ctx = telemetry.ContextWithRPCInfo(ctx, telemetry.RPCInfo{
		Service: s.serviceName,
		Method:  authz.Check,
	})

	err := s.checkAuthz(ctx, req.GetStoreId(), authz.Check)
	if err != nil {
		return nil, err
	}

	if !validator.RequestIsValidatedFromContext(ctx) {
		if err := req.Validate(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	checkQuery := commands.NewCheckCommand(
		s.datastore,
		s.checkResolver,
		typesys,
		commands.WithCheckCommandLogger(s.logger),
		commands.WithCheckCommandMaxConcurrentReads(s.maxConcurrentReadsForCheck),
		commands.WithCheckCommandCache(s.sharedCheckResources, s.cacheSettings),
	)

	resp, checkRequestMetadata, err := checkQuery.Execute(ctx, &commands.CheckCommandParams{
		StoreID:          storeID,
		TupleKey:         req.GetTupleKey(),
		ContextualTuples: req.GetContextualTuples(),
		Context:          req.GetContext(),
		Consistency:      req.GetConsistency(),
	})

	const methodName = "check"
	if err != nil {
		telemetry.TraceError(span, err)
		finalErr := commands.CheckCommandErrorToServerError(err)
		if errors.Is(finalErr, serverErrors.ErrThrottledTimeout) {
			throttledRequestCounter.WithLabelValues(s.serviceName, methodName).Inc()
		}
		// should we define all metrics in one place that is accessible from everywhere (including LocalChecker!)
		// and add a wrapper helper that automatically injects the service name tag?
		return nil, finalErr
	}

	span.SetAttributes(
		attribute.Bool("cycle_detected", resp.GetCycleDetected()),
		attribute.Bool("allowed", resp.GetAllowed()))

	queryCount := float64(resp.GetResolutionMetadata().DatastoreQueryCount)

	grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, queryCount)
	span.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, queryCount))
	datastoreQueryCountHistogram.WithLabelValues(
		s.serviceName,
		methodName,
	).Observe(queryCount)

	rawDispatchCount := checkRequestMetadata.DispatchCounter.Load()
	dispatchCount := float64(rawDispatchCount)

	grpc_ctxtags.Extract(ctx).Set(dispatchCountHistogramName, dispatchCount)
	span.SetAttributes(attribute.Float64(dispatchCountHistogramName, dispatchCount))
	dispatchCountHistogram.WithLabelValues(
		s.serviceName,
		methodName,
	).Observe(dispatchCount)

	res := &openfgav1.CheckResponse{
		Allowed: resp.Allowed,
	}

	checkResultCounter.With(prometheus.Labels{allowedLabel: strconv.FormatBool(resp.GetAllowed())}).Inc()

	requestDurationHistogram.WithLabelValues(
		s.serviceName,
		methodName,
		utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
		utils.Bucketize(uint(rawDispatchCount), s.requestDurationByDispatchCountHistogramBuckets),
		req.GetConsistency().String(),
	).Observe(float64(time.Since(start).Milliseconds()))

	if s.authorizer.AccessControlStoreID() == req.GetStoreId() {
		accessControlStoreCheckDurationHistogram.WithLabelValues(
			utils.Bucketize(uint(queryCount), s.requestDurationByQueryHistogramBuckets),
			utils.Bucketize(uint(rawDispatchCount), s.requestDurationByDispatchCountHistogramBuckets),
			req.GetConsistency().String(),
		).Observe(float64(time.Since(start).Milliseconds()))
	}

	wasRequestThrottled := checkRequestMetadata.WasThrottled.Load()
	if wasRequestThrottled {
		throttledRequestCounter.WithLabelValues(s.serviceName, methodName).Inc()
	}

	return res, nil
}
