package graph

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
)

var (
	deduplicatedDispatchCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "deduplicated_dispatch_count",
		Help:      "The total number of calls to ResolveCheck that were prevented due to deduplication of singleflight resolver.",
	})

	deduplicatedDBQueryCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "deduplicated_datastore_query_count",
		Help:      "The total number of datastore queries that were prevented due to deduplication of singleflight resolver.",
	})
)

type SingleflightCheckResolver struct {
	delegate CheckResolver
	group    singleflight.Group
	logger   logger.Logger
}

func NewSingleflightCheckResolver() *SingleflightCheckResolver {
	s := &SingleflightCheckResolver{}
	s.delegate = s

	return s
}

// SetDelegate sets this SingleflightCheckResolver's dispatch delegate.
func (s *SingleflightCheckResolver) SetDelegate(delegate CheckResolver) {
	s.delegate = delegate
}

// Close implements CheckResolver.
func (s *SingleflightCheckResolver) Close() {}

// ResolveCheck implements CheckResolver.
func (s *SingleflightCheckResolver) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "ResolveCheck")
	defer span.End()
	span.SetAttributes(attribute.String("resolver_type", "SingleflightCheckResolver"))
	span.SetAttributes(attribute.String("tuple_key", req.GetTupleKey().String()))

	key, err := CheckRequestCacheKey(req)
	if err != nil {
		s.logger.Error("singleflight cache key computation failed with error", zap.Error(err))
		return nil, err
	}

	isUnique := false
	singleFlightResp, err, shared := s.group.Do(key, func() (interface{}, error) {
		isUnique = true
		resp, err := s.delegate.ResolveCheck(ctx, req)
		if err != nil {
			return nil, err
		}
		return copyResolveResponse(*resp), nil
	})
	span.SetAttributes(singleflightRequestStateAttribute(shared, isUnique))
	if err != nil {
		telemetry.TraceError(span, err)
		return nil, err
	}

	r := singleFlightResp.(ResolveCheckResponse)
	// Important to create a dereferenced copy of the group.Do's response, otherwise
	// it establishes a shared memory reference between the goroutines that were
	// involved in the de-duplication, and thus is subject to race conditions.
	resp := copyResolveResponse(r)

	if shared && !isUnique {
		deduplicatedDispatchCount.Inc()
		deduplicatedDBQueryCount.Add(float64(resp.GetResolutionMetadata().DatastoreQueryCount))
		resp.ResolutionMetadata.DatastoreQueryCount = 0
	}

	return &resp, err
}

func copyResolveResponse(original ResolveCheckResponse) ResolveCheckResponse {
	return ResolveCheckResponse{
		Allowed: original.GetAllowed(),
		ResolutionMetadata: &ResolutionMetadata{
			Depth:               original.GetResolutionMetadata().Depth,
			DatastoreQueryCount: original.GetResolutionMetadata().DatastoreQueryCount,
		},
	}
}

func singleflightRequestStateAttribute(isShared bool, isUnique bool) attribute.KeyValue {
	attrName := "singleflight_resolver_state"
	if !isShared {
		return attribute.String(attrName, "not_shared")
	}
	if isUnique {
		return attribute.String(attrName, "shared_and_resolved")
	}
	return attribute.String(attrName, "shared_but_deduplicated")
}
